"""Coordination of online (CONCURRENTLY) index builds across API instances.

All coordination state lives in PostgreSQL itself, so any number of stateless
API instances can run concurrently without a leader:

* claim a build          -> INSERT ... ON CONFLICT (index_name) DO NOTHING
                            (unique constraint => exactly one winner)
* serialize decide/clean -> session-level advisory lock (DECISION_LOCK_KEY)
                            around the critical section that inspects the
                            catalog, retires orphaned claims, drops invalid
                            leftovers and claims a new build
* prove liveness         -> the claiming instance refreshes `heartbeat_at`
                            from a dedicated thread for the whole lifetime of
                            the build. A 'building' claim keeps its build
                            eligibility while the heartbeat is fresh -- a mere
                            timeout (claim age) never recycles a claim whose
                            owner is still alive.
* fence the DDL itself   -> the build thread holds a second session-level
                            advisory lock (BUILD_LOCK_KEY) across its
                            eligibility re-check and the CREATE INDEX. A
                            reclaim is only allowed when that lock is free, so
                            a stalled-but-alive builder can never lose its
                            claim, and a builder that did lose it (its process
                            was frozen past the lease, the row was retired and
                            re-claimed) observes the replacement inside the
                            lock and backs off WITHOUT issuing DDL.
* fence late completions -> UPDATE ... WHERE id = <job> AND status = 'building'
                            affects 0 rows once the row was retired/replaced,
                            so a late old job can never write back old results
* ground truth           -> pg_index.indisvalid / indisready, pg_get_indexdef,
                            pg_stat_progress_create_index / pg_stat_activity

"ready" is only ever reported after the database catalog confirms the index
exists, is valid + ready, and has the expected definition.

Build eligibility (shared by /indexes/build, /indexes/status,
/indexes/cleanup and startup reconcile): a 'building' claim is alive if ANY
of these holds --
  1. a backend is actively running CREATE/DROP INDEX on this index, or
  2. the owner's heartbeat is fresh (< LEASE_SECONDS), or
  3. a build thread holds the build advisory lock (DDL critical section).
Only when none holds is the claim orphaned: it is then retired (failed, or
ready if the catalog proves the build completed), leftovers are dropped, and
a fresh claim may take over and retry.
"""

import logging
import os
import socket
import threading
import time
import uuid

from .db import connect

log = logging.getLogger("index_manager")

INDEX_NAME = "idx_fault_logs_device_occurred"
TABLE_NAME = "device_fault_logs"
EXPECTED_COLUMNS = "(device_id, occurred_at)"

# Fixed advisory-lock keys serializing decisions and the DDL critical section
# across all instances.
DECISION_LOCK_KEY = 861_227_001
BUILD_LOCK_KEY = 861_227_002

# Heartbeat lease: a 'building' claim whose heartbeat is younger than this is
# alive, even if its CREATE INDEX is not visible in pg_stat_activity yet
# (claim -> statement start stall). Once the heartbeat is stale AND no build
# backend is active AND no build thread holds the build lock, the claim is
# orphaned and may be recycled. The owner refreshes the heartbeat roughly
# three times per lease, so one lost update is tolerated.
LEASE_SECONDS = float(os.environ.get("ORPHAN_GRACE_SECONDS", "20"))
HEARTBEAT_INTERVAL = max(1.0, LEASE_SECONDS / 3.0)

INSTANCE_ID = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"

CREATE_INDEX_SQL = (
    f'CREATE INDEX CONCURRENTLY "{INDEX_NAME}" '
    f"ON {TABLE_NAME} (device_id, occurred_at)"
)
DROP_INDEX_SQL = f'DROP INDEX CONCURRENTLY IF EXISTS "{INDEX_NAME}"'


# --------------------------------------------------------------------------
# Catalog inspection
# --------------------------------------------------------------------------

def _catalog_state(conn) -> dict:
    row = conn.execute(
        """
        SELECT i.indisvalid, i.indisready, pg_get_indexdef(i.indexrelid)
        FROM pg_class c
        JOIN pg_index i      ON i.indexrelid = c.oid
        JOIN pg_namespace n  ON n.oid = c.relnamespace
        WHERE c.relname = %s
          AND n.nspname = current_schema()
          AND c.relkind = 'i'
        """,
        (INDEX_NAME,),
    ).fetchone()
    if not row:
        return {"exists": False, "valid": False, "ready": False, "definition": None}
    return {"exists": True, "valid": bool(row[0]), "ready": bool(row[1]),
            "definition": row[2]}


def _definition_ok(definition) -> bool:
    if not definition:
        return False
    norm = " ".join(str(definition).split())
    return (
        TABLE_NAME in norm
        and EXPECTED_COLUMNS in norm
        and "USING btree" in norm
    )


def _catalog_ok(cat: dict) -> bool:
    """True only if the catalog proves the index is usable and correct."""
    return (
        cat["exists"]
        and cat["valid"]
        and cat["ready"]
        and _definition_ok(cat["definition"])
    )


def _active_builds(conn) -> int:
    """Number of backends currently running CREATE/DROP INDEX on our index."""
    in_progress = conn.execute(
        """
        SELECT count(*)
        FROM pg_stat_progress_create_index p
        JOIN pg_class c ON c.oid = p.index_relid
        WHERE c.relname = %s
        """,
        (INDEX_NAME,),
    ).fetchone()[0]
    # Only DDL statements count; monitoring queries merely *mention* the index
    # name (psycopg binds parameters client-side, so their text contains it).
    statements = conn.execute(
        """
        SELECT count(*)
        FROM pg_stat_activity
        WHERE pid <> pg_backend_pid()
          AND state = 'active'
          AND query ~* '^\\s*(create|drop)\\s+index\\b'
          AND query ILIKE %s
        """,
        (f"%{INDEX_NAME}%",),
    ).fetchone()[0]
    # The same backend can appear in both views; what matters is "is anyone
    # actively working on this index right now".
    return max(int(in_progress), int(statements))


# --------------------------------------------------------------------------
# Job ledger
# --------------------------------------------------------------------------

def _get_job(conn):
    row = conn.execute(
        """
        SELECT id, status, builder, error, created_at, updated_at, heartbeat_at,
               EXTRACT(EPOCH FROM (now() - created_at))::float   AS age_s,
               EXTRACT(EPOCH FROM (now() - heartbeat_at))::float AS heartbeat_age_s
        FROM index_build_jobs
        WHERE index_name = %s
        """,
        (INDEX_NAME,),
    ).fetchone()
    if not row:
        return None
    return {
        "id": row[0], "status": row[1], "builder": row[2], "error": row[3],
        "created_at": row[4], "updated_at": row[5], "heartbeat_at": row[6],
        "age_s": float(row[7]), "heartbeat_age_s": float(row[8]),
    }


def _heartbeat_fresh(job: dict) -> bool:
    return job["heartbeat_age_s"] < LEASE_SECONDS


def _claim_status(conn, job: dict, active: int) -> str:
    """Shared build-eligibility verdict for a 'building' claim.

    Returns "live" when the claim still has build eligibility (a backend is
    actively building, the owner's heartbeat is fresh, or a build thread
    holds the build advisory lock). Returns "orphan_locked" when the claim
    has definitely lost eligibility -- in that case the CALLER now holds the
    build advisory lock on this session and MUST release it after retiring
    the claim. Holding it across the retirement is what guarantees a late
    thread of the old claim cannot squeeze its CREATE INDEX in between our
    check and the row update: its own lock acquisition serializes behind
    ours, and its in-lock eligibility re-check then sees the retired row.
    """
    if active > 0 or _heartbeat_fresh(job):
        return "live"
    got = conn.execute(
        "SELECT pg_try_advisory_lock(%s)", (BUILD_LOCK_KEY,)
    ).fetchone()[0]
    if not got:
        # A build thread is inside its DDL critical section right now.
        return "live"
    return "orphan_locked"


def _release_build_lock(conn) -> None:
    conn.execute("SELECT pg_advisory_unlock(%s)", (BUILD_LOCK_KEY,))


def _transition(conn, job_id, to_status, error, from_status) -> int:
    """Fenced state change: only applies if the row is still in from_status."""
    cur = conn.execute(
        """
        UPDATE index_build_jobs
        SET status = %s, error = %s, updated_at = now()
        WHERE id = %s AND status = %s
        """,
        (to_status, error, job_id, from_status),
    )
    return cur.rowcount


def _finish(job_id: uuid.UUID, status: str, error) -> None:
    """Record a build outcome; a retired/replaced job row makes this a no-op."""
    try:
        with connect(autocommit=True) as conn:
            n = _transition(conn, job_id, status, error, "building")
            if n == 0:
                log.info(
                    "stale finish for job %s ignored (row retired or replaced)",
                    job_id,
                )
    except Exception:
        log.exception("could not record build result for job %s", job_id)


# --------------------------------------------------------------------------
# Build worker (build thread + heartbeat thread)
# --------------------------------------------------------------------------

def _pre_ddl_delay_seconds() -> float:
    """Test hook: artificial stall before the build critical section.

    Read at runtime from the index_build_controls table (key
    'pre_ddl_delay_seconds') so the self-test can reproduce the
    claim-then-stall race on any instance. Always 0 in production.
    """
    try:
        with connect(autocommit=True) as conn:
            row = conn.execute(
                "SELECT value FROM index_build_controls "
                "WHERE key = 'pre_ddl_delay_seconds'"
            ).fetchone()
        return max(0.0, float(row[0])) if row else 0.0
    except Exception:
        log.warning("pre-DDL delay hook unreadable; assuming 0", exc_info=True)
        return 0.0


def _heartbeat_loop(job_id: uuid.UUID, stop: threading.Event) -> None:
    """Keep the claim's heartbeat fresh while this instance owns it.

    The UPDATE is fenced (id + status='building'): once the row has been
    retired or replaced it affects 0 rows, which also tells this thread to
    stop -- a late heartbeat must never resurrect someone else's claim.
    """
    while not stop.wait(HEARTBEAT_INTERVAL):
        try:
            with connect(autocommit=True) as conn:
                n = conn.execute(
                    """
                    UPDATE index_build_jobs
                    SET heartbeat_at = now()
                    WHERE id = %s AND status = 'building'
                    """,
                    (job_id,),
                ).rowcount
            if n == 0:
                log.info("job %s: heartbeat fenced off (claim lost); stopping",
                         job_id)
                return
        except Exception:
            log.exception("job %s: heartbeat update failed", job_id)


def _start_build(job_id: uuid.UUID) -> None:
    stop_heartbeat = threading.Event()
    hb = threading.Thread(
        target=_heartbeat_loop, args=(job_id, stop_heartbeat),
        name=f"index-heartbeat-{job_id}", daemon=True,
    )
    bt = threading.Thread(
        target=_run_build, args=(job_id, stop_heartbeat),
        name=f"index-build-{job_id}", daemon=True,
    )
    hb.start()
    bt.start()


def _run_build(job_id: uuid.UUID, stop_heartbeat: threading.Event) -> None:
    try:
        error = None
        try:
            delay = _pre_ddl_delay_seconds()
            if delay > 0:
                log.info("job %s: test hook stalling %.1fs before the build",
                         job_id, delay)
                time.sleep(delay)

            log.info("job %s: acquiring build lock for %s", job_id, CREATE_INDEX_SQL)
            with connect(autocommit=True) as conn:
                # Session-level advisory lock held across the eligibility
                # re-check AND the DDL. Reclaim paths can only retire this
                # claim while this lock is free, so whatever the re-check sees
                # is still true when the CREATE INDEX runs.
                conn.execute("SELECT pg_advisory_lock(%s)", (BUILD_LOCK_KEY,))
                try:
                    still_mine = conn.execute(
                        "SELECT 1 FROM index_build_jobs "
                        "WHERE id = %s AND status = 'building'",
                        (job_id,),
                    ).fetchone()
                    if not still_mine:
                        # Lost eligibility while stalled: the row was retired
                        # and possibly re-claimed by another instance. Do NOT
                        # issue DDL and do NOT write anything back.
                        log.info(
                            "job %s: claim lost before DDL; "
                            "leaving without building",
                            job_id,
                        )
                        return
                    log.info("job %s: starting %s", job_id, CREATE_INDEX_SQL)
                    conn.execute(CREATE_INDEX_SQL)
                finally:
                    _release_build_lock(conn)
        except Exception as exc:  # e.g. canceled, deadlock, connection lost
            error = f"CREATE INDEX CONCURRENTLY failed: {exc}"
            log.warning("job %s: %s", job_id, error)

        if error is None:
            # Report ready only after the catalog confirms validity + definition.
            try:
                with connect(autocommit=True) as conn:
                    cat = _catalog_state(conn)
                if _catalog_ok(cat):
                    _finish(job_id, "ready", None)
                    log.info("job %s: index verified valid in catalog -> ready",
                             job_id)
                    return
                error = f"catalog verification failed after build: {cat}"
            except Exception as exc:
                error = f"catalog verification error: {exc}"
            log.warning("job %s: %s", job_id, error)

        _finish(job_id, "failed", error)
    finally:
        # The claim's liveness proof ends with the build thread, on every
        # path (success, failure, or lost-claim backoff).
        stop_heartbeat.set()


# --------------------------------------------------------------------------
# Responses
# --------------------------------------------------------------------------

def _resp(state, job, active, cat, detail="", **extra):
    r = {
        "state": state,
        "detail": detail,
        "index_name": INDEX_NAME,
        "job_id": str(job["id"]) if job else None,
        "active_build_backends": active,
        "catalog": cat,
        "lease_seconds": LEASE_SECONDS,
        "handled_by": INSTANCE_ID,
    }
    if job:
        r["job"] = {
            "id": str(job.get("id")),
            "status": job.get("status"),
            "builder": job.get("builder"),
            "error": job.get("error"),
            "created_at": (job["created_at"].isoformat()
                           if job.get("created_at") else None),
            "updated_at": (job["updated_at"].isoformat()
                           if job.get("updated_at") else None),
            "heartbeat_at": (job["heartbeat_at"].isoformat()
                             if job.get("heartbeat_at") else None),
            "heartbeat_age_s": (round(job["heartbeat_age_s"], 1)
                                if job.get("heartbeat_age_s") is not None else None),
        }
    r.update(extra)
    return r


# --------------------------------------------------------------------------
# Public operations
# --------------------------------------------------------------------------

def request_build() -> dict:
    """Idempotently request the index build.

    Concurrent callers (across all instances) collapse onto a single build:
    the first one claims it, the others observe 'building'.
    """
    with connect(autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (DECISION_LOCK_KEY,))
        try:
            return _request_build_locked(conn)
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (DECISION_LOCK_KEY,))


def _request_build_locked(conn) -> dict:
    job = _get_job(conn)
    cat = _catalog_state(conn)
    active = _active_builds(conn)
    build_lock_held = False
    try:
        # 1) A build is already claimed. A mere timeout never recycles it:
        #    only a claim that provably lost eligibility is retired here.
        if job and job["status"] == "building":
            if _claim_status(conn, job, active) == "live":
                return _resp("building", job, active, cat,
                             detail="build already in progress")
            # Orphaned claim. We now hold the build lock, so a late thread of
            # the old claim serializes behind us and will back off inside its
            # own eligibility re-check instead of issuing DDL.
            build_lock_held = True
            _transition(conn, job["id"], "failed",
                        "orphaned build: heartbeat expired, no active backend, "
                        "no build lock held", "building")
            log.info("retired orphaned build claim %s", job["id"])
            job = _get_job(conn)

        # 2) Already built: confirm against the catalog before saying ready.
        if job and job["status"] == "ready":
            if _catalog_ok(cat):
                return _resp("ready", job, active, cat,
                             detail="index verified valid in pg_catalog")
            _transition(conn, job["id"], "failed",
                        "catalog mismatch: index missing/invalid or definition changed",
                        "ready")
            job = _get_job(conn)
            cat = _catalog_state(conn)

        # 3) Clean up invalid leftovers -- but never while a build is active:
        #    an invalid catalog entry may simply be an in-progress CONCURRENTLY
        #    build, which must not be treated as junk.
        dropped_leftover = False
        if cat["exists"] and not cat["valid"]:
            if active > 0:
                return _resp("building", job, active, cat,
                             detail="invalid catalog entry belongs to an in-progress "
                                    "build; left untouched")
            conn.execute(DROP_INDEX_SQL)
            dropped_leftover = True
            log.info("dropped invalid leftover index %s", INDEX_NAME)
            cat = _catalog_state(conn)

        # 4) Clear the failed job row so a fresh claim can be inserted.
        if job and job["status"] == "failed":
            conn.execute(
                "DELETE FROM index_build_jobs WHERE id = %s AND status = 'failed'",
                (job["id"],),
            )
            job = None

        # 5) A valid index may already exist (created outside this tool, or the
        #    orphaned build completed server-side before being retired).
        if cat["exists"] and cat["valid"]:
            if _catalog_ok(cat):
                conn.execute(
                    """
                    INSERT INTO index_build_jobs (id, index_name, status, builder)
                    VALUES (%s, %s, 'ready', %s)
                    ON CONFLICT (index_name) DO NOTHING
                    """,
                    (uuid.uuid4(), INDEX_NAME, INSTANCE_ID),
                )
                return _resp("ready", _get_job(conn), active, cat,
                             detail="valid index already present")
            return _resp("conflict", job, active, cat,
                         detail=f"index name {INDEX_NAME} is used by a different "
                                f"definition: {cat['definition']}")

        # 6) Claim the build. The UNIQUE constraint guarantees a single winner.
        new_id = uuid.uuid4()
        cur = conn.execute(
            """
            INSERT INTO index_build_jobs (id, index_name, status, builder)
            VALUES (%s, %s, 'building', %s)
            ON CONFLICT (index_name) DO NOTHING
            """,
            (new_id, INDEX_NAME, INSTANCE_ID),
        )
        if cur.rowcount == 1:
            _start_build(new_id)
            return _resp("started", _get_job(conn), active, cat,
                         detail="build claimed by this instance",
                         dropped_invalid_leftover=dropped_leftover)
        job = _get_job(conn)
        return _resp(job["status"], job, active, cat,
                     detail="build claimed concurrently by another instance")
    finally:
        if build_lock_held:
            _release_build_lock(conn)


def get_status() -> dict:
    """Report job + live catalog state; converge orphaned claims to failed."""
    with connect(autocommit=True) as conn:
        job = _get_job(conn)
        cat = _catalog_state(conn)
        active = _active_builds(conn)
        note = ""
        if job and job["status"] == "building":
            # Same eligibility verdict as build/cleanup: a live claim is
            # reported as-is, a provably orphaned one is retired.
            if _claim_status(conn, job, active) == "orphan_locked":
                try:
                    _transition(conn, job["id"], "failed",
                                "orphaned build: heartbeat expired, no active "
                                "backend (status check)", "building")
                    log.info("status check retired orphaned build claim %s",
                             job["id"])
                    job = _get_job(conn)
                    note = "orphaned build marked failed"
                finally:
                    _release_build_lock(conn)

        if job is None:
            if not cat["exists"]:
                state = "not_started"
            elif _catalog_ok(cat):
                state = "untracked_valid"
            else:
                state = "invalid_leftover"
        elif job["status"] == "ready":
            # Never report ready unless the catalog confirms it right now.
            state = "ready" if _catalog_ok(cat) else "inconsistent"
        else:
            state = job["status"]
        return _resp(state, job, active, cat, detail=note)


def cleanup() -> dict:
    """Drop invalid leftover indexes once no build is active; allow retry."""
    with connect(autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (DECISION_LOCK_KEY,))
        build_lock_held = False
        try:
            job = _get_job(conn)
            cat = _catalog_state(conn)
            active = _active_builds(conn)
            if job and job["status"] == "building":
                # Same eligibility verdict as build/status: never clean under
                # a claim that still has build eligibility.
                if _claim_status(conn, job, active) == "live":
                    return _resp("busy", job, active, cat,
                                 detail="build claim still eligible; "
                                        "refusing to clean up",
                                 cleaned=False)
                build_lock_held = True
                _transition(conn, job["id"], "failed",
                            "orphaned build: heartbeat expired, no active "
                            "backend (cleanup)", "building")
                log.info("cleanup retired orphaned build claim %s", job["id"])
                job = _get_job(conn)
            dropped = False
            if cat["exists"] and not cat["valid"]:
                if active > 0:
                    return _resp("busy", job, active, cat,
                                 detail="active DDL detected; refusing to clean up",
                                 cleaned=False)
                conn.execute(DROP_INDEX_SQL)
                dropped = True
                log.info("cleanup: dropped invalid leftover index %s", INDEX_NAME)
                cat = _catalog_state(conn)
            cleared = False
            if job and not (job["status"] == "ready" and _catalog_ok(cat)):
                conn.execute(
                    "DELETE FROM index_build_jobs WHERE id = %s AND status <> 'ready'",
                    (job["id"],),
                )
                cleared = True
                job = None
            return _resp("cleaned", job, active, cat,
                         detail="invalid leftovers removed" if dropped
                                else "nothing to clean",
                         cleaned=True, dropped_invalid_index=dropped,
                         cleared_job=cleared)
        finally:
            if build_lock_held:
                _release_build_lock(conn)
            conn.execute("SELECT pg_advisory_unlock(%s)", (DECISION_LOCK_KEY,))


def reconcile() -> None:
    """Run at startup: make tracked state consistent with the actual index."""
    try:
        with connect(autocommit=True) as conn:
            conn.execute("SELECT pg_advisory_lock(%s)", (DECISION_LOCK_KEY,))
            try:
                job = _get_job(conn)
                if not job:
                    log.info("reconcile: no tracked job")
                    return
                cat = _catalog_state(conn)
                active = _active_builds(conn)
                if job["status"] == "building":
                    # Same eligibility verdict as the request-time APIs: a
                    # live claim (another instance is heartbeating/building)
                    # is left alone.
                    verdict = _claim_status(conn, job, active)
                    if verdict == "live":
                        log.info("reconcile: build %s still eligible, keeping state",
                                 job["id"])
                        return
                    try:
                        if _catalog_ok(cat):
                            _transition(conn, job["id"], "ready", None, "building")
                            log.info("reconcile: build %s had completed; marked ready",
                                     job["id"])
                        else:
                            _transition(conn, job["id"], "failed",
                                        "orphaned build: owner gone before completion",
                                        "building")
                            log.info("reconcile: orphaned build %s marked failed",
                                     job["id"])
                    finally:
                        _release_build_lock(conn)
                elif job["status"] == "ready" and not _catalog_ok(cat):
                    _transition(conn, job["id"], "failed",
                                "index missing/invalid at startup", "ready")
                    log.info("reconcile: job %s was ready but catalog disagrees; "
                             "marked failed", job["id"])
            finally:
                conn.execute("SELECT pg_advisory_unlock(%s)", (DECISION_LOCK_KEY,))
    except Exception:
        log.exception("reconcile failed")
