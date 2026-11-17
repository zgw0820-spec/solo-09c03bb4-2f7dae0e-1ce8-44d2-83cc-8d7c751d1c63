# Device Fault Log Ops — 在线建索引平台

设备故障日志持续写入，运维通过 API 在线构建 `(device_id, occurred_at)` 索引（`CREATE INDEX CONCURRENTLY`），构建期间日志照常写入。两个无状态 API 实例通过 PostgreSQL 自身协调，保证同一索引的并发申请只启动一次构建。

## 架构

```
                ┌────────────┐        ┌────────────┐
   客户端 ─────▶│  api1      │        │  api2      │◀───── 客户端
   (:8001/docs) │ FastAPI    │        │ FastAPI    │ (:8002/docs)
                └─────┬──────┘        └─────┬──────┘
                      │      同一个数据库    │
                      └─────────┬───────────┘
                                ▼
                     ┌─────────────────────┐
                     │  PostgreSQL 16 (db) │
                     │  device_fault_logs  │  预置 400 万行故障日志
                     │  index_build_jobs   │  构建任务台账（唯一约束）
                     └─────────────────────┘
```

协调机制（全部落在数据库内，实例无状态、无领导者）：

| 需求 | 机制 |
| --- | --- |
| 并发申请只构建一次 | `INSERT ... ON CONFLICT (index_name) DO NOTHING`（唯一约束保证跨实例只有一个赢家）；临界区再套一层会话级 advisory lock（decision lock） |
| 构建期间日志照常写入 | `CREATE INDEX CONCURRENTLY`（不阻塞 DML），构建在独立后台线程 + 独立连接上执行 |
| 存活性证明（心跳租约） | 认领实例的后台心跳线程在构建全程刷新 `index_build_jobs.heartbeat_at`；心跳在 `ORPHAN_GRACE_SECONDS` 租约内即视为仍有构建资格。**仅凭认领超时（created_at 年龄）永远不会回收任务**——认领后停顿再久，只要实例活着、心跳在走，资格就保留 |
| 停顿/崩溃后的接管 | 仅当三者同时成立才判定孤儿并接管：心跳过期 **且** `pg_stat_activity`/`pg_stat_progress_create_index` 无活跃构建后端 **且** build advisory lock 空闲（try-lock 成功）。此时退休旧任务（failed；若目录证明构建已完成则 ready）、`DROP INDEX CONCURRENTLY IF EXISTS` 清残件、重新认领重试 |
| 迟到旧线程不得发起 DDL | 构建线程在整个”资格复查 + CREATE INDEX”期间持有会话级 build advisory lock；回收路径只有在该锁空闲时才能退休旧任务，且退休全程持有该锁。迟到的旧线程获取锁后先复查任务行：行已退休/被替换则**直接退出，不发 DDL、不写回任何结果** |
| 迟到旧作业不得写回旧结果 | 所有终态写入带围栏条件 `WHERE id = <job> AND status = 'building'`；任务行被退休/重建后旧作业的 UPDATE 命中 0 行。心跳 UPDATE 同样带围栏，命中 0 行即自动停止 |
| 构建中的无效目录项不得误删 | 清理前检查活跃构建后端与构建资格；有资格存活的任务在构建时，无效索引条目（进行中的 CONCURRENTLY 构建）绝不动 |
| 确认有效才报告就绪 | 构建完成后校验 `pg_index.indisvalid && indisready` 且 `pg_get_indexdef` 定义匹配 `(device_id, occurred_at)`，才置 `ready`；`GET /indexes/status` 每次也实时核对目录 |
| 三个 API 资格判断一致 | `/indexes/build`、`/indexes/status`、`/indexes/cleanup` 与启动 `reconcile()` 共用同一个资格判定（活跃后端 / 心跳租约 / build lock 三要素） |
| 重启状态一致 | 实例启动时 `reconcile()`：把台账与 `pg_catalog` 实际状态对齐（孤儿→failed、已完成→ready、目录矛盾→failed） |

已知限制：

- 实例崩溃后，其 `building` 记录需等 `ORPHAN_GRACE_SECONDS`（默认 20s，compose 中设 15s）租约过期后才被判定为孤儿——这是区分”实例仍活着（心跳在走）”与”实例确已退出”的必要窗口。
- 若实例**僵死但进程未退出**（如 SIGSTOP 永久挂起）且其构建线程恰好持有 build lock，其他实例不会强行接管（try-lock 失败即视为仍有资格）。这是安全与可用性的取舍：宁可等待，也绝不制造两个并发 `CREATE INDEX CONCURRENTLY`。进程真正退出（kill/崩溃/容器停止）时其数据库会话随之消失，锁自动释放，接管正常进行。

## 运行（Docker Compose）

```bash
docker compose up -d --build
# Swagger UI: http://localhost:8001/docs  或  http://localhost:8002/docs
# PostgreSQL: localhost:5433 (ops/ops, db=faultlogs)
```

首次启动时 `db/init.sql` 预置 400 万行日志（约需几十秒），db 健康检查会等到 schema 就绪后才放行 API 容器。

## API

| 方法/路径 | 说明 |
| --- | --- |
| `POST /logs` | 写入一条故障日志（`device_id`、`message` 必填；`severity`、`occurred_at` 可选） |
| `GET /logs` | 按 `device_id` + 时间范围查询（走新索引） |
| `GET /logs/plan` | `EXPLAIN` 上述查询，验证索引被使用 |
| `POST /indexes/build` | 幂等申请建索引；并发调用只会有一个 `started`，其余 `building`；已就绪返回 `ready` |
| `GET /indexes/status` | 任务状态 + 实时目录核对（`catalog.exists/valid/ready/definition`、活跃构建后端数、任务心跳 `heartbeat_at` 与租约 `lease_seconds`） |
| `POST /indexes/cleanup` | 确认无活跃构建且任务无构建资格后，删除无效残件索引并清理失败任务，允许重试 |
| `GET /health` | 健康检查 |

`POST /indexes/build` 返回的 `state`：`started`（本次认领成功）、`building`（他处已在构建）、`ready`（目录已确认有效）、`conflict`（同名索引定义不符，需人工处理）。

## 自测

`tests/selftest.py` 端到端验证：并发去重、构建期写入、无效目录项保护、崩溃→孤儿检测→清理→重试、迟到作业围栏、**停顿竞态（P7a：认领后停顿超过租约但实例存活 → 三个 API 一致判定仍有资格、绝不回收；P7b：实例冻结 → 租约过期 → 对端接管重试 → 原实例恢复后迟到线程不发 DDL、不写回旧结果）**、重启一致性、查询计划命中索引。

```bash
# Docker 模式（需要本机有 docker）：
docker compose up -d --build
python3 tests/selftest.py --mode docker \
  --dsn "postgresql://ops:ops@localhost:5433/faultlogs"

# 本地模式（无 docker，使用 pgserver 提供的 PostgreSQL 二进制）：
python3 -m venv /tmp/ops-venv && /tmp/ops-venv/bin/pip install -r requirements.txt pgserver
tests/run_local.sh
```

## 验证状态

`tests/selftest.py` 已对两个真实 API 实例 + 真实 PostgreSQL 16.2（400 万行预置数据）完整跑通两轮，**81/81 检查通过**，覆盖：并发申请去重（12 并发仅 1 次构建、活跃构建后端数恒为 1）、构建期 34 次写入零失败、构建中的无效目录项全程未被删除、就绪前目录校验、崩溃后服务端完成构建的自愈、构建被中断后的无效残件清理与重试、迟到旧作业围栏（UPDATE 命中 0 行）、**停顿竞态（P7a：认领后停顿 25s 超过 15s 租约但实例存活，status/build/cleanup 三个 API 一致判定仍有资格、任务行始终唯一未被回收，停顿结束后由原认领者自己建成；P7b：SIGSTOP 冻结实例 → 租约过期 → 对端判定孤儿并接管重建至 ready → SIGCONT 恢复后旧线程日志确认 `claim lost before DDL; leaving without building`，未发 DDL、未写回旧结果，目录始终保持唯一有效索引）**、启动 reconcile 与重启一致性、查询计划命中索引。另单独验证了老版本库的增量迁移（`ensure_schema()` 自动补 `heartbeat_at` 列与 `index_build_controls` 表）。

说明：本交付环境的沙箱没有 Docker，`docker-compose.yml`/`Dockerfile` 未实际构建运行（已做 YAML/语法校验）；上述验证以 local 模式（pgserver 提供的 PostgreSQL 16.2 + 两个本地 uvicorn 实例）完成，应用代码路径与 docker 模式完全一致。docker 模式下自测脚本同样支持 P7 的冻结/恢复（`docker compose pause/unpause`）。

## 目录

```
app/main.py           FastAPI 接口
app/index_manager.py  建索引协调核心（认领/围栏/清理/核对/重启对齐）
app/db.py             连接与 schema 等待
db/init.sql           建表 + 400 万行预置 + 任务台账表
tests/selftest.py     端到端自测（local/docker 两种编排）
tests/local_db.py     本地 pgserver 数据库引导
docker-compose.yml    db(postgres:16) + api1 + api2
```
