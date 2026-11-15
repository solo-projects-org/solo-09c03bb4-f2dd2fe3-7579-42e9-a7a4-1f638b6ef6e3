# 双端 FASTQ.gz 接收 API

测序数据员把仪器导出的双端 `FASTQ.gz` 交给比对流水线前的单进程接收服务：按样本建批，
R1、R2 各以请求体**流式上传一次**，实时守字节上限、逐对校验读段，错配样本一律不放行。

- 框架：FastAPI + Uvicorn（单进程、单事件循环）
- 存储：SQLite（WAL 模式）+ 数据卷文件
- 交互文档：启动后访问 `http://localhost:8000/docs`（Swagger UI）与 `/openapi.json`

## 1. 启动

### Docker Compose（推荐）

```bash
docker compose up --build
```

数据保存在命名卷 `fastq-data`（容器内 `/data`），SQLite、压缩临时文件、展开后的 FASTQ 都在其中。
单批字节上限通过环境变量 `FASTQ_BATCH_BYTE_LIMIT` 配置（默认 64 MiB）。

### 本地直接运行

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
FASTQ_DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 配置项（均为环境变量，前缀 `FASTQ_`）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `FASTQ_DATA_DIR` | `/data` | 数据卷根目录（SQLite + 批次文件） |
| `FASTQ_BATCH_BYTE_LIMIT` | `67108864`（64 MiB） | 单批文件字节上限，压缩临时文件与展开 FASTQ 合计 |
| `FASTQ_UPLOAD_IDLE_TIMEOUT` | `15`（秒） | 上传请求体两个分块之间的最大静默时间 |

## 2. API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/batches` | 按 `sample_id` 创建批次，返回批次 ID |
| `GET` | `/batches` | 列出全部批次 |
| `GET` | `/batches/{id}` | 查询批次状态、已传文件与实时占用字节 |
| `POST` | `/batches/{id}/files/R1` | 流式上传 R1（请求体即 gzip 字节，非 multipart） |
| `POST` | `/batches/{id}/files/R2` | 流式上传 R2 |
| `POST` | `/batches/{id}/cancel` | 取消批次，等待写入停止、文件删除、空间释放后确认 |
| `GET` | `/batches/{id}/files/R1/download` | 下载展开后的 R1（仅 `ready` 批） |
| `GET` | `/batches/{id}/files/R2/download` | 下载展开后的 R2（仅 `ready` 批） |
| `GET` | `/health` | 健康检查 |

批次状态机：`receiving` →（两端齐全且配对通过）→ `ready`；任何校验失败 → `failed`；
操作员取消 → `cancelled`。三个终态都不可再上传。

## 3. 校验规则（任一不过，整批拒收并清理）

1. **单成员 gzip**：流式解压，gzip 结束标记后不允许再带数据（多成员归档拒绝）；截断/损坏拒绝。
2. **严格四行制 FASTQ**：`@读段名` / 序列 / `+` / 质量串，序列与质量串等长，文件以换行结尾。
3. **两端配对**：R1、R2 条数相同；逐对读段名取首个空白前的 token、去掉末尾 `/1`、`/2` 后必须一致。
4. 只有两端全部校验并保存成功，批次才置为 `ready` 并允许下载展开后的 FASTQ。

## 4. 字节上限与并发语义

- 接收过程中，**压缩临时文件与展开结果的每一次写入都先向配额器预留字节**，超过
  `FASTQ_BATCH_BYTE_LIMIT` 立即失败（HTTP 413），不会等整包落盘后才检查；解压炸弹同理在途中被拦。
- 配额按批记账。某批超限只清掉该批，其他批继续接收；慢上传占用的是协程而非线程，不挡其他批。
- 取消接口会先置取消信号、等待本批在途上传协程退出（此后不会再落盘），再删除全部文件、
  按磁盘真实占用回收空间、把状态落库为 `cancelled`，最后才返回确认。

## 5. 重启恢复

启动时扫描数据卷：

- `ready` 批：保留展开文件，按磁盘真实字节重建配额账，可继续下载；
- `receiving` 批（未完成）：判为 `failed`，删除残留文件并回收空间后再对外服务；
- `failed` / `cancelled` 批：清理任何残留；磁盘孤儿目录直接回收。

## 6. 样例与演示

生成正常、错配、超限、损坏四类样例：

```bash
python scripts/make_samples.py samples
```

对已启动的服务跑端到端演示（正常放行 / 错配拒收 / 超限 413）：

```bash
BASE_URL=http://127.0.0.1:8000 SAMPLES_DIR=samples bash scripts/demo.sh
```

典型手工调用：

```bash
BID=$(curl -s -X POST localhost:8000/batches -H 'Content-Type: application/json' \
  -d '{"sample_id":"S-2026-001"}' | python -c 'import json,sys;print(json.load(sys.stdin)["id"])')
curl -X POST localhost:8000/batches/$BID/files/R1 -H 'Content-Type: application/gzip' \
  --data-binary @samples/normal/R1.fastq.gz
curl -X POST localhost:8000/batches/$BID/files/R2 -H 'Content-Type: application/gzip' \
  --data-binary @samples/normal/R2.fastq.gz
curl localhost:8000/batches/$BID
curl -OJ localhost:8000/batches/$BID/files/R1/download
```

## 7. 测试

```bash
pip install -r requirements-dev.txt
pytest -q
```

- `tests/test_api.py` / `tests/test_lifecycle.py`：ASGI 级单元测试（正常、错配、条数不一致、
  序列质量不等长、坏 gzip、多成员 gzip、413 超限、取消中断、配额双计、重启恢复等）。
- `tests/test_server_integration.py`：拉起真实 uvicorn 进程，验证慢上传不挡其他批、
  取消「等待停止→删文件→释放空间」的端到端语义，以及正常/错配/超限全链路。

## 8. 目录结构

```
app/
  config.py       环境变量配置
  database.py     SQLite 连接与建表（WAL）
  models.py       状态枚举与运行期批次模型
  storage.py      按批配额器 SpaceManager、磁盘回收、批次注册表
  validation.py   流式 gunzip、四行制 FASTQ 结构与配对校验
  schemas.py      请求/响应模型
  service.py      批次生命周期：创建、上传、取消、下载、重启恢复
  main.py         FastAPI 入口
scripts/          样例生成与演示脚本
tests/            单元测试与真实服务器集成测试
docker-compose.yml / Dockerfile
```
