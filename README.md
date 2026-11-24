# 地震测线道序服务（单测线）

用 FastAPI + SQLite 维护一条地震测线的波形道顺序，支持版本化移段和
跨版本定位：处理程序换版后，仍可凭旧版目录找到每道对应的原始道号。

## 1. 数据模型与复杂度

核心是一棵**持久化隐式 treap**（按子树大小定位，节点不可变）：

- 节点表 `treap_nodes`：每节点存原始道号 `value`、左右孩子、子树大小、
  随机堆优先级。一个版本就是一个根节点 id。
- 移段 = 两次 `split` + 若干 `merge`；持久化结构只在受影响的根到叶
  路径上新建节点，未触碰的子树在新旧版本间共享。
- 另维护两张**只反映现行版**的镜像表：
  - `current_parent(node_id, parent_id)`：现行树父指针，支持由节点
    O(log n) 反算当前道位；
  - `current_node(original_trace, node_id)`：原始道号 → 现行节点。
  每次移段只沿变更路径做 O(log n) 行 upsert（新旧树并行 diff，遇到
  相同子树即剪枝），不做全表重建、不做 O(n) 删除。

| 操作 | 索引读 | 索引写 |
|---|---|---|
| 初始化（仅一次） | O(n) | O(n) |
| 一次移段 | O(log n) | O(log n) |
| 道位 → 原始道号（任意已发布版） | O(log n) | 0 |
| 原始道号 → 现行道位 | O(log n) | 0 |

读写量均不随总道数、移段长度或历史版本数线性增长（历史版本只是
`versions` 表里一行根 id，旧树节点永久保留、整体共享）。

实测（n = 1,000,000）：初始化约 5 s；移动 20 万道写 156 行、移动
30 万道写 124 行；单次定位平均 0.30 ms；库文件约 53 MB。

## 2. 并发与持久性

- SQLite 开启 WAL；移段在单个 `BEGIN IMMEDIATE` 事务内完成，进程内
  写锁串行化。过期版号（`base_version` 非现行版）返回 409，不产生
  新版；任何写失败整体回滚，同样不产生新版。
- 查询使用独立只读连接，在一个 `BEGIN` 快照事务内完成，整次查询
  看到同一已提交状态，不受并发移段影响。
- 全部状态在 SQLite 文件内，重启后任意已发布版仍可查。

## 3. API（交互式文档见 `/docs`）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/line/initialize` | 填总道数，建立第 1 版（原始道号 1..N 原序） |
| POST | `/line/move` | 提交移段，产生下一版 |
| GET  | `/line/lookup?version=&position=` | 任一已发布版某道位 → 原始道号 + 现行版号/道位 |
| GET  | `/line/traces/{original_trace}` | 原始道号 → 现行版号/道位 |
| GET  | `/line/versions` | 版本历史 |
| GET  | `/line` | 测线元数据 |
| GET  | `/health` | 健康检查 |

`POST /line/move` 请求体：

```json
{
  "base_version": 2,
  "start": 50001,
  "end": 350000,
  "insert_after": 0
}
```

- `start`/`end`：要抽出的首尾道位，**含首尾**，按 `base_version`
  的道序解释；段内顺序保持不变。
- `insert_after`：抽出后插入位置（基于**提交时该版的原始道位编号**
  语义，校验拒绝指向被抽段内部）；`0` 表示插到最前，`N` 表示接在
  末尾。
- 成功返回新版本号；版号过期返回 `409`；参数非法返回 `422`。

`GET /line/lookup` 返回示例：

```json
{
  "query_version": 1,
  "query_position": 250000,
  "original_trace": 250000,
  "current_version": 3,
  "current_position": 850000
}
```

## 4. 本地运行

```bash
uv venv .venv
uv pip install --python .venv/bin/python -r requirements.txt
DB_PATH=./line.db .venv/bin/uvicorn app.main:app --reload
# 打开 http://localhost:8000/docs
```

## 5. Docker Compose

```bash
docker compose up -d --build
docker compose logs -f
# 数据保存在命名卷 line-data -> /data/line.db
```

## 6. 测试

```bash
uv pip install --python .venv/bin/python pytest httpx
.venv/bin/python -m pytest                 # 全部测试（含百万道规模测试）
SCALE_FAST=1 .venv/bin/python -m pytest    # 规模测试改用 10 万道快速跑
SKIP_SCALE=1 .venv/bin/python -m pytest    # 跳过规模测试
```

测试覆盖：treap 原语与持久化不变量、API 全流程、过期提交/非法参数/
写失败回滚、并发写竞争、快照读、重启持久性、EXPLAIN QUERY PLAN
无全表扫描，以及百万道下两次大段移动后的旧版定位交叉核对。
