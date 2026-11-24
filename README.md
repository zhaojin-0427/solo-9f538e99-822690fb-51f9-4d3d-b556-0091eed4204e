# 地震测线道序服务（单测线）

用 FastAPI + SQLite 维护一条地震测线的波形道顺序，支持版本化移段和
跨版本定位：处理程序换版后，仍可凭旧版目录找到每道对应的原始道号。
另按**原始道号**登记坏道标记，并在现行版上按位置区间取最长可用波形段
（无坏道且原始道号逐道加一），供邻道去噪试算使用。

## 1. 数据模型与复杂度

核心是一棵**持久化隐式 treap**（按子树大小定位，节点不可变）：

- 节点表 `treap_nodes`：每节点存原始道号 `value`、左右孩子、子树大小、
  随机堆优先级。一个版本就是一个根节点 id。
- 移段 = 两次 `split` + 若干 `merge`；持久化结构只在受影响的根到叶
  路径上新建节点，未触碰的子树在新旧版本间共享。
- 另维护三张**只反映现行版**的镜像表：
  - `current_parent(node_id, parent_id)`：现行树父指针，支持由节点
    O(log n) 反算当前道位；
  - `current_node(original_trace, node_id)`：原始道号 → 现行节点；
  - `current_agg(node_id, ...)`：每节点的**取窗单子（monoid）摘要**
    （首尾原始道号、首尾道是否好、全段是否逐道加一、可用前缀/后缀/
    最长段长度及起点）。两子树摘要 O(1) 合并，区间取窗沿覆盖该区间的
    O(log n) 个 treap 节点折叠即可，不逐道扫描、不逐段比长度。
- 坏道表 `bad_traces(original_trace PRIMARY KEY)` 以原始道号为键。
  移段只搬道位、不改原始道号，因此标记天然跟着原道走，移段完全不碰
  这张表。

| 操作 | 索引读 | 索引写 |
|---|---|---|
| 初始化（仅一次） | O(n) | O(n) |
| 一次移段 | O(log n) | O(log n) |
| 道位 → 原始道号（任意已发布版） | O(log n) | 0 |
| 原始道号 → 现行道位 | O(log n) | 0 |
| 登记/解除一条坏道标记 | O(log n) | O(log n)（仅该道根→叶路径） |
| 区间取最长可用段 | O(log n) | 0 |

更正单道标记只重算该道到根的 O(log n) 个聚合行；移段只为本次新建的
O(log n) 个 treap 节点写聚合，**被搬走部分的共享子树一行都不重建**。
读写量均不随总道数、移段长度、取窗宽度或历史版本数线性增长（历史
版本只是 `versions` 表里一行根 id，旧树节点永久保留、整体共享）。

实测（n = 1,000,000）：初始化约 8 s；移动 20 万道写 156 行、移动
30 万道写 124 行；单次定位平均 0.32 ms；单道坏道更正约 0.5 ms；
50 万道宽段移动仅新增 90 行聚合；20 万道下取窗平均 0.55 ms
（全线窗口 0.40 ms、万道窗口 0.76 ms）；库文件约 60 MB。

## 2. 并发与持久性

- SQLite 开启 WAL；移段和坏道标记变更都在单个 `BEGIN IMMEDIATE`
  事务内完成，进程内写锁串行化。过期版号（`base_version` 非现行版）
  返回 409，不产生新版；任何写失败整体回滚，不产生新版、不产生半截
  标记或半截聚合。
- 查询（含取窗）使用独立只读连接，在一个 `BEGIN` 快照事务内完成，
  整次查询看到同一已提交状态——道序与坏道标记取自同一份快照，不受
  并发移段/标记变更影响。
- 全部状态在 SQLite 文件内，重启后任意已发布版、坏道标记与取窗结果
  均保持不变。

## 3. API（交互式文档见 `/docs`）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/line/initialize` | 填总道数，建立第 1 版（原始道号 1..N 原序） |
| POST | `/line/move` | 提交移段，产生下一版 |
| GET  | `/line/lookup?version=&position=` | 任一已发布版某道位 → 原始道号 + 现行版号/道位 |
| GET  | `/line/traces/{original_trace}` | 原始道号 → 现行版号/道位 |
| PUT  | `/line/bad-traces/{original_trace}` | 按原始道号登记坏道标记（幂等） |
| DELETE | `/line/bad-traces/{original_trace}` | 按原始道号解除坏道标记（幂等） |
| GET  | `/line/bad-traces` | 现行全部坏道原始道号 |
| GET  | `/line/window?start=&end=` | 现行版区间内最长可用段 |
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

坏道标记按原始道号登记/解除，重复登记或解除不存在的标记返回
`changed: false` 且不产生任何写入；标记不随道位变化，移段后仍指向
同一物理道。示例：

```json
PUT /line/bad-traces/7
→ {"original_trace": 7, "bad": true, "changed": true}
```

`GET /line/window?start=&end=` 填现行版首尾道位（含首尾）。可用段
要求段内无坏道且原始道号逐道加一；并列时取最靠前的一段；区间内没有
任何可用道时 `segment` 为 `null`。返回版号与该段的首尾道位、首尾
原始道号：

```json
{
  "version": 3,
  "window": [1, 20],
  "segment": {
    "start_position": 8,
    "end_position": 15,
    "start_original_trace": 13,
    "end_original_trace": 20,
    "length": 8
  }
}
```

无可用道时：

```json
{"version": 3, "window": [1, 3], "segment": null}
```

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

测试覆盖：treap 原语与持久化不变量、取窗 monoid 对暴力参考实现的
随机化核对、API 全流程（含带坏道移段后标记随原道走、解除标记后
运行段合并、并列取最前、无可用道返回空）、过期提交/非法参数/写失败
回滚、并发写竞争、快照读（取窗与标记取自同一快照）、重启持久性、
EXPLAIN QUERY PLAN 无全表扫描，以及百万道下单道更正与移段聚合维护
的 O(log n) 成本契约和取窗结果交叉核对。
