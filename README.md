# Edge API Gateway（本地可运行）

一个零外部依赖的本地边缘网关：对外提供**统一入口**，把请求按确定性规则转发到若干**本地模拟上游**，
并在并发访问下给出一致、可解释的路由、限流与故障转移结果。

- **动态路由 / 上游选择**：按路径、方法、Header、Query、调用方匹配；显式优先级；冲突与无匹配都返回可区分错误。
- **按调用方限流 / 配额**：固定窗口、原子"判定+消费"，并发与上游变慢都不会超额；拒绝请求不触碰上游。
- **健康探测 / 故障转移**：连续失败摘除、连续探测成功恢复；非幂等请求绝不重试；并发探测去重、防抖动。
- **可解释**：每个响应和测试日志都打印输入、判定依据（trace）与最终实例选择。

## 1. 运行

```bash
uv sync
uv run uvicorn main:app --reload
# http://127.0.0.1:8000
```

两个入口行为完全一致（同一请求在任何入口下命中同一规则）：

```bash
curl -s -H 'x-caller-id: alice' http://127.0.0.1:8000/users/1
curl -s -H 'x-caller-id: alice' http://127.0.0.1:8000/gateway/users/1
```

调用方通过 `x-caller-id` Header 传递（缺省为 `anonymous`）。

## 2. 路由匹配优先级

规则的所有条件之间是 **AND** 语义；可匹配维度：`path_prefix`、`methods`、
`header_equals`、`query_equals`、`callers`。

选择规则：

1. 找出所有完全匹配的规则。
2. 取其中 `priority` **数值最大**者。
3. 若最高分有**多条**规则并列，判定为冲突 → 返回 `409 ROUTE_CONFLICT`，
   响应中列出 `conflicting_rule_ids`，**绝不随机挑选、绝不静默丢弃**。
4. 若没有任何规则匹配 → 返回 `404 NO_ROUTE`。

> 规则 id 只用于日志/展示的稳定排序，不会被用来"悄悄打破"真实冲突。
> 演示规则中故意放置了一对同优先级冲突规则（`conflict-alpha` / `conflict-beta`）。

内置规则（见 `edge_gateway/config.py`）：

| 规则 | 优先级 | 匹配 | 集群 | 配额/窗口 |
| --- | --- | --- | --- | --- |
| users-canary | 500 | GET `/users` 且 `x-canary: true` | users-v2 | 10 / 1s |
| users-write | 400 | POST/PUT/PATCH/DELETE `/users` | users-v1 | 5 / 1s |
| users-read | 300 | GET `/users` | users-v1 | 8 / 1s |
| orders-vip | 250 | `/orders` 且 caller=vip | orders-v2 | 6 / 1s |
| orders-default | 200 | `/orders` | orders-v1 | 4 / 1s |
| conflict-alpha/beta | 100 | `/conflict` 且 `x-experiment: A`（两条并列） | — | 无 |

集群内实例按配置中的固定顺序尝试，**没有随机选择**。

## 3. 限流与配额语义

- 配额维度：**(调用方 caller, 规则 rule)**，各调用方相互独立。
- 固定窗口算法：一个窗口内前 N 个请求被接受并各消费 1 个单位；第 N+1 个起返回
  `429 QUOTA_EXCEEDED`，响应详情给出 `used/limit/remaining/window_start/window_end`。
- **判定与消费在同一把锁内原子完成**，且发生在调用上游**之前**：
  - 并发请求下接受数永远不超过 N（见 `test_concurrent_requests_never_exceed_quota`）；
  - 上游变慢不会让额外请求挤进来（见 `test_slow_upstream_still_enforces_quota`）；
  - 被拒绝请求不占用任何上游资源（执行计数不变，见
    `test_rejected_requests_consume_no_upstream_resource`）。
- 窗口随时间滚动，新窗口重新计数（可注入时钟测试，见 `test_window_rolls_over`）。
- 规则未配置 `quota_limit` 时不限流。

## 4. 健康检查、摘除与恢复

每个实例是一个状态机：

```
CLOSED（可服务）
   │ 连续失败达到 failure_threshold（默认 3）
   ▼
OPEN（已摘除，不参与选择；一次性产生摘除事件，重复失败不重复摘除）
   │ 距摘除时间达到 probe_interval（默认 50ms），探测被原子认领（全局最多一个在途探测）
   ▼
PROBING → 探测：
   ├─ 失败：恢复连胜清零，回到 OPEN，重新计时
   └─ 成功：恢复连胜 +1；未达到 recovery_threshold（默认 2）则仍留在 OPEN，
            达到后才回到 CLOSED
```

- **防抖动**：一次侥幸探测成功不足以恢复，必须连续 `recovery_threshold` 次成功。
- **并发去重**："探测到期 + 认领"在同一临界区完成，多个请求同时触发也只有一个真正探测，
  不会出现重复摘除 / OPEN↔CLOSED 反复抖动。
- 成功响应会清零该实例的连续失败计数。

### 故障转移与非幂等安全

| 上游结果 | 含义 | 幂等方法 (GET/HEAD/OPTIONS) | 非幂等方法 (POST/…) |
| --- | --- | --- | --- |
| 连接不可达 (`down`) | handler **尚未执行** | 尝试下一个实例 | 可安全尝试下一个实例 |
| HTTP 5xx (`fail`) | handler **已执行** | 尝试下一个实例 | 直接 `502 NON_IDEMPOTENT_NOT_RETRIED`，绝不重试 |
| 超时 (`timeout`) | 是否执行**不可确认** | 尝试下一个实例 | 直接 `502 NON_IDEMPOTENT_NOT_RETRIED` |

- 尝试次数上限 `max_attempts=3`；所有实例都不可用时返回
  `503 ALL_UPSTREAMS_UNAVAILABLE`，并带尝试明细。
- 非幂等请求的"最多执行一次"由模拟上游的用户请求执行计数在测试中断言。

## 5. 可观测 / 管理端点

| 方法与路径 | 说明 |
| --- | --- |
| `GET /healthz` | 网关存活 |
| `GET /__routes` | 全部规则与集群 |
| `GET /__explain?path=/users/1&method=GET` | 只做路由解释，不转发（逐条条件、命中集合、冲突/选择结论） |
| `GET /__admin/state` | 所有实例健康状态 |
| `POST /__admin/faults` | 注入/清除故障：`{"instance_id":"users-v1-1","fault":"fail"\|"timeout"\|"down"\|null}` |
| `POST /__admin/reset` | 重置配额、健康状态、执行计数与故障注入 |

普通转发响应包含 `rule_id / cluster / instance_id / upstream_status / data / quota / attempts / trace`。

错误响应统一为：

```json
{"ok": false, "error": {"code": "ROUTE_CONFLICT", "message": "...", "details": {...}}}
```

错误码：`NO_ROUTE(404)`、`ROUTE_CONFLICT(409)`、`QUOTA_EXCEEDED(429)`、
`NON_IDEMPOTENT_NOT_RETRIED(502)`、`ALL_UPSTREAMS_UNAVAILABLE(503)`。

## 6. 本地验证

```bash
uv run pytest -s                       # 全量测试并打印每个用例的输入/判定依据/结果
uv run pytest tests/test_quota.py -s   # 只看限流（含并发与慢上游）
uv run pytest tests/test_failover.py -s  # 只看摘除/恢复/非幂等
```

手工演练（先启动 uvicorn）：

```bash
# 1) 正常转发
curl -s -H 'x-caller-id: bob' localhost:8000/users/1 | jq .

# 2) 路由冲突
curl -s -H 'x-experiment: A' localhost:8000/conflict | jq .

# 3) 限流：连发 6 个请求，第 5、6 个应为 429
for i in $(seq 1 6); do curl -s -o /dev/null -w '%{http_code}\n' \
  -H 'x-caller-id: q1' localhost:8000/orders/1; done

# 4) 故障注入 + 故障转移（应落到 users-v1-2）
curl -s -X POST localhost:8000/__admin/faults \
  -H 'content-type: application/json' \
  -d '{"instance_id":"users-v1-1","fault":"fail"}'
curl -s -H 'x-caller-id: bob' localhost:8000/users/1 | jq '.instance_id, .attempts'

# 5) 查看恢复与重置
curl -s localhost:8000/__admin/state | jq .
curl -s -X POST localhost:8000/__admin/reset
```

## 7. 代码结构

```
edge_gateway/
  models.py    # Rule / RequestContext / ForwardResult 等数据结构
  errors.py    # 五类可区分网关错误
  config.py    # 内置路由规则与集群
  upstream.py  # 本地模拟上游 + 故障注入 + 用户请求执行计数
  router.py    # 规则匹配、优先级、冲突/无匹配判定与 explain
  quota.py     # 按 (caller, rule) 的固定窗口配额（线程安全）
  health.py    # CLOSED/OPEN/PROBING 状态机、并发探测去重、防抖
  gateway.py   # 路由→配额→健康感知分发→故障转移 的主流水线
  app.py       # FastAPI 统一入口与管理端点
tests/         # 路由 / 限流 / 故障转移 / HTTP 四组测试
```
