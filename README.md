# Edge API Gateway（Python）

一个可在本地完整运行的**边缘网关**：对外暴露统一入口，按确定性规则把请求
转发到若干本地模拟的上游服务，并在并发访问下给出**一致且可解释**的
路由、限流与故障转移结果。

- 统一入口：`http://127.0.0.1:8080`
- 调用方身份：请求头 `X-Caller-Id`
- 可解释响应头：`X-Gateway-Rule`（命中规则）、`X-Gateway-Instance`（最终实例）
- 每次请求的输入、判定依据、候选与尝试过程都会以 `gateway decision ...`
  日志打印（测试中同样打印）

---

## 1. 本地运行

需要 Python 3.14+ 与 [uv](https://docs.astral.sh/uv/)。

```bash
# 安装依赖
uv sync

# 一键启动 7 个模拟上游 + 网关（网关监听 8080）
uv run python -m sim.local_run
```

另开终端验证：

```bash
# 正常请求（带调用方身份）
curl -i -H 'X-Caller-Id: premium' http://127.0.0.1:8080/api/orders/123

# 查看规则判定表（不转发）
curl -s -X POST http://127.0.0.1:8080/_gw/explain \
  -H 'Content-Type: application/json' \
  -d '{"method":"GET","path":"/api/orders/1","headers":{"x-tenant":"acme"}}'

# 查看实例健康与摘除/恢复事件
curl -s http://127.0.0.1:8080/_gw/health | python3 -m json.tool

# 查看某调用方的限流/配额视图
curl -s http://127.0.0.1:8080/_gw/callers/demo-team | python3 -m json.tool
```

模拟上游支持运行时故障注入（端口见启动日志）：

```bash
# 让某实例业务返回 500 / 变慢 / 恢复
curl -s -X POST http://127.0.0.1:<port>/_sim/set_behaviour \
  -H 'Content-Type: application/json' -d '{"mode":"http_500"}'
curl -s -X POST http://127.0.0.1:<port>/_sim/set_behaviour \
  -H 'Content-Type: application/json' -d '{"mode":"slow","seconds":1.0}'
curl -s -X POST http://127.0.0.1:<port>/_sim/set_behaviour \
  -H 'Content-Type: application/json' -d '{"mode":"ok"}'
# 关闭/开启健康检查端点
curl -s -X POST http://127.0.0.1:<port>/_sim/set_healthy \
  -H 'Content-Type: application/json' -d '{"healthy":false}'
```

---

## 2. 路由匹配优先级

规则由三元组条件构成（见 `gateway/models.py` 的 `RouteRule`）：

- `path_prefix`：路径按 `/` **分段前缀**匹配（`/api/orders` 命中
  `/api/orders/123`，但不会误命中 `/api/orders-x`）；
- `methods`：非空时限定 HTTP 方法；
- `require_headers`：非空时要求请求头**全部相等**（头名大小写不敏感）。

命中后按**具体度**排序，优先级从高到低：

1. 路径分段数多者优先；
2. 相同则约束的请求头数量多者优先；
3. 再相同则“限定了方法”的规则优先；
4. 以上完全相同则按 `rule_id` 字典序兜底。

该排序只依赖规则与请求本身，因此**同一请求从任何入口进入，命中的都是
同一条规则**，与规则配置顺序、并发到达顺序无关。

异常显式失败，不静默、不随机：

| 情况 | 错误码 | HTTP |
|---|---|---|
| 没有任何规则命中 | `ROUTE_NO_MATCH` | 404 |
| 多条规则命中且具体度并列最高（无法裁决） | `ROUTE_AMBIGUOUS` | 409 |
| 规则引用了不存在的集群（配置错误） | 直接抛错（启动/请求期） | 500 |

默认配置刻意保留了一个可复现的冲突：对 `/api/orders` 同时携带
`X-Beta: true` 与 `X-Canary: true` 会命中 `r-orders-beta` 与
`r-orders-canary`（同为“2 段 + 1 个头约束”），返回 409：

```bash
curl -i -H 'X-Caller-Id: premium' -H 'X-Beta: true' -H 'X-Canary: true' \
  http://127.0.0.1:8080/api/orders
```

上游选择：命中集群内的**健康**实例先按 `instance_id` 排序，再用每集群的
原子轮转游标做起点轮换，以分散流量；它是“确定排序之上的确定移位”，
不引入随机。

---

## 3. 限流与配额语义（按调用方）

每个调用方（`X-Caller-Id`）有独立策略 `CallerPolicy`，三层控制彼此独立、
拒绝原因可区分：

| 控制 | 机制 | 拒绝码 | HTTP |
|---|---|---|---|
| 速率 | 令牌桶 `rate_per_second`，桶容量 `burst` | `RATE_LIMITED` | 429 |
| 配额 | 固定窗口 `quota_limit` / `window_seconds`，窗口结束自动重置 | `QUOTA_EXHAUSTED` | 403 |
| 并发 | 同时在途请求上限 `max_concurrency` | `CONCURRENCY_LIMITED` | 429 |
| 身份 | 缺失/未知调用方 | `CALLER_UNIDENTIFIED` | 401 |

关键语义与并发保证：

- **三层检查 + 预占在“每调用方一把锁”内原子完成**。并发请求不可能
  同时看到“最后一个配额”而双双放行；配额未耗尽的请求一定被接受，
  耗尽后的请求一定被拒绝。
- 被拒绝的请求在限流步骤立即返回，**不选择上游、不建立任何上游连接**，
  因此不占用上游资源（有测试用上游执行计数断言）。
- **上游变慢也不会超额**：并发槽位最先预占，慢响应导致在途占满时，
  新请求以 `CONCURRENCY_LIMITED` 快速失败，而不是排队堆积。
- 请求结束释放**并发槽位**；速率令牌与配额**不回补**——“被接受过”
  本身就计入配额。`RATE_LIMITED` 响应体给出 `retry_after_seconds`，
  `QUOTA_EXHAUSTED` 给出窗口重置时刻 `window_reset_at`。
- 默认 `demo-team` 策略较严格（burst=2 / 配额=10/分钟 / 并发=2），
  `premium` 宽松，便于在本地对比。

---

## 4. 健康检查、摘除与恢复

实例只有两种状态：`HEALTHY` 与 `SUSPENDED`（见 `gateway/health.py`）。

- **被动摘除**：真实流量中连续失败（连接错误 / 超时 / 5xx）达到
  `failure_threshold`（默认 3）次，实例被暂时摘除并产生一次 `suspend`
  事件；之后不再进入候选。4xx 视为上游正常应用响应（透传，不计失败）。
- **主动探测与恢复**：摘除后等待 `initial_cooldown`（默认 0.5s），
  到期允许探测一次 `GET /healthz`；成功则产生一次 `recover` 事件并恢复。
- **指数退避**：探测失败则保持摘除，退避翻倍
  （0.5s → 1s → 2s → 4s …，上限 `max_cooldown` 默认 8s）。
- **单飞探测（single-flight）**：退避到期的同一时刻即便有大量并发请求，
  实际也只会发起**一次**探测，其余请求等待并复用同一结果；
  因此不会重复摘除、也不会在 HEALTHY/SUSPENDED 之间反复抖动。
- 后台探测线程默认每 0.2s 扫描一次到期实例；所有实例都摘除时，
  请求路径也会同步触发一次单飞探测，给“恰好恢复”的实例即时机会。

### 故障转移与“非幂等不重复执行”

- **幂等方法**（GET/HEAD/OPTIONS/PUT/DELETE）：连接错误、超时、5xx 会按
  候选顺序尝试下一个健康实例；全部失败时按最后一次结果返回
  `UPSTREAM_TIMEOUT`(504) / `UPSTREAM_ERROR`(502) / `UPSTREAM_UNAVAILABLE`(503)。
- **非幂等方法**（POST/PATCH 等）：**至多在一个实例上执行一次**。
  即使是“连接拒绝”这种看似没发出去的错误，也无法在协议层证明服务端
  未执行，故绝不自动切换重试，直接失败并在错误明细中标注
  `"non_idempotent_no_retry": true`。模拟上游按方法统计执行次数，
  测试据此断言 POST 在故障转移中只被执行一次。

---

## 5. 错误码总览

| 错误码 | HTTP | 含义 |
|---|---|---|
| `CALLER_UNIDENTIFIED` | 401 | 缺失/未知 `X-Caller-Id` |
| `ROUTE_NO_MATCH` | 404 | 无规则匹配 |
| `ROUTE_AMBIGUOUS` | 409 | 命中多条同具体度规则，无法裁决 |
| `RATE_LIMITED` | 429 | 超出令牌桶速率 |
| `CONCURRENCY_LIMITED` | 429 | 超出调用方在途并发 |
| `QUOTA_EXHAUSTED` | 403 | 固定窗口配额耗尽 |
| `UPSTREAM_UNAVAILABLE` | 503 | 无可用上游（含非幂等不重试） |
| `UPSTREAM_ERROR` | 502 | 上游 5xx 且重试耗尽 |
| `UPSTREAM_TIMEOUT` | 504 | 上游超时且重试耗尽 |

所有错误体形如：

```json
{"error": {"code": "QUOTA_EXHAUSTED", "message": "...", "details": { ... }}}
```

`details` 中给出判定依据（如冲突规则列表、尝试过的实例、配额用量等）。

---

## 6. 运行测试

```bash
uv run pytest
```

测试覆盖（25 个）：

- `tests/test_rule_engine.py`：优先级、冲突、无匹配、20 次随机规则顺序下
  结果唯一；
- `tests/test_rate_limit.py`：burst 边界、**200 线程竞争配额=100 精确放行
  100**、窗口重置、慢上游下的并发上限、未知调用方不耗资源；
- `tests/test_health_machine.py`：阈值单次摘除、退避翻倍、
  **20 线程并发只发起一次探测**、恢复清零；
- `tests/test_health_failover.py`：真实 HTTP 500/连接拒绝下的幂等故障转移、
  探测恢复、停机/重启、**非幂等 POST 只执行一次**、全集群不可用、
  并发下摘除事件唯一；
- `tests/test_concurrency_slow_upstream.py`：上游变慢时并发上限不被突破、
  被拒不触达上游、配额记账精确；
- `tests/test_gateway_http.py`：经 FastAPI 入口的状态码、响应头解释、
  拒绝请求不增加上游计数、配额边界。

并发/定时相关测试使用**可控假时钟**驱动限流与健康状态机（真实线程与真实
锁仍保留），因此限流边界与恢复结论与机器速度无关、稳定可复现。
每个测试都会打印输入、判定依据与最终选择结果（使用 `-s` 即可看到）。

---

## 7. 代码结构

```
gateway/
  errors.py      稳定错误码与可区分错误
  models.py      请求特征/规则/集群/策略/判定日志等值对象
  rules.py       确定性规则匹配与冲突检测
  router.py      规则 -> 集群解析
  ratelimit.py   令牌桶 + 固定窗口配额 + 在途并发（每调用方原子预占）
  health.py      健康状态机 + 指数退避 + 单飞探测
  pool.py        健康过滤与确定性轮转候选选择
  forwarder.py   转发、故障转移、幂等感知的重试
  proxy.py       编排：识别->路由->限流预占->转发->释放，判定日志
  config.py      默认演示规则/集群/调用方策略
  app.py         FastAPI 统一入口与管理端点
sim/
  upstream.py    可控模拟上游（500/慢/健康开关/执行计数）
  local_run.py   多实例启停管理与一键运行
tests/           并发与抖动场景测试
```
