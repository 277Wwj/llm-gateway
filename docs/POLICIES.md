# 准入、资源和固定窗口熔断

网关只支持一个进程、一个 event loop、一个 worker。Redis 只共享 RPM 计数，**不提供多副本并发租约或分布式熔断**。

## RPM 和在途并发是两种约束

RPM 使用固定时长窗口：某个配置 key 的第一笔准入开始窗口，默认 60 秒。每次成功 acquire 计 1；本服务按供应商的上游尝试计数，重试再次消耗配额，逻辑请求数量由单独指标记录。之后取消不退款；超限调用不增加计数、不延长窗口。窗口边缘可以短时突发，这不是滑动窗口限流。调用方应使用有限的配置 key，不能直接使用任意客户端输入导致内存字典增长。

- `MemoryRateLimiter` 是显式选择的单进程演示后端。单 event loop 中一次 acquire 没有 await 切换点，所以检查和增加不可被同 loop 的其他请求插入。
- `RedisRateLimiter` 用一个 Lua EVAL 完成读取、判断、增加、设置 PTTL。时间窗口由 Redis TTL 决定，不依赖各客户端时钟。相同 Redis、prefix 和 key 的实例共享计数；key 做 SHA-256 后写入 Redis，不把原始调用标识写入 key 名。连接错误返回 `RateBackendError`，不会降级成内存。Lua 原子执行不代表 Redis 重启后保留配额；持久化、高可用属于运维范围。

供应商并发由 `ConcurrencyGate` 管理，拥有独立的在途上限和 FIFO 等待容量。排队满立即 `QueueFull`，到达传入的绝对截止时间返回 `QueueTimeout`。调用方要将排队、重试、HTTP 与流式读取放入同一个请求预算，不能每次重试重新计时。

队列取消会移除等待者；如果释放者刚把槽交给等待者、等待者就被取消，清理逻辑会把已预留的槽归还。`Lease.release()` 幂等且内部不挂起，避免释放中断造成泄漏。获得 lease 的调用方必须放进 finally；流式响应必须持续持有到正常结束、错误或客户端取消。

## 固定窗口错误率熔断

`CircuitBreaker` 按**完成时间**计数，使用不重叠的固定窗口。只有 success/failure 进入分母；非法参数、鉴权失败、队列拒绝、RPM 拒绝和客户端取消应为 neutral。连接错误、上游超时和供应商服务故障的分类由供应商适配和核心服务负责。达到 `min_samples` 且 failure / samples 大于等于 `failure_ratio` 时打开；窗口边界清零。它不是自适应算法，也不是滑动窗口。

冷却期间不放行；冷却结束进入 half_open，最多一个探测在途。探测成功关闭并重置统计，失败重新冷却，neutral 释放探测资格并保留 half_open。每次 allow 返回一个不透明 token，必须恰好在 finally 中结束记录；重复 record 不会重复计数。队列出队后、实际调用上游前必须调用 `is_current(token)` 再检查：如果排队期间熔断器已经打开，旧 token 作废，释放槽并记录 neutral，不能让旧队列继续冲击故障上游。旧状态中的延迟完成不允许关闭新一代熔断器。

调用 `snapshot()` 可读取 active/waiting 和 breaker 状态。指标标签只应包含受控供应商名、结果类别等有限集合，不应包含请求 ID、prompt 或 API key。

## 可复现验证

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_policies.py -q
# 只有这里显式设置真实 Redis，Lua 原子性集成测试才执行。
$env:LLMGW_TEST_REDIS_URL = 'redis://127.0.0.1:6379/15'
.\.venv\Scripts\python.exe -m pytest tests/test_policies.py -q
Remove-Item Env:LLMGW_TEST_REDIS_URL
```

真实 Redis 测试使用随机 prefix，两个独立连接池并发 64 次争取 8 个准入名额，并核验 TTL 到期后恢复；不会 FLUSHDB 或访问业务键。没有 Redis 环境变量时此测试明确 skip，其他测试不能替代 Redis 验证。HTTP 取消和资源释放另由服务验收测试验证。
