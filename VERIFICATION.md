# 实际验证记录

以下汇总项目已有的 2026-11-19 验证结果。2026-11-21 整理此索引时未重新运行测试；数值以链接的原始记录为准。

环境：Windows、Python 3.12、独立虚拟环境。真实 Redis 验证使用 WSL 中的 Redis 7.4.12。上游模型为显式 mock，尚未取得真实 OpenAI / DeepSeek 调用验证。

## 自动化测试

- 全量测试：**79 passed in 116.51s**；[原始输出](artifacts/test-output.txt)、[JUnit](artifacts/test-results.xml)。该次 JUnit 为 79 项、0 失败、0 错误、0 跳过，包含真实 Redis 策略测试。
- Redis 策略专项：**16 passed in 2.70s**；[原始输出](artifacts/policies-redis-output.txt)、[JUnit](artifacts/policies-redis-junit.xml)。
- 依赖检查：[pip check](artifacts/pip-check.txt)。

未设置 `LLMGW_TEST_REDIS_URL` 的新运行会明确跳过真实 Redis 项，不能将该运行当作 Redis 验证。

## 故障与恢复

[故障实验原始记录](artifacts/faults.json)及[汇总输出](artifacts/fault-output.txt)记录了 **12 个 mock 故障实验全部通过**。覆盖重试与切换、首事件超时、流中断、断连、过载及熔断恢复；实验使用本机真实 HTTP 进程，上游响应由 mock 控制。

取消验证针对本地连接、并发槽及探测资格回收，不证明真实供应商停止计算或计费。

## 真实 Redis

[HTTP 原始记录](artifacts/redis-http.json)及[验证说明](docs/REDIS_VERIFICATION.md)：

- 两个网关进程共享 8 个 RPM 名额，16 个 HTTP 请求得到 8 个 200、8 个 429；mock 上游收到 8 次请求。
- Redis 不可用时返回 503，mock 上游收到 0 次请求，没有回退到内存后端。
- 收尾时 active、waiting 和半开探测 inflight 均为 0。

双进程实验只验证 Redis RPM 的跨进程原子性。并发槽与熔断状态仍在进程内，项目的部署范围仍为单实例、单 worker。

历史 Redis 源码、二进制、构建目录和临时日志不纳入仓库。复现时可连接已有 Redis，按 README 中 `LLMGW_TEST_REDIS_URL` 和 `scripts/verify_redis.py` 的说明运行；历史本机构建路径见验证说明。

## mock 基准

[原始数据](artifacts/benchmark.json)、[汇总输出](artifacts/benchmark-output.txt)。工作负载为 40 个非流式文本请求、并发 4、模拟上游延迟 50 ms，先直连再经过网关。

| 路径 | 成功数 | 总时长 | 完成请求每秒 | P50 | P95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 直连 mock | 40/40 | 0.665 s | 60.14 | 59.73 ms | 82.54 ms |
| 经过网关 | 40/40 | 0.881 s | 45.42 | 77.54 ms | 146.64 ms |

该短时本机样本只展示测试方法和当次观察结果，不代表生产吞吐或真实模型速度。耗时差异包含调度、连接池和排队，不能单独解释为网关 CPU 开销。

## AgentRAG 联调

[联调原始记录](artifacts/agentrag-integration.json)验证问答、草稿、确认与重复确认，重复确认返回同一模拟工单。链路经过真实 HTTP 适配，但终点仍为 mock，不属于真实 LLM 供应商验证。
