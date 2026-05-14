# config 包说明

本目录保存系统运行时配置文件。

- `scheduler.yaml`：调度器监听地址、任务超时时间、Worker 失效阈值、负载均衡策略等配置。
- `worker.yaml`：Worker 默认连接的调度器地址、心跳间隔、并发任务数量等配置。

配置文件被 `distributed_scheduler.common.config_loader` 读取。课程展示时可以直接修改 YAML 文件，观察系统行为变化，例如切换 `round_robin`、`least_loaded`、`weighted_score` 三种调度策略。
