# common 包说明

`common` 包保存调度器、Worker 和客户端都会使用的公共代码。

主要文件：

- `config_loader.py`：读取 YAML 配置文件，并提供默认值兜底。
- `logging_utils.py`：统一日志格式，便于课堂演示时观察多个进程输出。
- `models.py`：定义内部任务、Worker、枚举转换等数据结构。
- `time_utils.py`：提供毫秒级 Unix 时间戳工具。

公共包的目标是避免三个角色重复实现基础逻辑，使核心业务代码更聚焦于“任务调度”和“负载均衡”。
