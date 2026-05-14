# 基于 gRPC 的分布式任务调度与负载均衡系统

这是一个用于分布式操作系统课程作业的 Python + gRPC 项目。系统由客户端、调度器和多个 Worker 节点组成，支持任务提交、节点心跳、负载均衡、任务执行和结果查询。

## 系统角色

- Client：提交任务、查询任务、查看 Worker 状态。
- Scheduler：中心调度器，维护任务队列和 Worker 状态，并根据负载均衡策略分配任务。
- Worker：工作节点，定期发送心跳，主动拉取任务，执行任务后回传结果。

## 目录结构

```text
config/                         配置文件
proto/                          gRPC 接口定义
scripts/                        辅助脚本
distributed_scheduler/common/   公共工具
distributed_scheduler/generated/自动生成的 gRPC Python 文件
distributed_scheduler/scheduler/调度器
distributed_scheduler/worker/   Worker 节点
distributed_scheduler/client/   命令行客户端
```

每个分包目录下都包含 README 文件，说明该包的作用。

## 快速运行

安装依赖：

```bash
pip install -r requirements.txt
```

生成 gRPC 文件：

```bash
python scripts/generate_grpc.py
```

启动调度器：

```bash
python -m distributed_scheduler.scheduler.server
```

启动两个 Worker：

```bash
python -m distributed_scheduler.worker.worker_node --worker-id worker-1 --listen-port 50061
python -m distributed_scheduler.worker.worker_node --worker-id worker-2 --listen-port 50062
```

提交任务：

```bash
python -m distributed_scheduler.client.cli submit --type sleep --payload 3 --name demo-sleep
python -m distributed_scheduler.client.cli submit --type fibonacci --payload 30 --name demo-fib
python -m distributed_scheduler.client.cli submit --type word_count --payload "hello grpc distributed system"
```

查看状态：

```bash
python -m distributed_scheduler.client.cli tasks
python -m distributed_scheduler.client.cli workers
```

## 负载均衡策略

在 `config/scheduler.yaml` 中修改 `strategy` 字段：

- `round_robin`：轮询分配。
- `least_loaded`：优先分配给正在运行任务最少的 Worker。
- `weighted_score`：综合 CPU、内存和并发占用率计算负载分数，选择分数最低的 Worker。

## 适合报告展示的点

- gRPC 接口设计：`proto/task_scheduler.proto`
- Worker 心跳与失效检测
- 任务状态流转：queued -> running -> succeeded / failed / timed_out
- 多 Worker 并发执行
- 不同负载均衡策略的效果对比
- 系统可扩展方向：任务持久化、优先级队列、容器化部署、Web 可视化界面


整体工作流程                                                                                                                                                             
                                                                                                                                                                           
  项目采用三类进程：Client、Scheduler、Worker。核心接口定义在 proto/task_scheduler.proto:159，主要 RPC 包括 SubmitTask、WorkerHeartbeat、PullTask、ReportTaskResult、      
  ListWorkers。                                                                                                                                                            
                                                                                                                                                                           
1. 生成 gRPC 代码                                                                                                                                                        
   proto/task_scheduler.proto 定义通信协议，运行：                                                                                                                       
                                                                                                                                                                           
   python scripts/generate_grpc.py                                                                                                                                       
                                                                                                                                                                           
   会生成：                                                                                                                                                              
    - distributed_scheduler/generated/task_scheduler_pb2.py                                                                                                              
    - distributed_scheduler/generated/task_scheduler_pb2_grpc.py
                                                                                                                                                                           
2. 启动 Scheduler                                                                                                                                                        
   Scheduler 读取 config/scheduler.yaml:16，创建 TaskStore，启动 gRPC 服务。入口在 distributed_scheduler/scheduler/server.py:1。                                         
                                                                                                                                                                           
   Scheduler 负责：                                                                                                                                                      
    - 保存任务队列                                                                                                                                                       
    - 维护 Worker 心跳状态                                                                                                                                               
    - 判断 Worker 是否存活                                                                                                                                               
    - 执行负载均衡选择                                                                                                                                                   
    - 接收 Worker 执行结果                                                                                                                                               
    - 回收超时任务                                                                                                                                                       
3. 启动 Worker                                                                                                                                                           
   Worker 读取 config/worker.yaml:13，启动本地健康检查服务，并连接 Scheduler。核心类是 distributed_scheduler/worker/worker_node.py:30。                                  
                                                                                                                                                                           
   Worker 启动后同时运行两个循环：                                                                                                                                       
    - 心跳循环：distributed_scheduler/worker/worker_node.py:114                                                                                                          
    - 拉取任务循环：distributed_scheduler/worker/worker_node.py:136                                                                                                      
4. Client 提交任务                                                                                                                                                       
   客户端通过 SubmitTask 提交任务，入口在 distributed_scheduler/client/cli.py:101。Scheduler 收到任务后调用 distributed_scheduler/scheduler/task_store.py:48，生成       
   task_id，把任务状态设为 QUEUED，并放入 FIFO 队列。                                                                                                                    
5. Worker 上报心跳                                                                                                                                                       
   Worker 定期调用 WorkerHeartbeat，上报：                                                                                                                               
    - worker_id                                                                                                                                                          
    - 地址                                                                                                                                                               
    - 最大并发数                                                                                                                                                         
    - 当前运行任务数                                                                                                                                                     
    - CPU 使用率                                                                                                                                                         
    - 内存使用率                                                                                                                                                         
                                                                                                                                                                           
   Scheduler 通过 distributed_scheduler/scheduler/task_store.py:86 新增或更新 Worker 状态。                                                                              
6. Worker 主动拉取任务                                                                                                                                                   
   当前系统采用“Worker 主动拉取，Scheduler 中心决策”的模式。Worker 调用 PullTask，Scheduler 执行 distributed_scheduler/scheduler/task_store.py:119。                     
                                                                                                                                                                           
   调度过程是：                                                                                                                                                          
    - 检查任务是否超时，需要的话重新入队
    - 检查当前 Worker 是否存在                                                                                                                                           
    - 检查 Worker 是否存活                                                                                                                                               
    - 检查 Worker 是否还有并发容量                                                                                                                                       
    - 从所有可用 Worker 中执行负载均衡选择                                                                                                                               
    - 只有当前拉取任务的 Worker 正好是被选中的最优 Worker，才会分配任务                                                                                                  
    - 任务状态从 QUEUED 变为 RUNNING                                                                                                                                     
7. Worker 执行任务并回传结果                                                                                                                                             
   Worker 调用 distributed_scheduler/worker/executor.py:35 执行任务。目前支持：                                                                                          
    - sleep：distributed_scheduler/worker/executor.py:61                                                                                                                 
    - fibonacci：distributed_scheduler/worker/executor.py:77                                                                                                             
    - word_count：distributed_scheduler/worker/executor.py:96                                                                                                            
                                                                                                                                                                           
   执行完成后，Worker 通过 ReportTaskResult 回传结果，Scheduler 调用 distributed_scheduler/scheduler/task_store.py:167 保存结果，并把任务状态改为 SUCCEEDED 或 FAILED。  
                                                                                                                                                                           
  任务调度机制                                                                                                                                                             
                                                                                                                                                                           
  当前任务队列是 FIFO 队列，具体弹出逻辑在 distributed_scheduler/scheduler/task_store.py:247。也就是说，任务默认按照提交顺序执行。                                         
                                                                                                                                                                           
  任务状态流转：                                                                                                                                                           
                                                                                                                                                                           
  QUEUED -> RUNNING -> SUCCEEDED                                                                                                                                           
  QUEUED -> RUNNING -> FAILED                                                                                                                                              
  QUEUED -> RUNNING -> 超时重新 QUEUED                                                                                                                                     
                                                                                                                                                                           
  配置中虽然保留了 priority 字段，但当前版本只是预留，尚未实现优先级队列。                                                                                                 
                                                                                                                                                                           
  Worker 是否可调度由两个条件决定：                                                                                                                                        
                                                                                                                                                                           
- 存活：最后一次心跳距离当前时间不超过 worker_ttl_seconds，判断逻辑在 distributed_scheduler/scheduler/task_store.py:259                                                  
- 有容量：running_tasks < max_concurrent_tasks，判断逻辑在 distributed_scheduler/common/models.py:84                                                                     
                                                                                                                                                                           
  超时任务回收由 distributed_scheduler/scheduler/task_store.py:207 完成。默认配置是 config/scheduler.yaml:24。                                                             
                                                                                                                                                                           
  负载均衡策略                                                                                                                                                             
                                                                                                                                                                           
  负载均衡统一实现在 distributed_scheduler/scheduler/load_balancer.py:15。当前支持三种策略，配置项是 config/scheduler.yaml:16。                                            
                                                                                                                                                                           
1. round_robin：轮询策略                                                                                                                                                 
   实现在 distributed_scheduler/scheduler/load_balancer.py:54。                                                                                                          
                                                                                                                                                                           
   算法逻辑：                                                                                                                                                            
                                                                                                                                                                           
   将可用 Worker 按 worker_id 排序                                                                                                                                       
   使用内部游标 index                                                                                                                                                    
   每次选择 workers[index % worker_count]                                                                                                                                
   index 自增                                                                                                                                                            
                                                                                                                                                                           
   特点：                                                                                                                                                                
    - 简单公平                                                                                                                                                           
    - 不关心 CPU、内存和真实负载                                                                                                                                         
    - 适合 Worker 性能接近、任务耗时接近的场景                                                                                                                           
                                                                                                                                                                           
2. least_loaded：最少运行任务数策略                                                                                                                                      
   实现在 distributed_scheduler/scheduler/load_balancer.py:65。                                                                                                          
                                                                                                                                                                           
   排序依据：                                                                                                                                                            
                                                                                                                                                                           
   running_tasks 越少越优先                                                                                                                                              
   CPU 使用率越低越优先                                                                                                                                                  
   内存使用率越低越优先                                                                                                                                                  
   worker_id 用于结果稳定                                                                                                                                                
                                                                                                                                                                           
   等价于：                                                                                                                                                              
                                                                                                                                                                           
   min(workers, key=(running_tasks, cpu_percent, memory_percent, worker_id))                                                                                             
                                                                                                                                                                           
   特点：                                                                                                                                                                
    - 比轮询更关注当前任务压力                                                                                                                                           
    - 对长短任务混合场景更合理                                                                                                                                           
    - CPU 和内存只是 tie-breaker，不是主要指标                                                                                                                           
                                                                                                                                                                           
3. weighted_score：综合加权负载策略                                                                                                                                      
   实现在 distributed_scheduler/scheduler/load_balancer.py:82，负载分数由 distributed_scheduler/common/models.py:89 计算。                                               
                                                                                                                                                                           
   公式是：                                                                                                                                                              
                                                                                                                                                                           
   concurrency_ratio = running_tasks / max_concurrent_tasks * 100                                                                                                        
                                                                                                                                                                           
   load_score =                                                                                                                                                          
       cpu_percent * 0.4                                                                                                                                                 
     + memory_percent * 0.3                                                                                                                                              
     + concurrency_ratio * 0.3                                                                                                                                           
                                                                                                                                                                           
   Scheduler 选择 load_score 最低的 Worker。                                                                                                                             
                                                                                                                                                                           
   特点：                                                                                                                                                                
    - 同时考虑 CPU、内存、并发占用率                                                                                                                                     
    - 当前默认策略                                                                                                                                                       
    - 更适合展示“资源感知型负载均衡”                                                                                                                                     
    - 权重可在报告中解释为实验参数，后续可根据任务类型调整                                                                                                               
                                                                                                                                                                           
   当前设计特点                                                                                                                                                             
                                                                                                                                                                           
   这个项目不是 Scheduler 主动推送任务给 Worker，而是 Worker 主动拉取任务。好处是 Worker 不需要暴露复杂任务接收接口，节点扩缩容也更简单；Scheduler 仍然保留中心化决策能力， 
   所以负载均衡逻辑依然集中、清晰。                                                                                                                                         
                                                                                                                                                                           
   当前版本适合课程展示，但不是生产级分布式系统。主要简化点是：任务和 Worker 状态保存在 Scheduler 内存中，进程重启后会丢失；priority 字段预留但未真正参与调度；没有持久化队 
   列或多 Scheduler 高可用。  
