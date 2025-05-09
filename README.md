# VoidRail

VoidRail 的名称来自于古老的修仙界，是虚空传送阵的意思。

VoidRail 基于 Celery 构建轻量级分布式任务处理框架，专为 CPU 密集型计算设计。它提供简单易用的接口，让您可以快速构建和部署分布式计算服务。

## 安装

使用 pip 安装：

```bash
pip install voidrail
```

**依赖说明**  
VoidRail 默认使用 Redis 作为 Broker 和结果后端，因此在启动 Worker 或客户端前，需要先确保 Redis 服务已运行（默认端口为 6379）。可以使用以下方式启动 Redis：

```bash
# 本地启动（需先安装 Redis）
redis-server

# 或使用 Docker 启动
docker run -d --name voidrail-redis -p 6379:6379 redis
```

## 核心组件

VoidRail 采用两组件架构：

1. **Worker**：服务实现模块，继承 `CeleryWorker` 基类，定义处理逻辑
2. **Client**：客户端模块，使用 `CeleryClient` 类发送任务请求

## 基本使用

### 简单例子

```python
# echo.py
import time
from voidrail import create_app

app = create_app('echo')

@app.task(name='echo.say_hello')
def say_hello(name):
    """简单的问候任务"""
    return f"Hello, {name}! Current time: {time.ctime()}"

@app.task(name='echo.say_hello_delay', bind=True)
def say_hello_delay(self, name, delay=3):
    """带延迟的问候任务，演示任务状态更新"""
    self.update_state(state='PROGRESS', meta={'progress': 0, 'message': '开始处理'})
    
    # 模拟处理过程
    for i in range(10):
        time.sleep(delay / 10)
        self.update_state(state='PROGRESS', meta={
            'progress': (i + 1) * 10, 
            'message': f'处理中 {(i + 1) * 10}%'
        })
    
    return f"Hello after {delay} seconds, {name}! Time: {time.ctime()}"
```

### 命令行工具

安装完成后，你可以通过命令行来启动服务或调用任务，无需额外代码：

```bash
# 启动 Worker（加载本地文件自定义模块 echo.py）
python -m voidrail --module echo

# 调用任务
python -m voidrail call echo.say_hello -a World

# 查看帮助信息
python -m voidrail --help
```

### 使用客户端

你也可以通过代码来访问已经启动的服务。

```python
from voidrail.client import CeleryClient

# 创建客户端
client = CeleryClient(service_name="echo")

# 同步调用任务
result = client.call(
    task_name="say_hello",
    args=["World"]
)
print(result["result"])  # Hello, World!

# 异步调用任务
async_res = client.call(
    task_name="say_hello_delay",
    args=["Async World"],
    kwargs={"delay": 2},
    wait_result=False
)
task_id = async_res["task_id"]
print(f"任务已提交，ID: {task_id}")

# 查询任务状态
status = client.get_task_status(task_id)
print(f"任务状态: {status['status']}")

# 获取最终结果（可在状态为 completed 后调用）
if status["status"] == "completed":
    final_res = client.get_task_result(task_id)
    print(f"结果: {final_res}")

# 列出当前注册的任务
tasks = client.list_registered_tasks()
print("可用任务:", tasks)
```

### 水平扩展能力

VoidRail的一个主要优势是支持简单而强大的水平扩展。当您启动多个相同服务的Worker实例时：

1. **自动负载均衡**：所有实例会自动协作处理队列中的任务
2. **无需额外配置**：不需要任何特殊设置，只需启动更多相同的服务实例
3. **容错和高可用**：如果某个实例崩溃，其他实例会继续处理任务

例如，您可以在多台服务器上启动相同的服务：

```mermaid
graph TB
    Client[客户端]
    Queue[(Redis消息队列)]
    
    subgraph "服务器A"
    Worker1[Worker实例1]
    end
    
    subgraph "服务器B"
    Worker2[Worker实例2]
    Worker3[Worker实例3]
    end
    
    subgraph "服务器C"
    Worker4[Worker实例4]
    end
    
    Client -->|发送任务| Queue -->|分发任务| Worker1 & Worker2 & Worker3 & Worker4
```

## 运行多个Worker实例

要充分利用多核CPU，可以启动多个Worker实例：

```bash
# 启动Worker进程
# 通过环境变量控制并发度
CELERY_CONCURRENCY=4 python hello_service.py
```

您可以在不同的服务器上多次启动相同的服务实例：

```bash
# 在服务器A上
python hello_service.py

# 在服务器B上
python hello_service.py

# 在服务器C上
python hello_service.py
```

每个实例都会自动加入相同的worker池，共同处理任务队列。Celery会为每个worker分配一个唯一ID，
确保任务只会被处理一次。这种设计使VoidRail非常适合需要动态扩展的场景 - 随着负载增加，
只需启动更多的worker实例即可线性提高处理能力。
