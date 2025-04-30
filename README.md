# VoidRail

VoidRail 的名称来自于古老的修仙界，是虚空传送阵的意思。

VoidRail 是一个基于 ZeroMQ 的轻量级微服务通信框架，采用 ROUTER-DEALER 模式实现服务发现和高可用性。它使用纯 JSON 作为数据交换格式，非常容易在分布式环境中部署和扩展。

VoidRail 的主要目标是实现 **CPU 密集型计算的分布式部署**，通过启动多个 DEALER 服务实例来实现并发控制，充分利用多核心和多机器资源。如果你需要进行 IO 密集型的应用开发，建议使用 FastAPI 等更适合的框架。

## 安装

使用 pip 安装：

```bash
pip install voidrail
```

或使用 poetry 安装：

```bash
poetry add voidrail
```

## 核心组件

VoidRail 采用三组件架构：

1. **ROUTER**：中央路由服务，通常通过命令行启动
2. **DEALER**：服务实现模块，需要自定义实现处理逻辑，可通过命令行启动多个实例
3. **CLIENT**：客户端模块，在宿主代码中使用，用于发送请求

这种组织方式基于 ZMQ 的核心概念，特别适合 CPU 密集型计算任务的分布式处理。

### 工作流程

```
ClientDealer --请求--> ServiceRouter --转发--> ServiceDealer
            <--响应-- ServiceRouter <--返回--
```

## 特性

- 使用 FIFO (先进先出) 模式分发请求，确保请求按顺序处理，适合 CPU 密集型计算
- 支持自动服务发现和注册（方便运维时手动下线或上线 DEALER 服务）
- 支持服务监控、健康检查和心跳机制
- 支持同步/异步方法和流式响应
- 使用 JSON 做数据交换
- 支持 API 密钥认证，提升服务安全性

## 快速入门

### 1. 创建自定义 DEALER 服务

首先创建一个 Python 模块，例如 `my_service.py`：

```python
from voidrail import ServiceDealer, service_method

class SimpleService(ServiceDealer):
    """简单的示例服务"""
    
    @service_method
    def echo(self, message):
        """简单的回显服务"""
        return f"收到消息: {message}"
    
    @service_method
    def add(self, a, b):
        """简单的加法服务"""
        return a + b
```

### 2. 使用命令行启动 ROUTER 和 DEALER

启动 ROUTER 服务（在一个终端中）：

```bash
voidrail router --host 0.0.0.0 --port 5555
```

启动多个 DEALER 服务实例（在另一个终端中）：

```bash
# 自动启动4个服务实例，充分利用多核 CPU
voidrail dealer --module my_service --class SimpleService --instances 4
```

### 3. 在应用中使用 CLIENT

在你的主应用中：

```python
import asyncio
from voidrail import ClientDealer

async def main():
    # 创建客户端连接
    client = ClientDealer(router_address="tcp://localhost:5555")
    
    # 调用回显服务
    result1 = await client.invoke("SimpleService.echo", "你好，世界！")
    print(result1)  # 输出: 收到消息: 你好，世界！
    
    # 调用加法服务
    result2 = await client.invoke("SimpleService.add", 40, 2)
    print(result2)  # 输出: 42
    
    await client.close()

# 运行主函数
asyncio.run(main())
```

## 使用场景

VoidRail 特别适合以下场景：

1. **AI 模型推理服务**：将大型 ML/DL 模型部署到多个 DEALER 实例，实现负载分散
2. **数据处理管道**：对大规模数据执行 CPU/GPU 密集型处理
3. **批量任务处理**：将需要长时间运行的任务分发到多个 DEALER 实例

## 命令行工具详解

大部分情况下，推荐使用命令行工具来启动 ROUTER 和 DEALER 服务，然后在应用代码中使用 CLIENT。

### 启动 ROUTER 服务

```bash
# 基本用法
voidrail router --host 0.0.0.0 --port 5555

# 启用 API 密钥认证
voidrail router --dealer-keys dealer_key1 --client-keys client_key1
```

### 启动 DEALER 服务实例

```bash
# 启动单个实例
voidrail dealer --module my_service --class SimpleService

# 启动多个实例（每个实例在独立进程中运行）
voidrail dealer --module my_service --class SimpleService --instances 4

# 使用 API 密钥认证
voidrail dealer --api-key your_dealer_key --module my_service
```

### 使用客户端命令行工具（调试用）

```bash
# 列出所有可用服务
voidrail client --list

# 查看路由器信息
voidrail client --router-info

# 调用服务方法（带参数）
voidrail client --call SimpleService.echo --args '"你好，世界！"'
```

## 分布式部署

VoidRail 特别适合分布式部署，可以将 DEALER 服务部署在多台机器上：

1. **部署 ROUTER**：在中央服务器上部署 ROUTER，确保所有 DEALER 和 CLIENT 都能访问

    ```bash
    voidrail router --host 0.0.0.0 --port 5555
    ```

2. **部署多台 DEALER**：在不同机器上启动 DEALER 服务，连接到同一个 ROUTER

    ```bash
    # 在机器 A
    voidrail dealer --host central_server_ip --port 5555 --module my_service
    
    # 在机器 B
    voidrail dealer --host central_server_ip --port 5555 --module my_service
    ```

3. **客户端连接**：在应用中连接到中央 ROUTER

    ```python
    client = ClientDealer(router_address="tcp://central_server_ip:5555")
    ```

## 安全建议

VoidRail 提供了基本的安全机制：

1. **API 密钥认证**：控制哪些 DEALER 和 CLIENT 能够连接到 ROUTER
2. **CURVE 加密**：可选启用 ZMQ 的 CURVE 加密，保护传输中的数据

在生产环境中，建议至少启用 API 密钥认证，并考虑将 ROUTER 放置在受保护的网络中。对于特别敏感的数据，可以启用 CURVE 加密提供更强的安全保障。

## 最佳实践

1. **合理设置实例数量**：DEALER 实例数通常应与可用 CPU 核心数匹配
2. **注意资源管理**：确保每个 DEALER 实例有足够的内存和 CPU 资源
3. **错误处理**：在 DEALER 方法中妥善处理异常，避免服务崩溃
4. **监控**：定期检查队列状态和服务健康，及时发现并解决问题
5. **合理拆分服务**：将不同类型的计算任务拆分为不同的 DEALER 服务类