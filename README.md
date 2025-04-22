# VoidRail

VoidRail 的名称来自于古老的修仙界，是虚空传送阵的意思。

VoidRail 是一个基于 ZeroMQ 的轻量级微服务通信框架，采用 ROUTER-DEALER 模式实现服务发现、负载均衡和高可用性。它使用纯 JSON 作为数据交换格式，非常容易在分布式环境中部署和扩展。

VoidRail 的主要目标是实现 CPU/GPU 密集型计算的分布式部署，尤其是与主服务的 fastapi 搭配使用，因此推荐使用默认的 FIFO 模式。

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

VoidRail 由三个主要组件构成：

- **ServiceRouter**：中央路由模块，管理服务注册、请求分发和健康监控
- **ServiceDealer**：服务实现模块，处理业务逻辑，支持同步/异步方法和流式响应
- **ClientDealer**：客户端访问，负责发现服务并发送请求

使用 ROUTER-DEALER 这些名词是为了与 ZMQ 的概念保持一致，建议用户使用时了解一些 ZMQ 的基本知识，这对于重度使用的用户来说非常必要。但 ZMQ 也是一个相当成熟的底层消息队列框架，如果你没有遇到阻碍，确实可以完全忽略这些专业名词。

### 工作流程

```
ClientDealer --请求--> ServiceRouter --转发--> ServiceDealer
            <--响应-- ServiceRouter <--返回--
```

## 特性

- 支持 FIFO 和负载均衡两种分发模式（前者适合CPU密集型计算，后者适合IO密集型计算）
- 支持自动服务发现和注册（这有利于你运维时手动下线或上线 DEALER 服务）
- 支持服务监控、健康检查和心跳机制
- 支持同步/异步方法和流式响应
- 使用 JSON 做数据交换（如果有必要，实际上你可以用其他语言来实现队列访问或提供服务）
- 支持 API 密钥认证，提升服务安全性

## 快速入门

### 1. 创建 Router（核心交换服务）

```python
from voidrail import ServiceRouter
from voidrail import RouterMode

# 创建并启动路由器
router = ServiceRouter(
    address="tcp://127.0.0.1:5555",  # 监听地址
    router_mode=RouterMode.FIFO,     # 或 RouterMode.LOAD_BALANCE
    heartbeat_timeout=5.0            # 心跳超时时间，这涉及判定对DELAER服务掉线，如果你的服务处理时间很长，最简单的办法是加大这个数值
)
await router.start()
```

### 2. 实现 Dealer（服务端）

```python
from voidrail import ServiceDealer, service_method

class EchoService(ServiceDealer):
    # 这些方法不需要全部定义，而是根据需要选择一个即可
    # 但你可以定义定义多个服务方法
    #
    # 同步方法
    @service_method
    def echo(self, message: str) -> str:
        return message
        
    # 异步方法
    @service_method
    async def async_echo(self, message: str) -> str:
        await asyncio.sleep(0.1)
        return message
        
    # 流式响应
    @service_method
    async def stream_numbers(self, start: int, end: int):
        for i in range(start, end):
            yield i
            await asyncio.sleep(0.1)
            
# 创建并启动服务
service = EchoService(router_address="tcp://127.0.0.1:5555")
await service.start()
```

### 3. 使用 Client（客户端）

```python
from voidrail import ClientDealer

# 创建客户端
client = ClientDealer(router_address="tcp://127.0.0.1:5555")

# 发现可用服务
available_methods = await client.discover_services()
print(f"可用方法: {available_methods}")

# 调用普通方法
result = await client.invoke("EchoService.echo", "Hello World")
print(f"结果: {result}")

# 使用流式响应
async for number in client.stream("EchoService.stream_numbers", 0, 5):
    print(f"收到数字: {number}")
```

## 认证

VoidRail 提供了 API 密钥认证机制，以提高服务的安全性。

### 1. 启用认证

在 Router 中启用认证：

```python
from voidrail import ServiceRouter, ApiKeyManager

# 生成密钥
dealer_key = ApiKeyManager.generate_key(prefix="dealer")
client_key = ApiKeyManager.generate_key(prefix="client")

# 创建带认证的 Router
router = ServiceRouter(
    address="tcp://127.0.0.1:5555",
    require_auth=True,
    dealer_api_keys=[dealer_key],  # 服务端密钥
    client_api_keys=[client_key]   # 客户端密钥
)
await router.start()
```

你也可以自己手工设定密钥，服务端不会对密钥做格式检查，但生成的密钥格式可能更符合最佳实践。

### 2. 配置服务认证

```python
# 创建带认证的服务
service = EchoService(
    router_address="tcp://127.0.0.1:5555",
    api_key=dealer_key  # 服务必须提供有效的 dealer_key
)
await service.start()
```

### 3. 配置客户端认证

```python
# 创建带认证的客户端
client = ClientDealer(
    router_address="tcp://127.0.0.1:5555",
    api_key=client_key  # 客户端必须提供有效的 client_key
)
await client.connect()
```

### 4. 通过环境变量配置认证

也可以通过环境变量设置认证：

```bash
# Router 环境变量
export VOIDRAIL_REQUIRE_AUTH=true
export VOIDRAIL_DEALER_API_KEYS=dealer_key1,dealer_key2
export VOIDRAIL_CLIENT_API_KEYS=client_key1,client_key2

# 客户端或服务的环境变量
export VOIDRAIL_API_KEY=your_api_key
```

## 分布式部署

VoidRail 天然支持分布式部署，实现方式如下：

### 1. 部署 Router

Router 需要在一个固定的、所有服务和客户端都能访问到的地址：

```python
# 监听所有网络接口
router = ServiceRouter(address="tcp://0.0.0.0:5555")
await router.start()
```

### 2. 部署多个 Dealer 服务

服务可以在不同机器上启动，只需连接到同一个 Router：

```python
# 服务器 A
service_a = EchoService(router_address="tcp://router_ip:5555")
await service_a.start()

# 服务器 B
service_b = EchoService(router_address="tcp://router_ip:5555")
await service_b.start()
```

### 3. 客户端连接

客户端只需要知道 Router 的地址：

```python
client = ClientDealer(router_address="tcp://router_ip:5555")
await client.connect()
```

## 服务监控

VoidRail 提供了内置的监控功能：

### 查看路由器信息

```python
router_info = await client.get_router_info()
print(f"路由器模式: {router_info['mode']}")
print(f"活跃服务数: {router_info['active_services']}")
```

### 查看队列状态

```python
queues = await client.get_queue_status()
for method, status in queues.items():
    print(f"方法 {method}: 队列长度 {status['queue_length']}, 空闲服务数 {status['available_services']}")
```

## 关键概念详解

### ROUTER-DEALER 通信架构

- **ROUTER** ZMQ 的核心概念之一，可以简单理解为路由服务总线，一般可以将其放在你的主服务中启动
- **DEALER** ZMQ 的核心概念之一，可以简单理解为需要你自定义的服务处理端，在分布式架构中可以根据CPU核心情况独立启动
- **CLIENT** 在 VoidRail 框架中 CLIENT 是一种特殊的 DEALER，一般也是在主服务中启动

实际上该框架使用了典型的 `ROUTER-DEALER` 通信架构来完成跨服异步双向通信。

该架构需要启动一个 ROUTER 端作为路由中心，由至少一个 DEALER 端负责处理，然后就可以由 CLIENT 端调用，组成完整服务了。

### 服务派发模式

路由策略，也就是服务派发策略，由 router_mode 参数决定。

- **FIFO 模式**：保证同一个方法的请求按顺序处理，适合需要严格顺序的应用
- **负载均衡模式**：基于服务当前负载分配请求，适合追求最高吞吐量的应用

### 服务方法类型

你至少应该对 Python 的异步行为有一定了解，不过实现时你也可以将服务方法定义为同步的。

- **同步方法**：直接返回结果，适合简单计算
- **异步方法**：使用 `async/await`，适合 I/O 密集型操作
- **流式响应**：通过 `yield` 产生多个结果，适合大数据传输或实时数据流

### 健康检查

- 服务通过定期心跳保持活跃状态
- Router 自动检测失效服务并停止向其转发请求
- 服务实例自动尝试重新连接 Router

## API 参考

### ServiceRouter

```python
ServiceRouter(
    address: str,                          # 监听地址
    heartbeat_timeout: float = 30.0,       # 心跳超时（秒）
    router_mode: RouterMode = RouterMode.FIFO,  # 路由模式
    require_auth: bool = None,           # 是否要求认证
    dealer_api_keys: List[str] = None,   # 允许的 DEALER 端 API 密钥列表
    client_api_keys: List[str] = None,   # 允许的 CLIENT 端 API 密钥列表
)
```

主要方法：
- `start()`: 启动路由器
- `stop()`: 停止路由器

### ServiceDealer

```python
ServiceDealer(
    router_address: str,                   # 路由器地址
    context: Optional[zmq.Context] = None, # ZMQ 上下文
    max_concurrent: int = 100,             # 最大并发请求数
    heartbeat_interval: float = 0.5,       # 心跳间隔
    service_name: str = None,              # 服务名称
)
```

主要方法：
- `start()`: 启动服务
- `stop()`: 停止服务
- `@service_method` 装饰器：标记服务方法

### ClientDealer

```python
ClientDealer(
    router_address: str,                   # 路由器地址
    context: Optional[zmq.Context] = None, # ZMQ 上下文
    timeout: Optional[float] = None,       # 请求超时
)
```

主要方法：
- `connect()`: 连接到 Router
- `close()`: 关闭连接
- `discover_services()`: 发现可用服务
- `invoke(method, *args, **kwargs)`: 调用方法并返回结果
- `stream(method, *args, **kwargs)`: 流式调用方法
- `get_router_info()`: 获取路由器信息
- `get_queue_status()`: 获取队列状态

## 命令行工具

VoidRail提供了便捷的命令行工具，无需编写代码即可启动和管理组件。

### 启动Router服务

```bash
# 基本用法
voidrail router --host 0.0.0.0 --port 5555

# 启用认证
voidrail router --require-auth --dealer-keys dealer_key1 --client-keys client_key1

# 生成API密钥
voidrail router --generate-keys
```

### 使用客户端工具

```bash
# 列出所有可用服务
voidrail client --list

# 查看路由器信息
voidrail client --router-info

# 查看队列状态
voidrail client --queue-status

# 传递参数的不同方式:

# 1. 使用字符串参数（注意引号嵌套）
voidrail client --call EchoService.hello --args '"Hello World"'

# 2. 使用数字参数
voidrail client --call MathService.square --args '42'

# 3. 使用位置参数列表
voidrail client --call MathService.add --args '[5, 3]'

# 4. 使用关键字参数（最灵活的方式）
voidrail client --call EchoService.greet --args '{"name":"John", "title":"Dr."}'

# 5. 使用复杂的嵌套结构
voidrail client --call DataService.process --args '{"config":{"max_items":100}, "filters":["active", "recent"]}'

# 使用API密钥认证
voidrail client --api-key your_client_key --list
```

### 常见参数传递问题和解决方法

1. **如果使用单引号包裹JSON**（推荐）:
   ```bash
   voidrail client --call EchoService.hello --args '{"name":"John"}'
   ```

2. **如果使用双引号包裹JSON，需要转义内部引号**:
   ```bash
   voidrail client --call EchoService.hello --args "{\"name\":\"John\"}"
   ```

3. **传递单个字符串时，需要额外的引号层**:
   ```bash
   voidrail client --call EchoService.hello --args '"John"'  # 正确
   voidrail client --call EchoService.hello --args 'John'    # 错误
   ```

### 启动Dealer服务

假设您有一个自定义服务类在 `myapp/services.py` 文件中：

```python
from voidrail import ServiceDealer, service_method

class MyService(ServiceDealer):
    @service_method
    async def hello(self, name: str) -> str:
        return f"Hello, {name}!"
```

#### 启动单个服务实例

```bash
# 基本用法
voidrail dealer --module myapp.services --class MyService

# 自动推断类名（当模块中只有一个ServiceDealer子类时）
voidrail dealer --module myapp.services

# 指定连接参数
voidrail dealer --host 192.168.1.100 --port 5555 --module myapp.services --class MyService

# 使用API密钥认证
voidrail dealer --api-key your_dealer_key --module myapp.services --class MyService
```

#### 启动多进程服务实例（优化CPU利用率）

```bash
# 为一个服务类启动多个实例（每个实例在独立进程中运行）
voidrail dealer --module myapp.services --class MyService --instances 4

# 自动推断类名并启动多个实例（当模块中只有一个ServiceDealer子类时）
voidrail dealer --module myapp.services --instances 4

# 启动多个不同服务类（每个类在独立进程中运行）
voidrail dealer --module myapp.services --class MyService --class OtherService

# 启动多个不同服务类，每个类多个实例
voidrail dealer --module myapp.services --class MyService --class OtherService --instances 2

# 结合其他参数
voidrail dealer --host 192.168.1.100 --port 5555 --module myapp.services \
  --class MyService --class OtherService --instances 4 \
  --max-concurrent 50 --api-key your_dealer_key
```

多进程启动功能允许您：
- 充分利用多核CPU资源
- 在单台机器上轻松管理多个服务实例
- 实现更高的服务吞吐量

## 最佳实践

1. **错误处理**：主动在服务方法中捕获异常，否则处理错误和异常信息会作为结果发送到客户端，这可能不够优雅
2. **资源管理**：使用异步上下文管理器（`async with`）自动处理连接生命周期
3. **监控**：在生产环境中定期检查队列状态和服务健康
4. **超时设置**：为客户端请求设置合适的超时时间，尤其是CPU密集型服务
5. **消息大小**：避免在单个请求中传递过大的数据