import pytest
import pytest_asyncio
import asyncio
import random
import logging
from voidrail import ServiceRouter, ServiceDealer, ClientDealer, DealerState, service_method

logger = logging.getLogger(__name__)

@pytest.fixture(autouse=True)
def setup_logging(caplog):
    """设置日志级别"""
    # 重置所有处理器的日志级别
    for handler in logging.getLogger().handlers:
        handler.setLevel(logging.DEBUG)
    # 设置 caplog 捕获级别
    caplog.set_level(logging.DEBUG)

@pytest.fixture()
def router_address():
    """返回路由器地址，使用随机TCP端口"""
    port = random.randint(45000, 49999)
    return f"tcp://127.0.0.1:{port}"

@pytest.fixture()
def test_config():
    """测试配置"""
    return {
        'heartbeat_interval': 0.5,   # 心跳间隔
    }

@pytest_asyncio.fixture
async def router(router_address, test_config):
    """创建并启动路由器"""
    router = ServiceRouter(
        router_address, 
        heartbeat_interval=test_config['heartbeat_interval']
    )
    await router.start()
    await asyncio.sleep(0.1)
    yield router
    # 在停止前等待一小段时间，确保能处理所有关闭请求
    await asyncio.sleep(0.5)
    await router.stop()

@pytest_asyncio.fixture
async def service(router, router_address, test_config):
    """创建并启动服务"""
    service = EchoService(
        router_address,
        heartbeat_interval=test_config['heartbeat_interval']
    )
    # 同步启动
    service.start()
    # 给点时间完成注册
    await asyncio.sleep(0.1)
    yield service
    # 同步停止
    service.stop()
    # 给router时间处理关闭确认
    await asyncio.sleep(0.1)

@pytest_asyncio.fixture
async def streaming_service(router, router_address, test_config):
    """创建并启动流式服务"""
    service = StreamingService(
        router_address,
        heartbeat_interval=test_config['heartbeat_interval']
    )
    # 同步启动
    service.start()
    # 给点时间完成注册
    await asyncio.sleep(0.1)
    yield service
    # 同步停止
    service.stop()
    # 给router时间处理关闭确认
    await asyncio.sleep(0.1)

@pytest_asyncio.fixture
async def client(router, service, router_address):
    """创建客户端"""
    # 确保服务已经注册
    assert service._state == DealerState.RUNNING, "Service not running"
    
    client = ClientDealer(router_address, timeout=2.0)
    try:
        yield client
    finally:
        await client.close()

@pytest_asyncio.fixture
async def streaming_client(router, streaming_service, router_address):
    """创建客户端，用于测试流式响应"""
    # 确保服务已经注册
    assert streaming_service._state == DealerState.RUNNING, "Service not running"
    
    client = ClientDealer(router_address, timeout=2.0)
    try:
        yield client
    finally:
        await client.close()

class BasicEchoService(ServiceDealer):
    """示例服务实现"""
    def __init__(
        self, 
        router_address: str, 
        heartbeat_interval: float = 0.5,
        **kwargs
    ):
        if 'heartbeat_interval' not in kwargs:
            kwargs['heartbeat_interval'] = heartbeat_interval
        super().__init__(
            router_address=router_address,
            **kwargs
        )

    @service_method
    async def echo(self, message: str) -> str:
        """简单回显服务"""
        await asyncio.sleep(0.1)
        logger.info(f"EchoService {self._service_id} echo: {message}")
        return message

class EchoService(BasicEchoService):
    """扩展回显服务，增加加法功能"""
    @service_method(
        name="add",
        description="Add two numbers",
        params={
            "a": "first number",
            "b": "second number"
        }
    )
    async def add_numbers(self, a: int, b: int) -> int:
        """带参数说明的加法服务"""
        await asyncio.sleep(0.01)
        return a + b

class StreamingService(BasicEchoService):
    """流式服务实现"""
    @service_method(name="stream")
    async def stream_numbers(self, start: int, end: int):
        """流式返回数字序列"""
        for i in range(start, end):
            yield i
            await asyncio.sleep(0.1)  # 模拟处理延迟

@pytest.mark.asyncio
async def test_simple_echo(client):
    """测试简单的回显服务"""
    message = "Hello, World!"
    async for response in client.stream("EchoService.echo", message):
        assert response == message
        break

@pytest.mark.asyncio
async def test_service_discovery(client):
    """测试服务发现"""
    available_methods = await client.discover_services()
    
    # 验证可用方法
    assert "EchoService.echo" in available_methods
    assert "EchoService.add" in available_methods
    
    # 验证方法描述信息
    add_info = available_methods["EchoService.add"]
    logger.info(f"add_info: {add_info}")
    assert add_info["description"] == "Add two numbers"
    assert "a" in add_info["params"]
    assert "b" in add_info["params"]

@pytest.mark.asyncio
async def test_streaming_response(streaming_client):
    """测试流式响应"""
    expected = list(range(0, 5))
    received = []
    
    async for response in streaming_client.stream("StreamingService.stream", 0, 5):
        received.append(response)
            
    assert received == expected
