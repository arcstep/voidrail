import pytest
import pytest_asyncio
import asyncio
import logging
import time
import zmq.asyncio

from voidrail import ServiceDealer, RouterMode, ServiceRouter, ClientDealer, service_method

logger = logging.getLogger(__name__)

@pytest.fixture(scope="module")
def router_address():
    """测试用路由器地址"""
    return "inproc://router_method_types_test"

@pytest.fixture
def zmq_context():
    """创建共享的ZMQ Context"""
    context = zmq.asyncio.Context.instance()
    yield context

@pytest_asyncio.fixture
async def router(router_address, zmq_context):
    """创建并启动路由器"""
    router = ServiceRouter(
        router_address,
        context=zmq_context,
        router_mode=RouterMode.LOAD_BALANCE,
        heartbeat_timeout=5.0
    )
    await router.start()
    yield router
    await router.stop()

class MyMethodService(ServiceDealer):
    """测试不同类型的方法处理"""
    
    def __init__(self, router_address: str, context=None):
        super().__init__(
            router_address=router_address,
            context=context,
            heartbeat_interval=0.5,
            heartbeat_timeout=2.0
        )

    @service_method
    def sync_method(self, x: int) -> int:
        """同步方法"""
        return x + 1

    @service_method
    def sync_generator(self, start: int, end: int):
        """同步生成器"""
        for i in range(start, end):
            yield i

    @service_method
    async def async_method(self, x: int) -> int:
        """异步方法"""
        await asyncio.sleep(0.1)
        return x + 1

    @service_method
    async def async_generator(self, start: int, end: int):
        """异步生成器"""
        for i in range(start, end):
            await asyncio.sleep(0.1)
            yield i

# 直接在测试函数中处理所有逻辑，避免复杂的fixture依赖
@pytest.mark.asyncio
async def test_method_types(router_address, zmq_context, router):
    """测试不同类型方法的处理"""
    # 创建服务
    service = MyMethodService(router_address, context=zmq_context)
    await service.start()
    await asyncio.sleep(1.0)  # 等待服务注册
    
    try:
        # 创建客户端
        client = ClientDealer(router_address, context=zmq_context, timeout=5.0)
        
        try:
            # 先确认服务已注册并可发现
            available_methods = await client.discover_services()
            logger.info(f"发现的可用方法: {list(available_methods.keys())}")
            
            # 1. 测试同步方法        
            async for b in client.stream("MyMethodService.sync_method", 1):
                logger.info(f"同步方法结果: {b}")
                assert b == 2
                break
            
            # 2. 测试同步生成器
            numbers = []
            async for num in client.stream("MyMethodService.sync_generator", 0, 3):
                logger.info(f"同步生成器结果: {num}")
                numbers.append(num)
            assert numbers == [0, 1, 2]
            
            # 3. 测试异步方法
            async for b in client.stream("MyMethodService.async_method", 1):
                logger.info(f"异步方法结果: {b}")
                assert b == 2
                break
            
            # 4. 测试异步生成器
            numbers = []
            async for num in client.stream("MyMethodService.async_generator", 0, 3):
                logger.info(f"异步生成器结果: {num}")
                numbers.append(num)
            assert numbers == [0, 1, 2]
                
        except Exception as e:
            logger.error(f"测试过程中发生错误: {e}", exc_info=True)
            raise
        finally:
            await client.close()
            
    finally:
        await service.stop() 