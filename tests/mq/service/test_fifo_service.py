import asyncio
import pytest
import pytest_asyncio
import logging
import zmq.asyncio
from voidrail.mq.service.dealer import ServiceDealer, service_method
from voidrail.mq.service.client import ClientDealer
from voidrail.mq.service.router import ServiceRouter, RouterMode

# 定义路由器地址
ROUTER_ADDRESS = "inproc://test_service"

@pytest.fixture()
def zmq_context():
    """创建共享的 ZMQ Context"""
    context = zmq.asyncio.Context.instance()  # 使用单例模式获取 Context
    yield context

@pytest_asyncio.fixture
async def router_fifo(zmq_context):
    """创建并启动FIFO模式路由器"""
    router = ServiceRouter(
        address=ROUTER_ADDRESS,
        context=zmq_context,
        router_mode=RouterMode.FIFO,
        heartbeat_timeout=0.5
    )
    await router.start()
    yield router
    await router.stop()

@pytest_asyncio.fixture
async def client_dealer(router_fifo, zmq_context):
    """创建客户端"""
    client = ClientDealer(ROUTER_ADDRESS, context=zmq_context)
    await client.connect()
    yield client
    await client.close()

@pytest.mark.asyncio
async def test_queue_status(client_dealer, router_fifo):
    """测试获取队列状态功能"""
    # 创建一个慢服务，确保请求会在队列中等待
    class SlowService(ServiceDealer):
        @service_method
        async def slow_echo(self, message):
            await asyncio.sleep(0.5)  # 慢响应
            return message
    
    # 启动服务
    service = SlowService(context=router_fifo.context, router_address=ROUTER_ADDRESS)
    await service.start()
    
    try:
        # 发送多个请求，但不等待结果
        tasks = [
            asyncio.create_task(client_dealer.invoke("default.slow_echo", f"test{i}"))
            for i in range(3)
        ]
        
        # 等待一小段时间确保请求发送到路由器
        await asyncio.sleep(0.1)
        
        # 获取队列状态
        status = await client_dealer.get_queue_status()
        
        # 验证状态信息
        assert "default.slow_echo" in status
        assert status["default.slow_echo"]["queue_length"] >= 0
        assert status["default.slow_echo"]["busy_services"] >= 0
        
        # 清理任务
        for task in tasks:
            try:
                await task
            except Exception:
                pass
                
    finally:
        await service.stop()

@pytest.mark.asyncio
async def test_router_info(client_dealer, router_fifo):
    """测试获取路由器信息功能"""
    # 获取路由器信息
    router_info = await client_dealer.get_router_info()
    
    # 验证基本字段
    assert "mode" in router_info
    assert router_info["mode"] == "fifo"  # 确认是FIFO模式
    assert "address" in router_info
    assert "heartbeat_timeout" in router_info
    assert "active_services" in router_info
    assert "total_services" in router_info
    assert "queue_stats" in router_info 