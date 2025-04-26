import pytest
import pytest_asyncio
import asyncio
import time
import logging
import random

from voidrail import ServiceRouter, ServiceDealer, ClientDealer, service_method

logger = logging.getLogger(__name__)

@pytest.fixture(autouse=True)
def setup_logging(caplog):
    """设置日志级别"""
    # 重置所有处理器的日志级别
    for handler in logging.getLogger().handlers:
        handler.setLevel(logging.INFO)
    # 设置 caplog 捕获级别
    caplog.set_level(logging.INFO)

@pytest.fixture()
def router_address():
    """返回唯一的路由器地址，使用随机TCP端口"""
    port = random.randint(45000, 49999)
    return f"tcp://127.0.0.1:{port}"

@pytest_asyncio.fixture
async def router_fifo(router_address):
    """创建并启动FIFO模式路由器"""
    router = ServiceRouter(
        address=router_address,
        heartbeat_interval=0.5
    )
    await router.start()
    yield router
    await router.stop()

@pytest_asyncio.fixture
async def client_dealer(router_fifo, router_address):
    """创建客户端 - 用于FIFO模式测试"""
    client = ClientDealer(router_address)
    await client.connect()
    yield client
    await client.close()

class MonitorTestService(ServiceDealer):
    """用于监控测试的简单服务"""
    def __init__(
        self,
        router_address: str,
        heartbeat_interval: float = 0.2,
        **kwargs
    ):
        if 'heartbeat_interval' not in kwargs:
            kwargs['heartbeat_interval'] = heartbeat_interval
        super().__init__(
            router_address=router_address,
            **kwargs
        )
    
    @service_method
    async def slow_echo(self, message):
        """慢响应方法，确保请求会在队列中等待"""
        await asyncio.sleep(0.5)
        return message

@pytest.mark.asyncio
async def test_router_info_fifo(client_dealer):
    """测试FIFO模式下获取路由器信息功能"""
    # 获取路由器信息
    router_info = await client_dealer.get_router_info()
    
    # 验证基本字段
    assert "mode" in router_info
    assert router_info["mode"] == "fifo"  # 确认是FIFO模式
    assert "address" in router_info
    assert "idle_heartbeat_timeout" in router_info  # 名称沿用旧的，但内部已经调整
    assert "busy_heartbeat_timeout" in router_info
    assert "max_busy_without_heartbeat" in router_info
    assert "active_services_count" in router_info
    assert "total_services_count" in router_info
    assert "requests_in_queue" in router_info
    
    # 打印路由器信息
    logger.info(f"Router 信息: {router_info}")

@pytest.mark.asyncio
async def test_queue_status(client_dealer, router_address):
    """测试获取队列状态功能"""
    # 创建一个慢服务，确保请求会在队列中等待
    service = MonitorTestService(router_address=router_address)
    # 同步启动
    service.start()
    
    try:
        # 等待服务注册完成
        await asyncio.sleep(0.2)
        
        # 发现服务
        methods = await client_dealer.discover_services()
        logger.info(f"Available methods: {methods}")
        
        # 发送多个请求，但不等待结果 - 只发送一个请求简化分析
        task = asyncio.create_task(client_dealer.invoke("MonitorTestService.slow_echo", "test_message"))
        
        # 等待请求入队并开始处理
        await asyncio.sleep(0.1)
        
        # 等待请求完成处理，这样它的响应就不会干扰我们接下来的队列状态查询
        await asyncio.sleep(0.6)  # 等待时间超过服务的响应时间(0.5秒)
        
        # 获取队列状态
        try:
            status = await client_dealer.get_queue_status()
            logger.info(f"Queue status type: {type(status)}")
            logger.info(f"Queue status content: {status}")
            
            # 如果是字符串，报告更多细节
            if isinstance(status, str):
                logger.error(f"状态是字符串而不是字典! 内容: '{status}'")
                # 尝试一下router_info看看它返回的内容是否正确
                router_info = await client_dealer.get_router_info()
                logger.info(f"对比 - Router info type: {type(router_info)}")
                logger.info(f"对比 - Router info content: {router_info}")
                assert False, "状态应该是字典而不是字符串"
                
            # 验证状态信息
            assert "MonitorTestService.slow_echo" in status, f"方法应该在队列状态中，但只找到这些键：{status.keys()}"
            assert status["MonitorTestService.slow_echo"]["queue_length"] >= 0
            assert status["MonitorTestService.slow_echo"]["busy_services"] >= 0
        except Exception as e:
            logger.error(f"获取队列状态时出错: {e}")
            raise
            
        # 清理任务
        try:
            await task
        except Exception as e:
            logger.error(f"Task error: {e}")
                
    finally:
        # 同步停止
        service.stop() 