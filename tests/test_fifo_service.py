import pytest
import pytest_asyncio
import asyncio
import zmq.asyncio
import time
from collections import defaultdict
import logging

from voidrail import ServiceRouter, RouterMode, ServiceDealer, ClientDealer, service_method

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
def zmq_context():
    """创建共享的 ZMQ Context"""
    context = zmq.asyncio.Context.instance()  # 使用单例模式获取 Context
    yield context

@pytest.fixture()
def router_address():
    """返回路由器地址"""
    return "inproc://router_fifo_test"

@pytest.fixture()
def test_config():
    """测试配置"""
    return {
        'heartbeat_timeout': 2.0,    # Router 心跳超时时间
    }

@pytest_asyncio.fixture
async def router_fifo(router_address, zmq_context, test_config):
    """创建并启动FIFO模式路由器"""
    router = ServiceRouter(
        router_address, 
        context=zmq_context,
        heartbeat_timeout=test_config['heartbeat_timeout'],
        router_mode=RouterMode.FIFO
    )
    await router.start()
    await asyncio.sleep(0.1)
    yield router
    # 在停止前等待一小段时间，确保能处理所有关闭请求
    await asyncio.sleep(0.5)
    await router.stop()

class EchoService(ServiceDealer):
    """测试用服务实现"""
    def __init__(self, router_address: str, context = None, heartbeat_timeout: float = 0.5):
        super().__init__(
            router_address=router_address,
            context=context
        )
        self.heartbeat_timeout = heartbeat_timeout

    @service_method
    async def delay_echo(self, message: str, delay: float = 0.1) -> dict:
        """带延迟的回显，返回消息和服务ID"""
        await asyncio.sleep(delay)
        return {
            "message": message,
            "service_id": self._service_id,
            "timestamp": time.time()
        }

@pytest_asyncio.fixture
async def service(router_fifo, router_address, zmq_context, test_config):
    """创建并启动测试服务1"""
    service = EchoService(
        router_address,
        context=zmq_context,
        heartbeat_timeout=test_config['heartbeat_timeout']
    )
    await service.start()
    yield service
    await service.stop()
    await asyncio.sleep(0.1)

@pytest_asyncio.fixture
async def service2(router_fifo, router_address, zmq_context, test_config):
    """创建并启动测试服务2"""
    service = EchoService(
        router_address,
        context=zmq_context,
        heartbeat_timeout=test_config['heartbeat_timeout']
    )
    await service.start()
    yield service
    await service.stop()
    await asyncio.sleep(0.1)

@pytest_asyncio.fixture
async def service3(router_fifo, router_address, zmq_context, test_config):
    """创建并启动测试服务3"""
    service = EchoService(
        router_address,
        context=zmq_context,
        heartbeat_timeout=test_config['heartbeat_timeout']
    )
    await service.start()
    yield service
    await service.stop()
    await asyncio.sleep(0.1)

@pytest_asyncio.fixture
async def client(router_fifo, router_address, zmq_context):
    """创建客户端"""
    client = ClientDealer(router_address, context=zmq_context, timeout=2.0)
    try:
        yield client
    finally:
        await client.close()

@pytest.mark.asyncio
async def test_fifo_ordering(router_fifo, service, service2, service3, client):
    """测试FIFO队列处理顺序是否正确"""
    # 发送20个请求，每个请求带上序号
    results = []
    for i in range(20):
        # delay_echo方法会在response中包含序号和处理它的service_id
        async for response in client.stream("EchoService.delay_echo", f"msg_{i}", i * 0.01):
            results.append(response)
            break
    
    # 验证结果是否按照请求顺序返回
    assert [r['message'] for r in results] == [f"msg_{i}" for i in range(20)]
    
    # 打印处理情况
    service_counts = {}
    for r in results:
        service_id = r['service_id']
        service_counts[service_id] = service_counts.get(service_id, 0) + 1
    
    logger.info(f"Service处理统计: {service_counts}")

@pytest.mark.asyncio
async def test_fifo_parallel_processing(router_fifo, service, service2, service3, client):
    """测试FIFO模式下多个DEALER并行处理"""
    # 创建请求记录器
    start_time = time.time()
    
    # 发送6个请求，前3个耗时长，后3个耗时短
    tasks = []
    for i in range(6):
        delay = 0.5 if i < 3 else 0.1  # 前3个请求延迟0.5秒，后3个延迟0.1秒
        tasks.append(client.stream("EchoService.delay_echo", f"msg_{i}", delay).__anext__())
    
    results = await asyncio.gather(*tasks)
    
    # 验证结果：前3个请求应该由3个不同的服务处理，后3个应该在前3个完成后处理
    service_ids = [r['service_id'] for r in results[:3]]
    assert len(set(service_ids)) == 3, "前3个请求应该由3个不同的服务并行处理"
    
    # 检查处理时间：总时间应该接近0.6秒（0.5秒+0.1秒），而不是1.8秒(6个顺序处理)
    total_time = time.time() - start_time
    logger.info(f"总处理时间: {total_time}秒")
    assert 0.55 < total_time < 0.9, "总处理时间应该接近0.6秒"


@pytest.mark.asyncio
async def test_fifo_load_imbalance(router_fifo, service, service2, service3, client):
    """测试FIFO模式下，当有2个长任务和4个短任务时，所有短任务应该分配给同一个处理端"""
    # 创建请求记录器
    start_time = time.time()
    
    # 分两批发送请求：前2个长任务，后4个短任务
    # 长任务：处理时间为0.5秒
    long_tasks = []
    for i in range(2):
        long_tasks.append(client.stream("EchoService.delay_echo", f"long_{i}", 0.5).__anext__())
    
    # 短任务：处理时间为0.1秒
    short_tasks = []
    for i in range(4):
        short_tasks.append(client.stream("EchoService.delay_echo", f"short_{i}", 0.1).__anext__())
    
    # 按顺序提交所有任务
    all_tasks = long_tasks + short_tasks
    results = await asyncio.gather(*all_tasks)
    
    # 提取结果中的关键信息
    task_info = [{
        "message": r["message"],
        "service_id": r["service_id"],
        "is_long": r["message"].startswith("long_")
    } for r in results]
    
    # 按服务ID分组任务
    tasks_by_service = defaultdict(list)
    for task in task_info:
        tasks_by_service[task["service_id"]].append(task)
    
    # 打印处理情况
    logger.info(f"任务分配情况: {dict(tasks_by_service)}")
    
    # 统计短任务的分配情况
    short_tasks_by_service = {}
    for service_id, tasks in tasks_by_service.items():
        short_tasks_count = sum(1 for t in tasks if not t["is_long"])
        if short_tasks_count > 0:
            short_tasks_by_service[service_id] = short_tasks_count
    
    logger.info(f"短任务分配情况: {short_tasks_by_service}")
    
    # 验证：
    # 1. 应该有1个服务处理了所有4个短任务
    assert max(short_tasks_by_service.values()) == 4, "应该有1个服务处理了所有4个短任务"
    
    # 2. 只有1个服务处理了短任务
    assert len(short_tasks_by_service) == 1, "只应该有1个服务处理短任务"
    
    # 3. 2个长任务应该由不同服务处理
    long_task_services = set()
    for task in task_info:
        if task["is_long"]:
            long_task_services.add(task["service_id"])
    assert len(long_task_services) == 2, "2个长任务应该由不同服务处理"
    
    # 4. 总处理时间应约为0.5秒（单个长任务的处理时间），因为长短任务是并行处理的
    total_time = time.time() - start_time
    logger.info(f"总处理时间: {total_time}秒")
    assert 0.45 < total_time < 0.7, "总处理时间应该约为0.5秒，即一个长任务的处理时间"
