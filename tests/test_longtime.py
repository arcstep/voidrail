import pytest
import pytest_asyncio
import asyncio
import time
import random

from voidrail.router import ServiceRouter
from voidrail.dealer import ServiceDealer, service_method
from voidrail.client import ClientDealer

# 每次测试使用随机端口以避免冲突
@pytest.fixture
def router_address():
    port = random.randint(40000, 49999)
    return f"tcp://127.0.0.1:{port}"

# 缩短心跳间隔以加快测试
@pytest.fixture
def test_config():
    return {
        "heartbeat_interval": 0.1,  # 100ms
    }

# 启动 Router
@pytest_asyncio.fixture
async def router(router_address, test_config):
    r = ServiceRouter(
        address=router_address,
        heartbeat_interval=test_config["heartbeat_interval"]
    )
    await r.start()
    # 等待初次心跳注册
    await asyncio.sleep(test_config["heartbeat_interval"] * 1.5)
    yield r
    await asyncio.wait_for(r.stop(), timeout=1.0)

# 定义一个同步耗时服务
class SlowService(ServiceDealer):
    @service_method
    def slow(self, duration: float):
        """同步阻塞任务，休眠指定时长再返回"""
        time.sleep(duration)
        return {"slept": duration}

# 启动 SlowService
@pytest_asyncio.fixture
async def service(router, router_address, test_config):
    svc = SlowService(
        router_address=router_address,
        heartbeat_interval=test_config["heartbeat_interval"]
    )
    svc.start()
    # 等待注册完成
    await asyncio.sleep(test_config["heartbeat_interval"] * 1.5)
    yield svc
    svc.stop()

@pytest_asyncio.fixture
async def stream_client(router):
    c = ClientDealer(router_address=router._address, timeout=1.0)
    await c.connect()
    yield c
    await c.close()

@pytest_asyncio.fixture
async def status_client(router):
    c = ClientDealer(router_address=router._address, timeout=1.0)
    await c.connect()
    yield c
    await c.close()

@pytest.mark.asyncio
async def test_long_running_busy_and_release(router, service, stream_client, status_client, test_config):
    """
    调用 SlowService.slow，让任务运行时间 > 心跳超时，
    验证执行期间 Router 报告 busy_services=1，
    任务完成后恢复 busy_services=0, available_services=1。
    """
    # 任务时长选为心跳超时的 4 倍
    duration = test_config["heartbeat_interval"] * 4

    # 1) 用 stream_client 发长时流式请求
    stream_iter = stream_client.stream("SlowService.slow", duration)
    task = asyncio.create_task(stream_iter.__anext__())

    # 等到请求派发
    await asyncio.sleep(test_config["heartbeat_interval"] * 1.1)

    # 2) 用 status_client 查询状态
    status = await status_client.get_queue_status()
    svc = status.get("SlowService.slow", {})
    assert svc.get("busy_services") == 1
    assert svc.get("available_services") == 0

    # 等任务做完
    result = await task
    assert result == {"slept": duration}

    # 再等一次让 Router 更新
    await asyncio.sleep(test_config["heartbeat_interval"] * 1.1)

    status2 = await status_client.get_queue_status()
    svc2 = status2.get("SlowService.slow", {})
    assert svc2.get("busy_services") == 0
    assert svc2.get("available_services") == 1

@pytest.mark.asyncio
async def test_long_running_single_client(router, service, stream_client, status_client, test_config):
    """
    用同一个 client 同时做流式调用和状态查询，
    验证在任务执行期间是否能正确看到 busy_services=1，任务结束后恢复为 idle。
    """
    # 让服务阻塞 4 倍心跳周期
    duration = test_config["heartbeat_interval"] * 4

    # 1) 启动流式调用（内部非流式也会被包装为流式）
    stream_iter = stream_client.stream("SlowService.slow", duration)
    result_task = asyncio.create_task(stream_iter.__anext__())

    # 2) 等待请求派发并进入执行
    await asyncio.sleep(test_config["heartbeat_interval"] * 1.1)

    # 3) 同一个 client 查询队列状态
    status = await status_client.get_queue_status()
    svc = status.get("SlowService.slow", {})
    # 这里如果没有正确抢占，会看到 busy_services==0
    assert svc.get("busy_services") == 1
    assert svc.get("available_services") == 0

    # 4) 等待任务完成并取回结果
    result = await result_task
    assert result == {"slept": duration}

    # 5) 再等一下，让 Router 收到 reply 并更新状态
    await asyncio.sleep(test_config["heartbeat_interval"] * 1.1)

    status2 = await status_client.get_queue_status()
    svc2 = status2.get("SlowService.slow", {})
    assert svc2.get("busy_services") == 0
    assert svc2.get("available_services") == 1
