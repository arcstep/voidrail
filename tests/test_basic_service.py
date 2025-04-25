import pytest
import pytest_asyncio
import asyncio
import zmq.asyncio
import logging
import json
import time
import uuid
from voidrail import ServiceRouter, ServiceDealer, ClientDealer, service_method, ServiceState, DealerState

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
    """每次测试使用唯一的地址"""
    return f"inproc://router_test_{uuid.uuid4().hex[:8]}"

@pytest.fixture()
def test_config():
    """测试配置"""
    return {
        'heartbeat_timeout': 0.1,  # 缩短到100ms
        'heartbeat_interval': 0.05, # 缩短到50ms
    }

@pytest_asyncio.fixture
async def router(router_address, zmq_context, test_config):
    """创建并启动路由器"""
    router = ServiceRouter(
        router_address, 
        context=zmq_context,
        heartbeat_timeout=test_config['heartbeat_timeout']
    )
    await router.start()
    await asyncio.sleep(0.01)  # 缩短到10ms
    yield router
    await router.stop()

@pytest_asyncio.fixture
async def service(router, router_address, zmq_context, test_config):
    """创建并启动服务"""
    service = EchoService(
        router_address,
        context=zmq_context,
        heartbeat_timeout=test_config['heartbeat_timeout'],
        heartbeat_interval=test_config['heartbeat_interval']
    )
    await service.start()
    await asyncio.sleep(test_config['heartbeat_interval'] * 1.5)  # 缩短等待时间
    yield service
    await service.stop()

@pytest_asyncio.fixture
async def client(router, service, router_address, zmq_context):
    """创建客户端"""
    client = ClientDealer(router_address, context=zmq_context, timeout=0.5)  # 缩短超时
    await asyncio.sleep(0.01)  # 缩短到10ms
    yield client
    await client.close()

@pytest_asyncio.fixture
async def second_service(router, router_address, zmq_context):
    """创建第二个服务实例"""
    service = EchoService(router_address, context=zmq_context)
    await service.start()
    yield service
    await service.stop()

@pytest.fixture
async def clean_zmq_context(zmq_context):
    yield
    # 测试后清理所有剩余套接字
    for i in range(32):  # 通常最多使用32个套接字
        try:
            sock = zmq_context.socket(zmq.DEALER)
            sock.close(linger=0)
        except:
            pass

@pytest.fixture
def router_address_ipc():
    """为子进程测试提供IPC协议地址"""
    return f"ipc:///tmp/test_router_{uuid.uuid4().hex}"

class EchoService(ServiceDealer):
    """示例服务实现"""
    def __init__(
        self,
        router_address: str,
        context = None,
        heartbeat_timeout: float = 5.0, # 保持一个默认值或根据需要调整
        heartbeat_interval: float = 0.2,
        **kwargs
    ):
        if 'heartbeat_timeout' not in kwargs:
            kwargs['heartbeat_timeout'] = heartbeat_timeout
        if 'heartbeat_interval' not in kwargs:
            kwargs['heartbeat_interval'] = heartbeat_interval
        super().__init__(
            router_address=router_address,
            context=context,
            **kwargs  # 通过 kwargs 传递
        )

    @service_method # 使用默认方法名
    async def echo(self, message: str) -> str:
        """简单回显服务"""
        await asyncio.sleep(0.1)
        logger.info(f"EchoService {self._service_id} echo: {message}")
        return message

    @service_method(name="stream")
    async def stream_numbers(self, start: int, end: int):
        """流式返回数字序列"""
        for i in range(start, end):
            yield i
            await asyncio.sleep(0.1)  # 模拟处理延迟

    @service_method(
        name="add",
        description="Add two numbers",
        params={
            "a": "first number",
            "b": "second number"
        }
    )
    def add_numbers(self, a: int, b: int) -> int:
        """带参数说明的加法服务"""
        return a + b

class TestBasicFunctionality:
    """测试服务的基本功能，验证服务框架的核心能力
    
    1. 基础方法调用能力
    2. 参数传递和返回值处理
    3. 流式响应支持
    """

    @pytest.mark.asyncio
    async def test_simple_echo(self, router, service, client):
        """测试简单的回显服务"""
        message = "Hello, World!"
        async for response in client.stream("EchoService.echo", message):
            assert response == message
            break

    @pytest.mark.asyncio
    async def test_with_parameters(self, router, service, client):
        """测试带参数的方法调用"""
        async for response in client.stream("EchoService.add", 5, 3):
            assert response == 8
            break

    @pytest.mark.asyncio
    async def test_streaming_response(self, router, service, client):
        """测试流式响应功能"""
        expected = list(range(0, 5))
        received = []
        
        async for response in client.stream("EchoService.stream", 0, 5):
            received.append(response)
                
        assert received == expected


class TestServiceDiscovery:
    """测试服务发现和元数据获取功能
    
    1. 服务和方法发现能力
    2. 元数据传递和获取
    3. 集群状态和监控
    """

    @pytest.mark.asyncio
    async def test_discover_services(self, router, service, client):
        """测试服务发现"""
        available_methods = await client.discover_services()
        
        assert "EchoService.echo" in available_methods
        assert "EchoService.add" in available_methods
        assert "EchoService.stream" in available_methods
        
            # 验证方法元数据
        add_info = available_methods["EchoService.add"]
        assert add_info["description"] == "Add two numbers"
        assert "a" in add_info["params"]
        assert "b" in add_info["params"]

    @pytest.mark.asyncio
    async def test_discover_clusters(self, router, service, client):
        """测试集群发现"""
        clusters = await client.discover_clusters()
        assert len(clusters) >= 1
        
        # 验证集群状态信息
        for cluster_id, info in clusters.items():
            assert "state" in info
            assert info["state"] == "active"


class TestConnectionReliability:
    """测试连接可靠性，确保通信的稳定性
    
    1. 连接重用
    2. 客户端自动重连
    3. 连接恢复后功能正常
    """

    @pytest.mark.asyncio
    async def test_connection_reuse(self, router, service, client):
        """测试连接重用"""
        # 第一次调用
        async for response in client.stream("EchoService.echo", "test1"):
            assert response == "test1"
            break
            
        # 第二次调用（应该重用连接）
        async for response in client.stream("EchoService.echo", "test2"):
            assert response == "test2"
            break

    @pytest.mark.asyncio
    async def test_auto_reconnect(self, router, service, router_address, zmq_context):
        """测试客户端自动重连"""
        # 创建新客户端
        client = ClientDealer(router_address, context=zmq_context, timeout=1.0)
        
        try:
            # 第一次调用
            async for response in client.stream("EchoService.echo", "test1"):
                assert response == "test1"
                break
            
            # 关闭并重新创建客户端
            await client.close()
            client = ClientDealer(router_address, context=zmq_context, timeout=1.0)
            
            # 第二次调用（应该自动重连）
            async for response in client.stream("EchoService.echo", "test2"):
                assert response == "test2"
                break
        finally:
            await client.close()


class TestErrorHandling:
    """测试错误处理机制，验证系统对异常情况的响应
    
    1. 超时处理
    2. 不存在的服务/方法处理
    3. 服务端抛出异常的处理
    4. 条件性异常处理
    """

    @pytest.mark.asyncio
    async def test_timeout(self, router, service, client):
        """测试请求超时处理"""
        with pytest.raises(TimeoutError):
            async for _ in client.stream("EchoService.echo", "test", timeout=0.001):
                await asyncio.sleep(0.1)  # 强制超时

    @pytest.mark.asyncio
    async def test_service_not_found(self, router, client):
        """测试请求不存在的服务"""
        with pytest.raises(RuntimeError) as exc_info:
            async for _ in client.stream("EchoService.non_existent", "test"):
                pass
        assert "not found" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_service_exception(self, router, router_address, zmq_context, client):
        """测试服务端抛出异常时客户端能正确接收错误信息"""
        # 创建错误服务
        class ErrorService(ServiceDealer):
            @service_method
            async def throw_error(self, error_message: str = "测试异常"):
                """故意抛出异常的方法"""
                raise ValueError(error_message)
                
            @service_method
            async def conditional_error(self, should_fail: bool = False):
                """根据条件决定是否抛出异常"""
                if should_fail:
                    raise RuntimeError("条件触发的异常")
                return "成功执行，无异常"
        
        error_service = ErrorService(router_address, context=zmq_context)
        await error_service.start()
        
        try:
            # 等待服务注册并更新发现缓存
            await asyncio.sleep(0.5)
            await client.discover_services()
            
            # 测试1: 服务抛出异常
            error_message = "测试异常信息12345"
            with pytest.raises(RuntimeError) as exc_info:
                async for _ in client.stream("ErrorService.throw_error", error_message):
                    pass
            assert "Method execution error" in str(exc_info.value)
            assert error_message in str(exc_info.value)
            
            # 测试2: 条件性异常 - 成功路径
            async for response in client.stream("ErrorService.conditional_error", False):
                assert response == "成功执行，无异常"
                break
                
            # 测试3: 条件性异常 - 失败路径
            with pytest.raises(RuntimeError) as exc_info:
                async for _ in client.stream("ErrorService.conditional_error", True):
                    pass
            assert "条件触发的异常" in str(exc_info.value)
        finally:
            await error_service.stop()


class TestLoadBalancing:
    """测试负载均衡和故障转移能力
    
    1. 并发请求处理
    2. 多实例负载分布
    3. 实例故障转移
    """
    
    @pytest.mark.asyncio
    async def test_concurrent_requests(self, router, service, client):
        """测试并发请求处理"""
        async def make_request(a: int, b: int):
            async for response in client.stream("EchoService.add", a, b):
                assert response == a + b
                break

        # 创建多个并发请求
        requests = [
            make_request(i, i+1)
            for i in range(10)
        ]
        await asyncio.gather(*requests)
    
    @pytest.mark.asyncio
    async def test_multiple_instances(self, router, service, second_service, router_address, zmq_context):
        """测试多实例负载均衡"""
        # 发送多个请求
        clients = []
        tasks = []
        for i in range(10):
            client = ClientDealer(router_address, context=zmq_context, timeout=2.0)
            clients.append(client)
            tasks.append(client.stream("EchoService.echo", f"test_{i}").__anext__())
        
        responses = await asyncio.gather(*tasks)
        
        # 检查服务发现，应该能看到两个服务
        clusters = await clients[0].discover_clusters()
        assert len(clusters.keys()) == 2, "应该有两个服务注册"
        
        # 检查响应是否正确
        assert len(responses) == 10, "应该收到所有请求的响应"
        assert all(resp == f"test_{i}" for i, resp in enumerate(responses)), "响应内容应该正确"

        for client in clients:
            await client.close()

    @pytest.mark.asyncio
    async def test_failover(self, router, service, second_service, client):
        """测试服务故障转移"""
        # 首先确认两个服务都在工作
        clusters = await client.discover_clusters()
        active_clusters = {k: v for k, v in clusters.items() if v['state'] == 'active'}
        assert len(active_clusters) == 2

        # 测试初始状态
        async for response in client.stream("EchoService.echo", "test1"):
            assert response == "test1"
            break
        
        # 停止第一个服务
        await service.stop()
        await asyncio.sleep(0.3)  # 增加等待时间，确保ROUTER完成处理
        
        # 确认可以使用剩余服务
        async for response in client.stream("EchoService.echo", "test2"):
            assert response == "test2"
            break
        
        # 验证集群状态
        clusters = await client.discover_clusters()
        active_clusters = {k: v for k, v in clusters.items() if v['state'] == 'active'}
        assert len(active_clusters) == 1, "应该只剩一个活跃服务"


class TestReliability:
    """测试系统在各种故障场景下的行为
    
    1. ROUTER主动/被动下线
    2. DEALER主动/被动下线
    3. 忙时任务完成保证
    4. 心跳机制和故障检测
    """
    
    @pytest.mark.asyncio
    async def test_router_active_shutdown_notification(self, router_address, zmq_context):
        """测试ROUTER主动下线时通知所有连接的DEALER和CLIENT"""
        router = ServiceRouter(router_address, context=zmq_context)
        await router.start()
        
        services = []
        try:
            for i in range(3):
                service = EchoService(
                    router_address, 
                    context=zmq_context, 
                    service_id=f"test-service-{i}",
                    heartbeat_interval=0.02  # 快速心跳
                )
                await service.start()
                services.append(service)
            
            client = ClientDealer(router_address, context=zmq_context)
            
            # 确认初始状态
            await asyncio.sleep(0.1)  # 增加等待时间确保连接建立
            for service in services:
                assert service._state == DealerState.RUNNING
            
            # 停止Router
            await router.stop()
            
            # 等待足够长时间使DEALER能检测到断开
            await asyncio.sleep(0.3)  # 显著增加等待时间
            
            # 验证状态转换为重连
            for service in services:
                assert service._state == DealerState.RECONNECTING
            
            # 清理
            await client.close()
            for service in services:
                await service.stop()
        finally:
            # 确保资源完全清理
            for service in services:
                if service._state != DealerState.STOPPED:
                    await service.stop()
            if router._state != "stopped":  # Router使用字符串状态
                await router.stop()

    @pytest.mark.asyncio
    async def test_router_crash_detection(self, zmq_context):
        """测试ROUTER意外崩溃时的快速检测"""
        import multiprocessing as mp
        
        # 唯一的IPC地址
        router_address = f"ipc:///tmp/test_router_crash_{uuid.uuid4().hex}"
        ready_flag = mp.Event()
        stop_flag = mp.Event()
        
        # 启动Router子进程
        router_process = mp.Process(
            target=start_router_process,
            args=(router_address, ready_flag, stop_flag)
        )
        router_process.start()
        
        service = None
        try:
            # 等待Router启动
            assert ready_flag.wait(timeout=1.0), "Router启动超时"
            
            # 启动Dealer，缩短心跳间隔和超时时间以加速测试
            service = EchoService(
                router_address, 
                context=zmq_context,
                heartbeat_interval=0.02,  # 更短的心跳间隔
                heartbeat_timeout=0.1     # 更短的心跳超时
            )
            await service.start()
            await asyncio.sleep(0.2)  # 增加等待时间确保连接稳定
            
            # 保存初始任务引用和状态
            initial_message_task = service._process_messages_task
            assert not initial_message_task.done(), "消息任务应该正在运行"
            
            # 记录测试开始时间
            start_time = time.time()
            
            # 模拟Router崩溃
            router_process.terminate()
            router_process.join(timeout=0.1)
            
            # 增加等待时间，确保有足够时间检测到崩溃
            max_wait = 2.0  # 最长等待2秒
            detected = False
            
            while time.time() - start_time < max_wait:
                await asyncio.sleep(0.05)  # 较短的检查间隔
                
                # 检查是否重连中或消息任务已取消
                if (service._state == DealerState.RECONNECTING or 
                    initial_message_task.done()):
                    detected = True
                    break
            
            # 断言已检测到崩溃
            assert detected, f"Router崩溃应在{max_wait}秒内被检测到（当前状态：{service._state}）"
        finally:
            if service:
                await service.stop()
            if router_process.is_alive():
                router_process.terminate()
                router_process.join(timeout=0.1)

    @pytest.mark.asyncio
    async def test_dealer_active_shutdown_notification(self, router_address, zmq_context):
        """测试DEALER主动下线时立即通知ROUTER并被标记为下线"""
        router = ServiceRouter(
            router_address, 
            context=zmq_context,
            heartbeat_timeout=0.1
        )
        await router.start()
        
        service = None
        client = None
        try:
            service = EchoService(
                router_address, 
                context=zmq_context,
                heartbeat_interval=0.02
            )
            await service.start()
            
            # 等待服务注册
            await asyncio.sleep(0.1)
            service_id = service._service_id
            assert service_id in router._services
            
            # 验证客户端能发现服务
            client = ClientDealer(router_address, context=zmq_context)
            clusters = await client.discover_clusters()
            assert service_id in clusters
            
            # 主动停止服务
            await service.stop()
            
            # 等待Router处理关闭消息
            await asyncio.sleep(0.2)
            
            # 修改断言: 服务应被设置为SHUTDOWN状态或从列表中移除
            if service_id in router._services:
                assert router._services[service_id].state == ServiceState.SHUTDOWN
            else:
                # 服务被完全移除也是可接受的
                assert True
            
            # 客户端不应再发现服务
            clusters = await client.discover_clusters()
            active_clusters = {k: v for k, v in clusters.items() if v['state'] == 'active'}
            assert service_id not in active_clusters
        finally:
            if client:
                await client.close()
            if service and service._state != DealerState.STOPPED:
                await service.stop()
            await router.stop()

    @pytest.mark.asyncio
    async def test_dealer_completes_busy_tasks(self, router_address, zmq_context):
        """测试DEALER即使超过忙时限制，也能完成任务并正确返回结果"""
        # 修复SlowService实现
        class SlowService(ServiceDealer):
            def __init__(self, router_address, context=None, **kwargs):
                # 保存自定义参数
                self._busy_interval = kwargs.pop('busy_heartbeat_interval', 0.02)
                self._busy_timeout = kwargs.pop('busy_heartbeat_timeout', 0.2)
                
                # 调用父类构造函数
                super().__init__(router_address=router_address, context=context, **kwargs)
                
                # 调整属性
                self._busy_heartbeat_interval = self._busy_interval
                self._busy_heartbeat_timeout = self._busy_timeout
                self._CONSECUTIVE_FAILURES_THRESHOLD = 1
                
            @service_method
            async def slow_task(self, duration: float = 0.1):
                """模拟耗时任务"""
                start_time = time.time()
                await asyncio.sleep(duration)
                return {"duration": time.time() - start_time}
        
        router = ServiceRouter(router_address, context=zmq_context)
        await router.start()
        
        service = None
        client = None
        try:
            # 创建服务
            service = SlowService(
                router_address, 
                context=zmq_context,
                heartbeat_interval=0.02,  # 原生参数
                heartbeat_timeout=0.2,     # 原生参数
                busy_heartbeat_interval=0.05,  # 自定义参数 
                busy_heartbeat_timeout=0.5     # 自定义参数
            )
            await service.start()
            await asyncio.sleep(0.1)
            
            # 创建客户端
            client = ClientDealer(router_address, context=zmq_context, timeout=2.0)
            
            # 发送并发任务
            tasks = []
            for i in range(5):
                tasks.append(client.invoke("SlowService.slow_task", 0.1))
            
            results = await asyncio.gather(*tasks)
            
            # 验证结果
            assert len(results) == 5
            for result in results:
                assert "duration" in result[0]
                assert result[0]["duration"] >= 0.08
        finally:
            if client:
                await client.close()
            if service:
                await service.stop()
            await router.stop()

    @pytest.mark.asyncio
    async def test_dealer_idle_crash_detection(self, router_address_ipc, zmq_context):
        """测试DEALER闲时心跳缺失被视为下线"""
        import multiprocessing as mp
        
        # 创建Router，使用IPC地址
        router = ServiceRouter(
            router_address_ipc,  # 使用IPC协议
            context=zmq_context,
            idle_heartbeat_check=0.1,
            heartbeat_timeout=0.2
        )
        await router.start()
        
        dealer_process = None
        try:
            # 等待1秒确保启动完成
            await asyncio.sleep(0.2)
            
            # 启动Dealer子进程
            ready_flag = mp.Event()
            crash_flag = mp.Event()
            dealer_process = mp.Process(
                target=start_dealer_process,
                args=(router_address_ipc, ready_flag, crash_flag)
            )
            dealer_process.start()
            
            # 等待Dealer启动
            assert ready_flag.wait(timeout=2.0), "Dealer启动超时"
            
            # 等待足够长时间确保服务注册
            for _ in range(10):
                await asyncio.sleep(0.1)
                # 检查是否已注册
                for sid in router._services:
                    if "crash-test-service" in sid:
                        service_id = sid
                        break
                else:
                    service_id = None
                    continue
                break
            
            assert service_id is not None, "服务应已注册"
            assert router._services[service_id].state == ServiceState.ACTIVE
            
            # 模拟Dealer崩溃
            crash_flag.set()
            await asyncio.sleep(0.2)
            
            # 等待Router检测到心跳缺失 - 等待至少3秒
            await asyncio.sleep(3.5)  # 确保进行至少两次健康检查
            
            # 验证服务状态
            assert (service_id not in router._services or 
                    router._services[service_id].state == ServiceState.INACTIVE)
        finally:
            if dealer_process and dealer_process.is_alive():
                dealer_process.terminate()
                dealer_process.join(timeout=0.2)
            await router.stop()

# 修改模块级辅助函数，避免使用import
def start_router_process(router_address, ready_flag, stop_flag):
    import asyncio
    async def _run():
        from voidrail import ServiceRouter
        router = ServiceRouter(router_address, heartbeat_timeout=0.1)
        await router.start()
        ready_flag.set()
        while not stop_flag.is_set():
            await asyncio.sleep(0.01)
        await router.stop()
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_run())

def start_dealer_process(router_address, ready_flag, crash_flag):
    """在子进程中启动一个简单的DEALER服务"""
    import asyncio
    import os, signal
    from voidrail import ServiceDealer, service_method
    
    # 直接在函数内定义服务类，避免导入问题
    class SimpleService(ServiceDealer):
        def __init__(self, router_address, **kwargs):
            kwargs.setdefault('service_id', 'crash-test-service')
            super().__init__(router_address=router_address, **kwargs)
            
        @service_method
        async def echo(self, message):
            return message
    
    async def _run():
        # 使用本地定义的类
        service = SimpleService(
            router_address=router_address,
            heartbeat_interval=0.02
        )
        await service.start()
        ready_flag.set()
        
        while not crash_flag.is_set():
            await asyncio.sleep(0.01)
        
        # 模拟崩溃
        os.kill(os.getpid(), signal.SIGKILL)
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_run())
