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
    return f"inproc://router_test"

@pytest.fixture()
def test_config():
    """测试配置"""
    return {
        'heartbeat_timeout': 1.0, # Use a slightly shorter timeout for tests
        'heartbeat_interval': 0.2,
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
    await asyncio.sleep(0.1)
    yield router
    await router.stop()

@pytest_asyncio.fixture
async def service(router, router_address, zmq_context, test_config):
    """创建并启动服务"""
    service = EchoService(
        router_address,
        context=zmq_context,
        heartbeat_timeout=test_config['heartbeat_timeout'],
        heartbeat_interval=test_config['heartbeat_interval'] # Pass interval too
    )
    await service.start()
    await asyncio.sleep(test_config['heartbeat_interval'] * 2) # Wait for registration
    yield service
    await service.stop()

@pytest_asyncio.fixture
async def client(router, service, router_address, zmq_context):
    """创建客户端"""
    # Ensure service fixture runs before client to guarantee service is up
    client = ClientDealer(router_address, context=zmq_context, timeout=2.0)
    await asyncio.sleep(0.1) # Give client time to potentially connect
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
        await asyncio.sleep(0.1)  # 给路由器处理时间
        
        # 确认可以使用剩余服务
        async for response in client.stream("EchoService.echo", "test2"):
            assert response == "test2"
            break
        
        # 验证集群状态
        clusters = await client.discover_clusters()
        active_clusters = {k: v for k, v in clusters.items() if v['state'] == 'active'}
        assert len(active_clusters) == 1, "应该只剩一个活跃服务"


class TestReconnection:
    """测试服务重连机制，确保系统在网络故障后能自动恢复
    
    1. Dealer断线后自动重连 (Revised: Router stop/start simulation)
    2. Router重启后Dealer重连
    3. 多次网络故障后的连续恢复能力 (Covered by other tests implicitly)
    """
    
    @pytest.mark.asyncio
    async def test_dealer_auto_reconnect_after_router_stop(self, router_address, zmq_context, test_config):
        """测试 Router 停止后，Dealer 能通过自动重连机制恢复"""
        router = None
        service = None
        client = None
        
        try:
            # 1. Start Router
            router = ServiceRouter(router_address, context=zmq_context, heartbeat_timeout=test_config['heartbeat_timeout'])
            await router.start()

            # 2. Start Service (ensure automatic reconnect is enabled)
            service = EchoService(
                router_address,
                context=zmq_context,
                heartbeat_timeout=test_config['heartbeat_timeout'],
                heartbeat_interval=test_config['heartbeat_interval'],
                disable_reconnect=False # Explicitly enable auto-reconnect
            )
            await service.start()
            # Ensure service is connected initially
            await asyncio.sleep(test_config['heartbeat_interval'] * 3)
            assert service._connection_state == "CONNECTED"

            # 3. Create Client and test initial connection
            client = ClientDealer(router_address, context=zmq_context, timeout=2.0)
            try:
                result = await client.invoke("EchoService.echo", "initial")
                assert result == ["initial"] # Expect list result from invoke

                # 4. Stop the Router to simulate disconnect
                await router.stop()
                logger.info("Test: Router stopped.")
                # Wait longer than heartbeat timeout for dealer to detect
                await asyncio.sleep(test_config['heartbeat_timeout'] * 1.5) 
                # At this point, dealer's _reconnect_monitor should have triggered

                # 5. Restart the Router
                logger.info("Test: Restarting Router.")
                router = ServiceRouter(router_address, context=zmq_context, heartbeat_timeout=test_config['heartbeat_timeout'])
                await router.start()

                # 6. Wait for Dealer to automatically reconnect and register
                # Needs enough time for monitor check + reconnect attempt + registration
                await asyncio.sleep(test_config['heartbeat_timeout'] * 3) 

                # 7. Verify connection is re-established using retries
                max_retries = 5
                success = False
                last_error = None
                for retry in range(max_retries):
                    logger.info(f"Test: Reconnect attempt {retry+1}/{max_retries}")
                    try:
                        # It might be necessary to rediscover services if client lost connection info
                        await client.discover_services() 
                        result = await client.invoke("EchoService.echo", "reconnected")
                        assert result == ["reconnected"]
                        success = True
                        logger.info("Test: Reconnect successful!")
                        break
                    except Exception as e:
                        last_error = e
                        logger.warning(f"Test: Reconnect attempt failed: {e}")
                        await asyncio.sleep(1.0) # Wait before retrying

                assert success, f"Dealer failed to reconnect and respond after router restart. Last error: {last_error}"

            finally:
                await client.close()
                await service.stop()
                await router.stop() # Ensure the second router instance is stopped

            # 对重连等待添加明确超时
            max_wait_time = test_config['heartbeat_timeout'] * 5
            start_time = time.time()
            while time.time() - start_time < max_wait_time:
                try:
                    await client.discover_services()
                    result = await client.invoke("EchoService.echo", "reconnected")
                    if result == ["reconnected"]:
                        break  # 成功重连
                except Exception:
                    await asyncio.sleep(0.5)  # 失败后短暂等待再重试
            else:
                pytest.fail("重连超时")
            
        except Exception as e:
            pytest.fail(f"测试失败: {e}")
        finally:
            # 使用独立的异常处理确保每个资源都能被清理
            try:
                if client:
                    await asyncio.wait_for(client.close(), timeout=1.0)
            except Exception:
                pass
            
            try:
                if service:
                    await asyncio.wait_for(service.stop(), timeout=2.0)
            except Exception:
                pass
            
            try:
                if router:
                    await asyncio.wait_for(router.stop(), timeout=2.0)
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_dealer_reconnect_after_router_restart(self, router_address, zmq_context, test_config):
        """测试Router重启后Dealer能正常重连 (Revised Assertion)"""
        # 1. Start Router1
        router1 = ServiceRouter(router_address, context=zmq_context, heartbeat_timeout=test_config['heartbeat_timeout'])
        await router1.start()
        
        # 2. Start Service (allow auto-reconnect)
        service = EchoService(
            router_address,
            context=zmq_context,
            service_id="TestEchoService-reconnect",
            heartbeat_interval=test_config['heartbeat_interval'],
            heartbeat_timeout=test_config['heartbeat_timeout'],
            disable_reconnect=False 
        )
        await service.start()
        await asyncio.sleep(test_config['heartbeat_interval'] * 3) # Wait for registration

        # 3. Client & Initial Call
        client = ClientDealer(router_address, context=zmq_context, timeout=3.0) # Increased timeout slightly
        try:
            result = await client.invoke("EchoService.echo", "test-before-restart")
            assert result == ["test-before-restart"] # FIX: Expect list

            # 4. Stop Router1
            await router1.stop()
            logger.info("Test: Router 1 stopped.")
            # Wait for dealer to potentially detect timeout
            await asyncio.sleep(test_config['heartbeat_timeout'] * 1.5)

            # 5. Start Router2
            logger.info("Test: Starting Router 2.")
            router2 = ServiceRouter(router_address, context=zmq_context, heartbeat_timeout=test_config['heartbeat_timeout'])
            await router2.start()

            # 6. Wait for automatic reconnect
            logger.info("Test: Waiting for automatic reconnect...")
            await asyncio.sleep(test_config['heartbeat_timeout'] * 3) # Allow time for monitor check + reconnect + register

            # 7. Verify connection using retries
            max_retries = 5
            success = False
            last_error = None
            for retry in range(max_retries):
                logger.info(f"Test: Reconnect attempt {retry+1}/{max_retries}")
                try:
                    # Re-discover might be needed
                    await client.discover_services() 
                    result_after = await client.invoke("EchoService.echo", "test-after-restart")
                    assert result_after == ["test-after-restart"] # FIX: Expect list
                    success = True
                    logger.info("Test: Reconnect successful!")
                    break
                except Exception as e:
                    last_error = e
                    logger.warning(f"Test: Reconnect attempt failed: {e}")
                    await asyncio.sleep(1.0)
            
            assert success, f"Dealer failed to respond after router restart. Last error: {last_error}"

        finally:
            # Cleanup
            await client.close()
            await service.stop()
            # Stop router2, router1 is already stopped
            await router2.stop()

    @pytest.mark.asyncio
    async def test_dealer_stalls_after_timeout_detection(self, router_address, zmq_context, caplog):
        """
        测试 Dealer 检测到心跳超时后，重连逻辑是否会被外部锁阻塞。
        (Verifies the deadlock fix)
        """
        caplog.set_level(logging.INFO)

        # Config: short timeout, reconnect enabled
        heartbeat_interval = 0.1
        heartbeat_timeout = 0.3
        monitor_check_interval = max(0.1, heartbeat_timeout / 3.0)
        test_wait_timeout = heartbeat_timeout * 15 # Increased wait time slightly

        config = {
            'heartbeat_interval': heartbeat_interval,
            'heartbeat_timeout': heartbeat_timeout,
            'disable_reconnect': False # Must be False to test the monitor
        }

        # 1. Start Router
        router = ServiceRouter(router_address, context=zmq_context, heartbeat_timeout=heartbeat_timeout)
        await router.start()

        # 2. Start Dealer
        service = EchoService(router_address, context=zmq_context, **config)
        await service.start()
        # service._reconnect_protected_until = 0 # Ensure no initial protection delays detection

        await asyncio.sleep(heartbeat_interval * 3)
        assert service._connection_state == "CONNECTED"
        initial_log_len = len(caplog.text)

        # 3. Stop Router
        await router.stop()
        logger.info("Test: Router stopped.")

        # 4. Wait for "心跳超时...触发重连" logs from the monitor
        trigger_log_fragment = f"<{service._service_id}> Confirmed heartbeat timeout, triggering reconnect." # Updated log message check
        start_reconnect_call_log = f"<{service._service_id}> Calling request_reconnect..." # Check if reconnect *attempt* starts
        
        detected = False
        start_wait_time = time.time()
        while time.time() - start_wait_time < test_wait_timeout:
            new_logs = caplog.text[initial_log_len:]
            if trigger_log_fragment in new_logs and start_reconnect_call_log in new_logs:
                logger.info(f"Test: Detected timeout trigger and reconnect attempt logs for {service._service_id}")
                detected = True
                break
            await asyncio.sleep(monitor_check_interval * 0.8)

        assert detected, f"Dealer did not log timeout trigger and reconnect attempt within {test_wait_timeout}s. Logs:\n{caplog.text[initial_log_len:]}"

        # 5. Attempt to acquire the lock *after* the monitor should have tried
        #    This test now verifies the monitor *doesn't* hold the lock indefinitely.
        logs_before_lock_attempt = caplog.text
        lock_acquire_start_time = time.time()
        logger.info("Test: Attempting to acquire reconnect_lock (should succeed quickly)...")
        try:
            # Use a timeout to prevent test hanging if deadlock wasn't fixed
            async with asyncio.timeout(monitor_check_interval * 3):
                 async with service._reconnect_lock:
                    lock_acquired_time = time.time()
                    logger.info(f"Test: Acquired reconnect_lock (took {lock_acquired_time - lock_acquire_start_time:.3f}s). Deadlock likely fixed.")
                    # Hold the lock briefly
                    await asyncio.sleep(0.1)
            logger.info("Test: Released reconnect_lock.")
        except TimeoutError:
            pytest.fail(f"Failed to acquire reconnect_lock within timeout. Deadlock might still exist. Logs:\n{caplog.text[len(logs_before_lock_attempt):]}")
        except Exception as e:
             pytest.fail(f"Unexpected error acquiring lock: {e}. Logs:\n{caplog.text[len(logs_before_lock_attempt):]}")


        # 7. Cleanup
        await service.stop()
        logger.info("Test: Service stopped.")
        # Router is already stopped


class TestHeartbeatMechanism:
    """测试心跳机制，验证健康检查和服务状态监控
    
    1. 心跳超时检测
    2. 服务状态变更
    3. 异常心跳行为处理
    """
    
    @pytest.mark.asyncio
    async def test_heartbeat_timeout_detection(self, router_address, zmq_context):
        """测试心跳超时检测"""
        # 设置较短的超时
        heartbeat_timeout = 0.5
        
        # 创建Router
        router = ServiceRouter(
            router_address, 
            context=zmq_context,
            heartbeat_timeout=heartbeat_timeout
        )
        await router.start()
        
        # 创建自定义心跳服务
        class HeartbeatTestService(ServiceDealer):
            async def _heartbeat_loop(self):
                # 发送足够多的心跳确保Router能收到
                for i in range(3):  # 增加心跳次数
                    if self._socket and self._state == DealerState.RUNNING:
                        await self._socket.send_multipart([
                            b"heartbeat", 
                            json.dumps({"api_key": self._api_key}).encode()
                        ])
                    await asyncio.sleep(0.1)
                
                # 确保心跳完全停止时间足够长
                await asyncio.sleep(self._heartbeat_timeout * 2)
            
            async def _reconnect_monitor(self):
                """禁用重连监控用于测试"""
                logger.info("禁用重连监控")
                while self._state == DealerState.RUNNING:
                    check_interval = self._heartbeat_timeout / 3.0  # 尝试在超时周期内检查3次
                    check_interval = max(0.1, check_interval)     # 设置一个最小检查间隔，避免过于频繁
                    await asyncio.sleep(check_interval)

                    current_time = time.time()
                    if current_time - self._last_successful_heartbeat > self._heartbeat_timeout:
                        logger.info(f"心跳超时: {current_time - self._last_successful_heartbeat:.2f}s")
                        self._heartbeat_status = False
                        self._connection_state = "RECONNECTING"
                        self._consecutive_reconnects += 1
                        self._last_reconnect_time = current_time
                        if self._consecutive_reconnects > self._max_consecutive_reconnects:
                            self._connection_state = "PROTECTED"
                            self._reconnect_protected_until = current_time + self._heartbeat_timeout
                        await self._reconnect()
                        await self._register_to_router()
        
        # 创建并启动服务
        service = HeartbeatTestService(
            router_address, 
            context=zmq_context,
            heartbeat_timeout=heartbeat_timeout,
            disable_reconnect=True
        )
        await service.start()
        
        try:
            # 等待Router检测超时
            await asyncio.sleep(heartbeat_timeout * 3)
            
            # 检查Router中的服务状态
            if service._service_id in router._services:
                state = router._services[service._service_id].state
                logger.info(f"服务当前状态: {state}")
                assert state == ServiceState.INACTIVE, "服务应该被标记为不活跃"
            else:
                logger.info("服务已从Router中移除")
                assert True
                
        finally:
            await service.stop()
            await router.stop()
