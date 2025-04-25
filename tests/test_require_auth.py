import pytest
import pytest_asyncio
import asyncio
import os
import zmq.asyncio
import logging
import tempfile
import uuid

from voidrail import ServiceRouter
from voidrail import ServiceDealer, service_method
from voidrail import ClientDealer
from voidrail import ApiKeyManager

logger = logging.getLogger(__name__)

@pytest.fixture(autouse=True)
def setup_logging(caplog):
    """设置日志级别为 DEBUG"""
    logging.getLogger().setLevel(logging.INFO)
    for handler in logging.getLogger().handlers:
        handler.setLevel(logging.INFO)
    caplog.set_level(logging.INFO)

@pytest.fixture
def zmq_context():
    """创建独立的 ZMQ Context 并确保终止"""
    context = zmq.asyncio.Context()
    yield context
    logger.debug("Attempting to terminate ZMQ context...")
    context.term()
    logger.debug("ZMQ context terminated.")

@pytest.fixture
def router_address():
    """返回唯一的路由器地址，避免测试间冲突"""
    return f"inproc://router_auth_test_{uuid.uuid4().hex}"

@pytest.fixture
def auth_keys():
    """生成测试用的API Keys"""
    dealer_key = ApiKeyManager.generate_key(prefix="dealer")
    client_key = ApiKeyManager.generate_key(prefix="client")
    invalid_key = ApiKeyManager.generate_key(prefix="invalid")
    return {
            "dealer_key": dealer_key,
            "client_key": client_key,
            "invalid_key": invalid_key
        }

@pytest_asyncio.fixture
async def both_auth_router(router_address, zmq_context, auth_keys):
    """创建并启动需要双重认证的路由器 (DEBUG log)"""
    router = ServiceRouter(
        router_address,
        context=zmq_context,
        heartbeat_timeout=1.0,
        idle_heartbeat_check=0.5,
        dealer_api_keys=[auth_keys["dealer_key"]],
        client_api_keys=[auth_keys["client_key"]],
        logger_level=logging.DEBUG
    )
    await router.start()
    yield router
    await router.stop()

@pytest_asyncio.fixture
async def only_client_auth_router(router_address, zmq_context, auth_keys):
    """创建并启动仅需客户端认证的路由器 (DEBUG log)"""
    router = ServiceRouter(
        router_address,
        context=zmq_context,
        heartbeat_timeout=1.0,
        idle_heartbeat_check=0.5,
        client_api_keys=[auth_keys["client_key"]],
        logger_level=logging.DEBUG
    )
    await router.start()
    yield router
    await router.stop()

@pytest_asyncio.fixture
async def only_dealer_auth_router(router_address, zmq_context, auth_keys):
    """创建并启动仅需处理端认证的路由器 (DEBUG log)"""
    router = ServiceRouter(
        router_address,
        context=zmq_context,
        heartbeat_timeout=1.0,
        idle_heartbeat_check=0.5,
        dealer_api_keys=[auth_keys["dealer_key"]],
        logger_level=logging.DEBUG
    )
    await router.start()
    yield router
    await router.stop()

@pytest_asyncio.fixture
async def no_auth_router(router_address, zmq_context):
    """创建并启动不需要认证的路由器 (DEBUG log)"""
    router = ServiceRouter(
        router_address, 
        context=zmq_context, 
        heartbeat_timeout=1.0,
        idle_heartbeat_check=0.5,
        logger_level=logging.DEBUG
    )
    await router.start()
    yield router
    await router.stop()

class AuthTestService(ServiceDealer):
    """认证测试服务"""
    def __init__(self,
                router_address: str,
                context=None,
                api_key=None,
                service_id_suffix="",
                **kwargs):
        service_id = f"AuthTestService-{service_id_suffix}-{uuid.uuid4().hex[:4]}"

        kwargs.setdefault('logger_level', logging.DEBUG)
        kwargs.setdefault('heartbeat_interval', 0.1)
        kwargs.setdefault('heartbeat_timeout', 0.5)

        super().__init__(
            router_address=router_address,
            context=context,
            api_key=api_key,
            service_id=service_id,
            group=service_id,
            **kwargs
        )
    
    @service_method
    async def echo(self, message: str) -> str:
        """简单回显服务"""
        logger.debug(f"{self._service_id} echoing: {message}")
        return message

@pytest_asyncio.fixture
async def valid_client(router_address, zmq_context, auth_keys):
    """创建有效认证的客户端 (DEBUG log)"""
    client = ClientDealer(
        router_address, 
        context=zmq_context, 
        timeout=2.0,
        api_key=auth_keys["client_key"],
        logger_level=logging.DEBUG
    )
    yield client
    await client.close()

@pytest_asyncio.fixture
async def invalid_client(router_address, zmq_context, auth_keys):
    """创建无效认证的客户端 (DEBUG log)"""
    client = ClientDealer(
        router_address, 
        context=zmq_context, 
        timeout=1.0,
        api_key=auth_keys["invalid_key"],
        logger_level=logging.DEBUG
    )
    yield client
    await client.close()

@pytest_asyncio.fixture
async def no_key_client(router_address, zmq_context):
    """创建无认证密钥的客户端 (DEBUG log)"""
    client = ClientDealer(
        router_address, 
        context=zmq_context, 
        timeout=1.0,
        logger_level=logging.DEBUG
    )
    yield client
    await client.close()

@pytest.mark.asyncio
async def test_auth_router_info(both_auth_router, valid_client):
    """测试认证的路由器信息 (with timeout)"""
    await asyncio.wait_for(valid_client.connect(), timeout=3.0)
    
    # 显式等待一小段时间确保认证完成
    await asyncio.sleep(0.1)
    
    router_info = await asyncio.wait_for(valid_client.get_router_info(), timeout=3.0)
    assert router_info["client_api_keys_require"] is True
    assert router_info["dealer_api_keys_require"] is True

@pytest.mark.asyncio
async def test_no_auth_router_info(no_auth_router, no_key_client):
    """测试无认证的路由器信息 (with timeout)"""
    await asyncio.wait_for(no_key_client.connect(), timeout=3.0)
    router_info = await asyncio.wait_for(no_key_client.get_router_info(), timeout=3.0)
    assert router_info["client_api_keys_require"] is False
    assert router_info["dealer_api_keys_require"] is False

@pytest.mark.asyncio
async def test_only_client_auth_router_info(only_client_auth_router, valid_client):
    """测试仅客户端认证的路由器信息 (with timeout)"""
    await asyncio.wait_for(valid_client.connect(), timeout=3.0)
    router_info = await asyncio.wait_for(valid_client.get_router_info(), timeout=3.0)
    assert router_info["client_api_keys_require"] is True
    assert router_info["dealer_api_keys_require"] is False

@pytest.mark.asyncio
async def test_only_dealer_auth_router_info(only_dealer_auth_router, no_key_client):
    """测试仅处理端认证的路由器信息 (with timeout)"""
    await asyncio.wait_for(no_key_client.connect(), timeout=3.0)
    router_info = await asyncio.wait_for(no_key_client.get_router_info(), timeout=3.0)
    assert router_info["client_api_keys_require"] is False
    assert router_info["dealer_api_keys_require"] is True

@pytest.mark.asyncio
async def test_no_auth_router_allows_all(no_auth_router, router_address, zmq_context, no_key_client, auth_keys):
    """测试无认证Router允许任何Dealer和Client (with timeouts)"""
    service_no_key = AuthTestService(router_address, context=zmq_context, service_id_suffix="no_key_svc")
    await service_no_key.start()
    await asyncio.sleep(0.2)
    assert service_no_key._service_id in no_auth_router._services
    logger.info(f"test_no_auth_router_allows_all >> step1")

    service_with_key = AuthTestService(router_address, context=zmq_context, api_key=auth_keys['dealer_key'], service_id_suffix="with_key_svc")
    await service_with_key.start()
    await asyncio.sleep(0.2)
    assert service_with_key._service_id in no_auth_router._services
    logger.info(f"test_no_auth_router_allows_all >> step2")

    method_no_key = f"{service_no_key._service_id}.echo"
    method_with_key = f"{service_with_key._service_id}.echo"

    try:
        await asyncio.wait_for(no_key_client.connect(), timeout=3.0)
        services = await asyncio.wait_for(no_key_client.discover_services(), timeout=3.0)
        logger.debug(f"Discovered services (no_key_client): {services.keys()}")
        assert method_no_key in services
        assert method_with_key in services
        
        msg = "no_key_client_msg"
        resp = await asyncio.wait_for(no_key_client.invoke(method_no_key, msg), timeout=3.0)
        assert resp[0] == msg

        with pytest.raises(RuntimeError) as exc_info_invalid:
            client = ClientDealer(router_address, context=zmq_context, api_key=auth_keys['client_key'], logger_level=logging.DEBUG)
            await client.connect()
        assert "no need to authenticate" in str(exc_info_invalid.value).lower()

    finally:
        await asyncio.wait_for(service_no_key.stop(), timeout=2.0)
        await asyncio.wait_for(service_with_key.stop(), timeout=2.0)

@pytest.mark.asyncio
async def test_only_client_auth_required(only_client_auth_router, router_address, zmq_context, valid_client, invalid_client, no_key_client, auth_keys):
    """测试仅需Client认证的场景 (with timeouts)"""
    router = only_client_auth_router
    service_no_key = AuthTestService(router_address, context=zmq_context, service_id_suffix="no_key_svc")
    await service_no_key.start()
    await asyncio.sleep(0.2)
    assert service_no_key._service_id in router._services

    service_with_key = AuthTestService(router_address, context=zmq_context, api_key=auth_keys['dealer_key'], service_id_suffix="with_key_svc")
    await service_with_key.start()
    await asyncio.sleep(0.2)
    assert service_with_key._service_id in router._services

    method_no_key = f"{service_no_key._service_id}.echo"
    method_with_key = f"{service_with_key._service_id}.echo"

    try:
        await asyncio.wait_for(valid_client.connect(), timeout=3.0)
        services = await asyncio.wait_for(valid_client.discover_services(), timeout=3.0)
        assert method_no_key in services
        assert method_with_key in services
        
        msg = "valid_client_msg"
        resp = await asyncio.wait_for(valid_client.invoke(method_no_key, msg), timeout=3.0)
        assert resp[0] == msg

        with pytest.raises((RuntimeError, asyncio.TimeoutError, zmq.ZMQError)) as exc_info_invalid:
            await asyncio.wait_for(invalid_client.connect(), timeout=2.0)
            await asyncio.wait_for(invalid_client.invoke(method_no_key, "should fail"), timeout=2.0)
        logger.debug(f"Invalid client exception: {exc_info_invalid.value}")
        assert "auth" in str(exc_info_invalid.value).lower() or \
               "认证" in str(exc_info_invalid.value).lower() or \
               "timeout" in str(exc_info_invalid.value).lower()

        with pytest.raises((RuntimeError, asyncio.TimeoutError, zmq.ZMQError)) as exc_info_no_key:
            await asyncio.wait_for(no_key_client.connect(), timeout=2.0)
            await asyncio.wait_for(no_key_client.invoke(method_no_key, "should fail"), timeout=2.0)
        logger.debug(f"No key client exception: {exc_info_no_key.value}")
        assert "auth" in str(exc_info_no_key.value).lower() or \
               "认证" in str(exc_info_no_key.value).lower() or \
               "timeout" in str(exc_info_no_key.value).lower()

    finally:
        await asyncio.wait_for(service_no_key.stop(), timeout=2.0)
        await asyncio.wait_for(service_with_key.stop(), timeout=2.0)

@pytest.mark.asyncio
async def test_only_dealer_auth_required(only_dealer_auth_router, router_address, zmq_context, no_key_client, valid_client, auth_keys):
    """测试仅需Dealer认证的场景 (with timeouts)"""
    router = only_dealer_auth_router

    service_valid_key = AuthTestService(router_address, context=zmq_context, api_key=auth_keys['dealer_key'], service_id_suffix="valid_key_svc")
    await service_valid_key.start()
    await asyncio.sleep(0.2)
    assert service_valid_key._service_id in router._services
    method_valid = f"{service_valid_key._service_id}.echo"

    service_invalid_key = AuthTestService(router_address, context=zmq_context, api_key=auth_keys['invalid_key'], service_id_suffix="invalid_key_svc")
    await service_invalid_key.start()
    await asyncio.sleep(0.2)
    assert service_invalid_key._service_id not in router._services

    service_no_key_dealer = AuthTestService(router_address, context=zmq_context, service_id_suffix="no_key_dealer_svc")
    await service_no_key_dealer.start()
    await asyncio.sleep(0.2)
    assert service_no_key_dealer._service_id not in router._services

    try:
        await asyncio.wait_for(no_key_client.connect(), timeout=3.0)
        services = await asyncio.wait_for(no_key_client.discover_services(), timeout=3.0)
        assert method_valid in services
        assert f"{service_invalid_key._service_id}.echo" not in services
        assert f"{service_no_key_dealer._service_id}.echo" not in services
        
        msg = "no_key_client_msg"
        resp = await asyncio.wait_for(no_key_client.invoke(method_valid, msg), timeout=3.0)
        assert resp[0] == msg

        with pytest.raises((RuntimeError, asyncio.TimeoutError, zmq.ZMQError)) as exc_info_invalid:
            await asyncio.wait_for(valid_client.connect(), timeout=3.0)
            await asyncio.wait_for(valid_client.invoke(method_valid, "should fail"), timeout=3.0)
        assert "no need to authenticate" in str(exc_info_invalid.value).lower()

    finally:
        await asyncio.wait_for(service_valid_key.stop(), timeout=2.0)
        try:
            await asyncio.wait_for(service_invalid_key.stop(), timeout=2.0)
        except Exception as e:
            logger.warning(f"Error stopping invalid_key service (may be expected): {e}")
        try:
            await asyncio.wait_for(service_no_key_dealer.stop(), timeout=2.0)
        except Exception as e:
            logger.warning(f"Error stopping no_key_dealer service (may be expected): {e}")

@pytest.mark.asyncio
async def test_both_auth_required(both_auth_router, router_address, zmq_context, valid_client, invalid_client, no_key_client, auth_keys):
    """测试需要双重认证的场景 (with timeouts)"""
    router = both_auth_router

    service_valid_dealer = AuthTestService(router_address, context=zmq_context, api_key=auth_keys['dealer_key'], service_id_suffix="valid_dealer_svc")
    await service_valid_dealer.start()
    await asyncio.sleep(0.2)
    assert service_valid_dealer._service_id in router._services
    method_valid = f"{service_valid_dealer._service_id}.echo"

    service_invalid_dealer = AuthTestService(router_address, context=zmq_context, api_key=auth_keys['invalid_key'], service_id_suffix="invalid_dealer_svc")
    await service_invalid_dealer.start()
    await asyncio.sleep(0.2)
    assert service_invalid_dealer._service_id not in router._services

    service_no_key_dealer = AuthTestService(router_address, context=zmq_context, service_id_suffix="no_key_dealer_svc")
    await service_no_key_dealer.start()
    await asyncio.sleep(0.2)
    assert service_no_key_dealer._service_id not in router._services

    try:
        await asyncio.wait_for(valid_client.connect(), timeout=3.0)
        services = await asyncio.wait_for(valid_client.discover_services(), timeout=3.0)
        assert method_valid in services
        assert f"{service_invalid_dealer._service_id}.echo" not in services
        assert f"{service_no_key_dealer._service_id}.echo" not in services
        
        msg = "valid_client_msg"
        resp = await asyncio.wait_for(valid_client.invoke(method_valid, msg), timeout=3.0)
        assert resp[0] == msg

        with pytest.raises((RuntimeError, asyncio.TimeoutError, zmq.ZMQError)) as exc_info_invalid:
            await asyncio.wait_for(invalid_client.connect(), timeout=2.0)
            await asyncio.wait_for(invalid_client.invoke(method_valid, "should fail"), timeout=2.0)
        logger.debug(f"Invalid client exception: {exc_info_invalid.value}")
        assert "auth" in str(exc_info_invalid.value).lower() or \
               "认证" in str(exc_info_invalid.value).lower() or \
               "timeout" in str(exc_info_invalid.value).lower()

        with pytest.raises((RuntimeError, asyncio.TimeoutError, zmq.ZMQError)) as exc_info_no_key:
            await asyncio.wait_for(no_key_client.connect(), timeout=2.0)
            await asyncio.wait_for(no_key_client.invoke(method_valid, "should fail"), timeout=2.0)
        logger.debug(f"No key client exception: {exc_info_no_key.value}")
        assert "auth" in str(exc_info_no_key.value).lower() or \
               "认证" in str(exc_info_no_key.value).lower() or \
               "timeout" in str(exc_info_no_key.value).lower()

    finally:
        await asyncio.wait_for(service_valid_dealer.stop(), timeout=2.0)
        try:
            await asyncio.wait_for(service_invalid_dealer.stop(), timeout=2.0)
        except Exception as e:
            logger.warning(f"Error stopping invalid_dealer service (may be expected): {e}")
        try:
            await asyncio.wait_for(service_no_key_dealer.stop(), timeout=2.0)
        except Exception as e:
            logger.warning(f"Error stopping no_key_dealer service (may be expected): {e}")

@pytest.fixture(scope="session", autouse=True)
def _close_asyncio_tasks():
    yield
    loop = asyncio.get_event_loop()
    pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
    if pending:
        logging.warning(f"Force-cancel {len(pending)} pending tasks")
        for t in pending:
            t.cancel()
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True)) 