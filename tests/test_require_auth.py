import pytest
import pytest_asyncio
import asyncio
import os
import zmq.asyncio
import logging
import tempfile

from voidrail import ServiceRouter, RouterMode
from voidrail import ServiceDealer, service_method
from voidrail import ClientDealer
from voidrail import ApiKeyManager

logger = logging.getLogger(__name__)

@pytest.fixture(autouse=True)
def setup_logging(caplog):
    """设置日志级别"""
    # 重置所有处理器的日志级别
    for handler in logging.getLogger().handlers:
        handler.setLevel(logging.INFO)
    # 设置 caplog 捕获级别
    caplog.set_level(logging.INFO)

@pytest.fixture
def zmq_context():
    """创建共享的 ZMQ Context"""
    context = zmq.asyncio.Context.instance()
    yield context

@pytest.fixture
def router_address():
    """返回路由器地址"""
    return "inproc://router_auth_test"

@pytest.fixture
def auth_files():
    """创建测试用的环境变量文件"""
    with tempfile.TemporaryDirectory() as temp_dir:
        dealer_env = os.path.join(temp_dir, "dealer.env")
        client_env = os.path.join(temp_dir, "client.env")
        router_env = os.path.join(temp_dir, "router.env")
        
        # 生成密钥
        dealer_key = ApiKeyManager.generate_key(prefix="dealer")
        client_key = ApiKeyManager.generate_key(prefix="client")
        invalid_key = ApiKeyManager.generate_key(prefix="invalid")
        
        # 创建环境变量文件
        with open(dealer_env, "w") as f:
            f.write(f"VOIDRAIL_API_KEY={dealer_key}\n")
        
        with open(client_env, "w") as f:
            f.write(f"VOIDRAIL_API_KEY={client_key}\n")
            
        with open(router_env, "w") as f:
            f.write(f"VOIDRAIL_REQUIRE_AUTH=true\n")
            f.write(f"VOIDRAIL_DEALER_API_KEYS={dealer_key}\n")
            f.write(f"VOIDRAIL_CLIENT_API_KEYS={client_key}\n")
        
        yield {
            "dealer_env": dealer_env,
            "client_env": client_env,
            "router_env": router_env,
            "dealer_key": dealer_key,
            "client_key": client_key,
            "invalid_key": invalid_key
        }

@pytest_asyncio.fixture
async def auth_router(router_address, zmq_context, auth_files):
    """创建并启动带认证的路由器"""
    # 加载环境变量
    os.environ["VOIDRAIL_REQUIRE_AUTH"] = "true"
    os.environ["VOIDRAIL_DEALER_API_KEYS"] = auth_files["dealer_key"]
    os.environ["VOIDRAIL_CLIENT_API_KEYS"] = auth_files["client_key"]
    
    router = ServiceRouter(
        router_address,
        context=zmq_context,
        heartbeat_timeout=1.0,
        router_mode=RouterMode.FIFO,
        require_auth=True,
        dealer_api_keys=[auth_files["dealer_key"]],
        client_api_keys=[auth_files["client_key"]]
    )
    await router.start()
    yield router
    
    # 清理环境变量
    if "VOIDRAIL_REQUIRE_AUTH" in os.environ:
        del os.environ["VOIDRAIL_REQUIRE_AUTH"]
    if "VOIDRAIL_DEALER_API_KEYS" in os.environ:
        del os.environ["VOIDRAIL_DEALER_API_KEYS"]
    if "VOIDRAIL_CLIENT_API_KEYS" in os.environ:
        del os.environ["VOIDRAIL_CLIENT_API_KEYS"]
    
    await router.stop()

@pytest_asyncio.fixture
async def no_auth_router(router_address, zmq_context):
    """创建并启动不需要认证的路由器"""
    router = ServiceRouter(
        router_address,
        context=zmq_context,
        heartbeat_timeout=1.0,
        router_mode=RouterMode.FIFO,
        require_auth=False
    )
    await router.start()
    yield router
    await router.stop()

@pytest_asyncio.fixture
async def valid_auth_service(auth_router, router_address, zmq_context, auth_files):
    """创建并启动有效认证的服务"""
    service = AuthTestService(
        router_address, 
        context=zmq_context, 
        api_key=auth_files["dealer_key"]
    )
    await service.start()
    yield service
    await service.stop()

@pytest_asyncio.fixture
async def valid_auth_client(router_address, zmq_context, auth_files):
    """创建有效认证的客户端，但不自动连接"""
    client = ClientDealer(
        router_address, 
        context=zmq_context, 
        timeout=2.0,
        api_key=auth_files["client_key"]
    )
    yield client
    await client.close()

@pytest_asyncio.fixture
async def invalid_auth_client(router_address, zmq_context, auth_files):
    """创建无效认证的客户端，但不自动连接"""
    client = ClientDealer(
        router_address, 
        context=zmq_context, 
        timeout=2.0,
        api_key=auth_files["invalid_key"]
    )
    yield client
    await client.close()

@pytest_asyncio.fixture
async def no_auth_client(router_address, zmq_context):
    """创建无认证的客户端，但不自动连接"""
    client = ClientDealer(
        router_address, 
        context=zmq_context, 
        timeout=2.0
    )
    yield client
    await client.close()

class AuthTestService(ServiceDealer):
    """认证测试服务"""
    def __init__(self, 
                router_address: str, 
                context=None, 
                api_key=None):
        super().__init__(
            router_address=router_address,
            context=context,
            api_key=api_key
        )
    
    @service_method
    async def echo(self, message: str) -> str:
        """简单回显服务"""
        await asyncio.sleep(0.1)
        logger.info(f"AuthTestService {self._service_id} echo: {message}")
        return message

@pytest.mark.asyncio
async def test_auth_router_info(auth_router, valid_auth_client):
    """测试认证的路由器信息"""
    await valid_auth_client.connect()
    router_info = await valid_auth_client.get_router_info()
    
    # 验证路由器认证状态
    assert router_info["auth_required"] is True

@pytest.mark.asyncio
async def test_no_auth_router_info(no_auth_router, no_auth_client):
    """测试无认证的路由器信息"""
    await no_auth_client.connect()
    router_info = await no_auth_client.get_router_info()
    
    # 验证路由器认证状态
    assert router_info["auth_required"] is False

@pytest.mark.asyncio
async def test_valid_client_auth(auth_router, valid_auth_service, valid_auth_client):
    """测试客户端有效认证"""
    await valid_auth_client.connect()
    
    # 应该能成功获取服务信息
    services = await valid_auth_client.discover_services()
    assert "AuthTestService.echo" in services
    
    # 应该能成功调用服务
    test_message = "Hello Auth World"
    async for response in valid_auth_client.stream("AuthTestService.echo", test_message):
        assert response == test_message
        break

@pytest.mark.asyncio
async def test_invalid_client_auth(auth_router, valid_auth_service, invalid_auth_client):
    """测试客户端无效认证"""
    # 连接应该失败，因为认证失败
    with pytest.raises(RuntimeError) as exc_info:
        await invalid_auth_client.connect()
    
    assert "Not authenticated" in str(exc_info.value)

@pytest.mark.asyncio
async def test_no_client_auth(auth_router, valid_auth_service, no_auth_client):
    """测试客户端无认证"""
    # 连接应该失败，因为认证失败
    with pytest.raises(RuntimeError) as exc_info:
        await no_auth_client.connect()
    
    assert "Not authenticated" in str(exc_info.value)

@pytest.mark.asyncio
async def test_valid_dealer_auth(auth_router, valid_auth_service, valid_auth_client):
    """测试处理端有效认证"""
    # 服务应该能成功启动并注册
    await valid_auth_client.connect()
    
    # 应该能发现服务
    services = await valid_auth_client.discover_services()
    assert "AuthTestService.echo" in services

@pytest.mark.asyncio
async def test_invalid_dealer_auth(auth_router, router_address, zmq_context, auth_files, valid_auth_client):
    """测试处理端无效认证"""
    # 使用无效密钥创建服务
    service = AuthTestService(
        router_address, 
        context=zmq_context, 
        api_key=auth_files["invalid_key"]
    )
    
    # 服务应该能启动，但注册应该失败
    await service.start()
    await asyncio.sleep(0.5)  # 等待注册处理
    
    try:
        await valid_auth_client.connect()
        
        # 不应该能发现服务
        services = await valid_auth_client.discover_services()
        assert "AuthTestService.echo" not in services
    finally:
        await service.stop()

@pytest.mark.asyncio
async def test_no_dealer_auth(auth_router, router_address, zmq_context, valid_auth_client):
    """测试处理端无认证"""
    # 不提供密钥创建服务
    service = AuthTestService(
        router_address, 
        context=zmq_context
    )
    
    # 服务应该能启动，但注册应该失败
    await service.start()
    await asyncio.sleep(0.5)  # 等待注册处理
    
    try:
        await valid_auth_client.connect()
        
        # 不应该能发现服务
        services = await valid_auth_client.discover_services()
        assert "AuthTestService.echo" not in services
    finally:
        await service.stop()

@pytest.mark.asyncio
async def test_no_auth_needed(no_auth_router, router_address, zmq_context, no_auth_client):
    """测试不需要认证的情况"""
    # 创建服务，不提供密钥
    service = AuthTestService(router_address, context=zmq_context)
    await service.start()
    
    try:
        await no_auth_client.connect()
        
        # 应该能成功获取服务信息
        services = await no_auth_client.discover_services()
        assert "AuthTestService.echo" in services
        
        # 应该能成功调用服务
        test_message = "Hello No Auth World"
        async for response in no_auth_client.stream("AuthTestService.echo", test_message):
            assert response == test_message
            break
    finally:
        await service.stop()

@pytest.mark.asyncio
async def test_heartbeat_auth(auth_router, router_address, zmq_context, auth_files, valid_auth_client):
    """测试心跳认证"""
    # 先创建一个有效认证的服务
    valid_service = AuthTestService(
        router_address,
        context=zmq_context,
        api_key=auth_files["dealer_key"]
    )
    await valid_service.start()
    await asyncio.sleep(0.5)  # 等待注册完成
    
    try:
        await valid_auth_client.connect()
        services = await valid_auth_client.discover_services()
        assert "AuthTestService.echo" in services
        
        # 再创建一个无效认证的服务尝试发送心跳
        invalid_service = AuthTestService(
            router_address,
            context=zmq_context,
            api_key=auth_files["invalid_key"]
        )
        
        # 手动设置服务ID，尝试冒充有效服务
        invalid_service._service_id = valid_service._service_id
        
        # 启动服务但不等待注册（只发送心跳）
        invalid_service._heartbeat_task = asyncio.create_task(
            invalid_service._heartbeat_loop()
        )
        
        await asyncio.sleep(1.0)  # 等待几个心跳周期
        
        # 如果心跳验证有效，服务应该仍然可用
        services = await valid_auth_client.discover_services()
        assert "AuthTestService.echo" in services
        
        # 停止无效服务的心跳任务
        invalid_service._heartbeat_task.cancel()
        await asyncio.gather(invalid_service._heartbeat_task, return_exceptions=True)
    finally:
        await valid_service.stop()

@pytest.mark.asyncio
async def test_multiple_services_with_auth(auth_router, router_address, zmq_context, auth_files, valid_auth_client):
    """测试多个服务的认证场景"""
    # 创建两个有效认证服务和一个无效认证服务
    valid_service1 = AuthTestService(
        router_address,
        context=zmq_context,
        api_key=auth_files["dealer_key"]
    )
    
    valid_service2 = AuthTestService(
        router_address,
        context=zmq_context,
        api_key=auth_files["dealer_key"]
    )
    
    invalid_service = AuthTestService(
        router_address,
        context=zmq_context,
        api_key=auth_files["invalid_key"]
    )
    
    # 启动所有服务
    await valid_service1.start()
    await valid_service2.start()
    await invalid_service.start()
    
    await asyncio.sleep(0.5)  # 等待注册完成
    
    try:
        await valid_auth_client.connect()
        
        # 查询集群信息
        clusters = await valid_auth_client.discover_clusters()
        
        # 应该只有两个有效服务
        active_services = [s for s in clusters.values() if s["state"] == "active"]
        assert len(active_services) == 2
        
        # 调用服务10次，确保负载均衡在两个有效服务之间进行
        service_ids = set()
        for i in range(10):
            async for response in valid_auth_client.stream("AuthTestService.echo", f"Test {i}"):
                # 这里无法直接得到处理的服务ID，但可以确认响应是正确的
                assert response == f"Test {i}"
                break
    finally:
        await valid_service1.stop()
        await valid_service2.stop()
        await invalid_service.stop()

@pytest.mark.asyncio
async def test_router_auth_env_variable(router_address, zmq_context, auth_files):
    """测试从环境变量加载认证配置"""
    # 设置环境变量
    os.environ["VOIDRAIL_REQUIRE_AUTH"] = "true"
    os.environ["VOIDRAIL_DEALER_API_KEYS"] = auth_files["dealer_key"]
    os.environ["VOIDRAIL_CLIENT_API_KEYS"] = auth_files["client_key"]
    
    # 创建路由器，不手动设置认证参数
    router = ServiceRouter(
        router_address,
        context=zmq_context,
        heartbeat_timeout=1.0
    )
    await router.start()
    
    try:
        # 验证路由器启用了认证
        assert router._require_auth is True
        assert auth_files["dealer_key"] in router._dealer_api_keys
        assert auth_files["client_key"] in router._client_api_keys
        
        # 创建有效认证的服务
        service = AuthTestService(
            router_address,
            context=zmq_context,
            api_key=auth_files["dealer_key"]
        )
        await service.start()
        
        try:
            # 创建有效认证的客户端
            client = ClientDealer(
                router_address,
                context=zmq_context,
                timeout=2.0,
                api_key=auth_files["client_key"]
            )
            
            try:
                await client.connect()
                
                # 应该能发现服务
                services = await client.discover_services()
                assert "AuthTestService.echo" in services
            finally:
                await client.close()
        finally:
            await service.stop()
    finally:
        await router.stop()
        
        # 清理环境变量
        if "VOIDRAIL_REQUIRE_AUTH" in os.environ:
            del os.environ["VOIDRAIL_REQUIRE_AUTH"]
        if "VOIDRAIL_DEALER_API_KEYS" in os.environ:
            del os.environ["VOIDRAIL_DEALER_API_KEYS"]
        if "VOIDRAIL_CLIENT_API_KEYS" in os.environ:
            del os.environ["VOIDRAIL_CLIENT_API_KEYS"] 