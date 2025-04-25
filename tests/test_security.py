import pytest
import pytest_asyncio
import asyncio
import zmq
import os
import tempfile
import uuid
import logging
from pathlib import Path
import json
import zmq.auth
import time

from voidrail import ServiceRouter, ServiceDealer, ClientDealer, service_method, ServiceState
from voidrail.security import generate_curve_keys

# 启用更详细的日志记录
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@pytest.fixture(scope="function")
def temp_keys_dir():
    """创建临时目录用于存储密钥"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname

@pytest.fixture(scope="function")
def server_keys(temp_keys_dir):
    """生成并返回服务器密钥"""
    keys = generate_curve_keys(temp_keys_dir)
    return keys  # 返回完整的密钥信息

@pytest.fixture(scope="function")
def router_address():
    """每次测试使用唯一的地址"""
    return f"tcp://127.0.0.1:{5000 + uuid.uuid4().int % 10000}"

@pytest.fixture(scope="function")
def zmq_context():
    """创建ZMQ Context并确保清理"""
    context = zmq.asyncio.Context()
    yield context
    # 强制关闭并清理所有套接字
    context.destroy(linger=0)

class SecureEchoService(ServiceDealer):
    """用于测试的加密服务"""
    
    @service_method
    async def echo(self, message):
        """简单回显服务"""
        logger.debug(f"服务收到消息: {message}")
        return message

@pytest.mark.asyncio
async def test_curve_communication_minimal(server_keys, router_address, zmq_context):
    """极简测试：直接测试 ZMQ ROUTER <-> DEALER 的 CURVE 通信"""
    server_public_key, server_secret_key = zmq.auth.load_certificate(server_keys["secret_file"])
    client_public_key, client_secret_key = zmq.curve_keypair()

    # --- 服务器端 (模拟 Router 的核心) ---
    server_socket = zmq_context.socket(zmq.ROUTER)
    server_socket.curve_secretkey = server_secret_key
    server_socket.curve_publickey = server_public_key
    server_socket.curve_server = True  # 启用服务器模式
    server_socket.bind(router_address)
    logger.info(f"Minimal Server: Bound to {router_address}, CURVE enabled.")

    server_task = None
    received_message = None
    server_ready = asyncio.Event()

    async def server_run():
        nonlocal received_message
        logger.info("Minimal Server: Task started, waiting for connection...")
        server_ready.set() # 通知客户端可以连接了
        try:
            # 等待一个消息
            multipart = await asyncio.wait_for(server_socket.recv_multipart(), timeout=7.0) # 增加超时
            logger.info(f"Minimal Server: Received message: {multipart}")
            received_message = multipart # 存储收到的消息
            # 回复一个简单的 pong
            client_identity = multipart[0]
            await server_socket.send_multipart([client_identity, b"pong"])
            logger.info("Minimal Server: Sent pong reply.")
        except asyncio.TimeoutError:
             logger.error("Minimal Server: Timeout waiting for message.")
        except Exception as e:
            logger.error(f"Minimal Server: Error - {e}", exc_info=True)
        finally:
             logger.info("Minimal Server: Task finishing.")


    # --- 客户端 (模拟测试代码的核心) ---
    client_socket = zmq_context.socket(zmq.DEALER)
    client_id = f"minimal_client_{uuid.uuid4().hex[:8]}".encode()
    client_socket.identity = client_id
    client_socket.curve_secretkey = client_secret_key
    client_socket.curve_publickey = client_public_key
    client_socket.curve_serverkey = server_public_key # 设置服务器公钥
    logger.info("Minimal Client: Configured.")


    try:
        # 启动服务器任务
        server_task = asyncio.create_task(server_run())
        await server_ready.wait() # 等待服务器绑定完成

        # 客户端连接
        logger.info(f"Minimal Client: Connecting to {router_address}...")
        client_socket.connect(router_address)
        await asyncio.sleep(1.0) # 等待握手时间
        logger.info("Minimal Client: Connected (presumably).")

        # 发送测试消息
        logger.info("Minimal Client: Sending ping...")
        await client_socket.send_multipart([b"ping", b"test data"])
        logger.info("Minimal Client: Ping sent.")

        # 等待回复
        logger.info("Minimal Client: Waiting for pong reply...")
        try:
            reply = await asyncio.wait_for(client_socket.recv_multipart(), timeout=5.0)
            logger.info(f"Minimal Client: Received reply: {reply}")
            assert reply == [b"pong"], f"Expected [b'pong'], got {reply}"
            logger.info("Minimal CURVE communication test PASSED.")
            # 验证服务器是否收到了正确的 ping 消息
            assert received_message is not None, "Server did not receive any message"
            assert received_message[1:] == [b"ping", b"test data"], f"Server received wrong message: {received_message}"

        except asyncio.TimeoutError:
            logger.error("Minimal Client: Timeout waiting for reply.")
            assert False, "Minimal CURVE test failed: Timeout waiting for reply."
        except AssertionError as e:
             logger.error(f"Minimal Client: Assertion failed - {e}")
             assert False, f"Minimal CURVE test failed: {e}"


    finally:
        logger.info("Minimal Test: Cleaning up...")
        client_socket.close(linger=0)
        server_socket.close(linger=0)
        if server_task and not server_task.done():
            server_task.cancel()
            try:
                await server_task
            except asyncio.CancelledError:
                pass
        logger.info("Minimal Test: Cleanup complete.")


@pytest.mark.asyncio
async def test_curve_communication(server_keys, router_address, zmq_context):
    """使用直接套接字测试CURVE加密的 'methods' 请求"""
    server_public = server_keys["public_key"]

    # 启动Router
    router = ServiceRouter(
        router_address,
        context=zmq_context,
        curve_server_key_file=server_keys["secret_file"],
        logger_level=logging.DEBUG  # 开启详细日志
    )
    logger.debug("启动Router...")
    await router.start()
    await asyncio.sleep(1.0)  # 等待Router启动
    logger.debug("Router应该已启动")

    # 创建直接套接字连接
    socket = zmq_context.socket(zmq.DEALER)
    client_id = f"test_client_{uuid.uuid4().hex[:8]}".encode()
    socket.identity = client_id
    logger.debug(f"客户端ID: {client_id.decode()}")

    # 配置CURVE
    logger.debug("配置客户端CURVE密钥...")
    client_public, client_secret = zmq.curve_keypair()
    socket.curve_secretkey = client_secret
    socket.curve_publickey = client_public
    socket.curve_serverkey = server_public
    logger.debug("客户端CURVE密钥配置完成")

    logger.debug(f"连接到Router地址: {router_address}...")
    socket.connect(router_address)
    await asyncio.sleep(0.5) # 等待连接建立
    logger.debug("应该已连接到Router")

    try:
        # 发送 'methods' 请求，Router应该能处理这个
        logger.debug("发送 'methods' 请求...")
        await socket.send_multipart([b"methods", b""]) # <---- 修改点：发送 methods
        logger.debug("'methods' 请求已发送")

        try:
            # 较长超时等待响应
            logger.debug("等待响应...")
            response = await asyncio.wait_for(socket.recv_multipart(), timeout=5.0)
            logger.debug(f"收到响应: {response}")

            # Router 应该返回一个包含 'type': 'reply' 和 'result': {} 的JSON消息
            assert len(response) >= 1, "响应不应为空"
            try:
                response_data = json.loads(response[-1].decode())
                assert response_data.get("type") == "reply", f"响应类型应为 'reply', 收到: {response_data.get('type')}"
                assert response_data.get("result") == {}, f"响应结果应为空字典 (无服务注册), 收到: {response_data.get('result')}"
                logger.info("CURVE加密的 'methods' 请求测试成功")
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                assert False, f"响应无法解析为JSON: {e}, 原始响应: {response[-1]}"
            except AssertionError as e:
                 assert False, f"响应内容断言失败: {e}"

        except asyncio.TimeoutError:
            logger.error("等待响应超时")
            assert False, "Router未响应 'methods' 请求，CURVE通信或请求处理失败"

    finally:
        logger.debug("关闭套接字和Router...")
        socket.close(linger=0)
        await router.stop()
        logger.debug("测试清理完成")


class TestCurveKeyManagement:
    """测试椭圆曲线密钥管理功能"""
    
    def test_generate_curve_keys(self, temp_keys_dir):
        """测试密钥生成功能"""
        keys = generate_curve_keys(temp_keys_dir)
        assert "public_file" in keys
        assert "secret_file" in keys
        assert os.path.exists(keys["public_file"])
        assert os.path.exists(keys["secret_file"])
        
        # 验证返回了十六进制格式的公钥
        assert "public_key_hex" in keys
        assert isinstance(keys["public_key_hex"], str)
        assert "fingerprint" in keys
        assert len(keys["fingerprint"]) == 16  # 指纹长度为16个字符

@pytest.mark.asyncio
async def test_secure_echo(router_address, zmq_context, server_keys):
    """基础加密通信测试"""
    logger.debug("开始基础加密通信测试")
    
    # 创建并启动路由器
    router = ServiceRouter(
        router_address, 
        context=zmq_context,
        heartbeat_timeout=0.5,
        curve_server_key_file=server_keys["secret_file"],  # 只提供服务器密钥文件
    )
    
    try:
        await router.start()
        await asyncio.sleep(0.5)  # 等待Router完全启动
        logger.debug("路由器启动成功")
        
        # 创建并启动服务
        service = SecureEchoService(
            router_address,
            context=zmq_context,
            heartbeat_interval=0.1,
            heartbeat_timeout=0.5,
            curve_server_key=server_keys["public_key"]  # 只提供服务器公钥
        )
        
        try:
            await service.start()
            await asyncio.sleep(1.0)  # 给服务更多时间注册
            logger.debug("服务启动成功")
            
            # 等待服务注册
            await asyncio.sleep(1.0)
            
            # 创建客户端
            client = ClientDealer(
                router_address, 
                context=zmq_context,
                timeout=5.0,  # 大幅增加超时时间
                curve_server_key=server_keys["public_key"]
            )
            
            # 等待连接建立
            await asyncio.sleep(0.5)
            
            try:
                # 测试通信
                test_message = "安全通信测试"
                response = None
                
                async for resp in client.stream("SecureEchoService.echo", test_message):
                    logger.debug(f"收到响应: {resp}")
                    response = resp
                    break
                
                assert response == test_message, "回显服务返回的消息与发送的不一致"
                logger.debug("测试通过：成功收到正确响应")
                
            finally:
                await client.close()
        finally:
            await service.stop()
    finally:
        await router.stop()

@pytest.mark.asyncio
async def test_env_key_communication(router_address, zmq_context, server_keys, monkeypatch):
    """测试通过环境变量配置公钥的通信"""
    # 设置环境变量
    monkeypatch.setenv("VOIDRAIL_CURVE_SERVER_KEY", server_keys["public_key_hex"])
    
    # 创建并启动路由器
    router = ServiceRouter(
        router_address,
        context=zmq_context,
        curve_server_key_file=server_keys["secret_file"]
    )
    
    try:
        await router.start()
        
        # 创建服务 - 不直接提供公钥，从环境变量获取
        service = SecureEchoService(
            router_address,
            context=zmq_context
            # 无需提供curve_server_key，将从环境变量读取
        )
        
        try:
            await service.start()
            await asyncio.sleep(0.3)
            
            # 创建客户端 - 不直接提供公钥，从环境变量获取
            client = ClientDealer(
                router_address,
                context=zmq_context,
                timeout=2.0
                # 无需提供curve_server_key，将从环境变量读取  
            )
            
            # 测试通信
            test_message = "环境变量配置测试"
            response = None
            
            async for resp in client.stream("SecureEchoService.echo", test_message):
                response = resp
                break
            
            assert response == test_message, "通过环境变量配置的加密通信失败"
            
            await client.close()
        finally:
            await service.stop()
    finally:
        await router.stop()

@pytest.mark.asyncio
async def test_unauthorized_client_rejection(router_address, zmq_context, server_keys):
    """测试未授权客户端被拒绝连接"""
    # 创建并启动路由器
    router = ServiceRouter(
        router_address,
        context=zmq_context,
        curve_server_key_file=server_keys["secret_file"],
        client_api_keys=["authorized_key"]  # 设置API密钥验证
    )
    
    try:
        await router.start()
        
        # 创建服务
        service = SecureEchoService(
            router_address,
            context=zmq_context,
            api_key="authorized_key",  # 使用有效的API密钥
            curve_server_key=server_keys["public_key"]
        )
        
        try:
            await service.start()
            await asyncio.sleep(0.3)
            
            # 创建未授权客户端 - 加密正确但API密钥错误
            client = ClientDealer(
                router_address,
                context=zmq_context,
                timeout=1.0,
                curve_server_key=server_keys["public_key"],
                api_key="wrong_key"  # 使用错误的API密钥
            )
            
            # 预期发现服务请求会被拒绝
            with pytest.raises(Exception):
                await asyncio.wait_for(
                    client.discover_services(),
                    timeout=1.0
                )
            
            await client.close()
        finally:
            await service.stop()
    finally:
        await router.stop()

@pytest.mark.asyncio
async def test_secure_echo_with_auth(router_address, zmq_context, server_keys):
    """测试使用ClientDealer/ServiceDealer进行带API Key认证的CURVE加密通信"""
    logger.debug("开始带认证的CURVE加密通信测试 (使用框架类)")

    VALID_API_KEY = f"secure-key-{uuid.uuid4().hex}" # 每次测试使用唯一Key

    # 1. 启动带认证和CURVE的Router
    #    Router 需要配置服务器密钥文件以启用CURVE
    #    Router 需要配置 client_api_keys 以启用客户端API Key认证
    router = ServiceRouter(
        router_address,
        context=zmq_context,
        curve_server_key_file=server_keys["secret_file"],
        client_api_keys=[VALID_API_KEY], # <--- 启用客户端API Key认证
        heartbeat_timeout=2.0, # 使用稍长的超时增加稳定性
        logger_level=logging.DEBUG
    )
    await router.start()
    await asyncio.sleep(0.5) # 等待Router启动
    logger.debug(f"带认证的Router启动成功，地址: {router_address}")

    # 2. 启动带CURVE的服务
    #    ServiceDealer 需要配置服务器公钥以启用CURVE客户端模式
    #    注意：ServiceDealer注册本身通常不需要API Key (除非Router配置了dealer_api_keys)
    service = SecureEchoService(
        router_address,
        context=zmq_context,
        curve_server_key=server_keys["public_key"], # Dealer连接Router需要服务器公钥
        heartbeat_interval=0.5,
        heartbeat_timeout=2.0,
        logger_level=logging.DEBUG
    )
    service_id = service._service_id # 获取服务ID用于日志
    logger.debug(f"启动服务 {service_id}...")
    await service.start()
    # 等待服务注册完成
    # 可以轮询 Router 的服务列表来确认，而不是固定等待
    for _ in range(10): # 最多等待1秒
        if service_id in router._services and router._services[service_id].state == ServiceState.ACTIVE:
             logger.debug(f"服务 {service_id} 已在Router注册并激活")
             break
        await asyncio.sleep(0.1)
    else:
         pytest.fail(f"服务 {service_id} 未能在预期时间内注册到Router")


    # 3. 创建带正确API Key和CURVE的客户端
    #    ClientDealer 需要配置服务器公钥以启用CURVE
    #    ClientDealer 需要配置正确的 api_key 以通过Router认证
    authorized_client = ClientDealer(
        router_address,
        context=zmq_context,
        timeout=5.0,
        curve_server_key=server_keys["public_key"],
        api_key=VALID_API_KEY, # <--- 提供正确的API Key
        logger_level=logging.DEBUG
    )
    logger.debug(f"创建授权客户端 (ID: {authorized_client._client_id})，API Key: {VALID_API_KEY}")

    # 4. 创建带错误API Key和CURVE的客户端 (用于验证拒绝)
    unauthorized_client = ClientDealer(
        router_address,
        context=zmq_context,
        timeout=2.0, # 短超时即可
        curve_server_key=server_keys["public_key"],
        api_key="wrong-key", # <--- 提供错误的API Key
        logger_level=logging.DEBUG
    )
    logger.debug(f"创建未授权客户端 (ID: {unauthorized_client._client_id})，API Key: wrong-key")

    try:
        # 5. 测试授权客户端的通信
        #    调用 ClientDealer 的方法，它会处理连接、认证和消息格式化
        logger.debug(f"测试授权客户端 {authorized_client._client_id} 调用 SecureEchoService.echo...")
        test_message = f"安全消息 @ {time.time()}"
        response = None
        # 使用 invoke 获取单个回复
        response_list = await authorized_client.invoke("SecureEchoService.echo", test_message)
        assert len(response_list) == 1, "Invoke 对于非流式方法应返回包含一个元素的列表"
        response = response_list[0]

        assert response == test_message, f"授权客户端的回显消息不匹配, 期望 '{test_message}', 收到 '{response}'"
        logger.info(f"授权客户端 ({authorized_client._client_id}) 通信成功")

        # 6. 测试未授权客户端被拒绝
        logger.debug(f"测试未授权客户端 {unauthorized_client._client_id} 调用 SecureEchoService.echo...")
        with pytest.raises(Exception) as exc_info:
            # 尝试调用服务，预期失败。失败可能发生在 connect/authenticate 或 invoke 阶段
            # invoke 会触发 connect（如果未连接），connect 会触发 _authenticate
            await unauthorized_client.invoke("SecureEchoService.echo", "此消息不应发送")

        # 检查异常类型或消息。认证失败可能导致超时或特定的运行时错误
        logger.info(f"未授权客户端 ({unauthorized_client._client_id}) 按预期失败: {exc_info.type.__name__} - {exc_info.value}")
        # 异常消息应该包含认证失败、超时或找不到服务等信息
        error_msg = str(exc_info.value).lower()
        assert "auth" in error_msg or \
               "failed" in error_msg or \
               "timeout" in error_msg or \
               "not found" in error_msg or \
               "认证" in error_msg, \
               f"异常信息 '{error_msg}' 应表明认证失败或超时"

    finally:
        logger.debug("清理测试资源...")
        await authorized_client.close()
        await unauthorized_client.close()
        await service.stop()
        await router.stop()
        logger.debug("测试清理完成")
