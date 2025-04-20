from typing import Dict, Any, Optional, AsyncGenerator
import zmq
import zmq.asyncio
import asyncio
import logging
import json
import uuid
import os
from contextlib import asynccontextmanager

class ClientDealer:
    """客户端 DEALER 实现，按需连接"""
    def __init__(
        self,
        router_address: str,
        context: Optional[zmq.asyncio.Context] = None,
        timeout: Optional[float] = None,
        api_key: Optional[str] = None,
        logger_level: int = logging.INFO,
    ):
        self._router_address = router_address
        self._timeout = timeout
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logger_level)
        self._context = context or zmq.asyncio.Context.instance()
        self._socket = None
        self._lock = asyncio.Lock()
        self._connected = False
        self._client_id = str(uuid.uuid4().hex)
        self._available_methods = {}  # 缓存可用方法
        
        # API密钥设置
        self._api_key = api_key or os.environ.get("VOIDRAIL_API_KEY")
        if not self._api_key:
            self._logger.warning(f"ClientDealer: 未设置API密钥，可能无法连接到开启了验证的Router")
        
        # 认证状态
        self._authenticated = False

    async def connect(self):
        """连接到路由器"""
        self._logger.info(f"Connecting to router at {self._router_address}, {self._socket}")
        if not self._socket:
            self._socket = self._context.socket(zmq.DEALER)
            self._socket.identity = self._client_id.encode()
            self._socket.connect(self._router_address)
            
            # 如果有API密钥，先尝试认证
            if self._api_key:
                await self._authenticate()
            
            self._connected = True
            self._logger.info(f"Connected to router at {self._router_address}, {self._socket}")

            # 连接后立即更新可用方法
            await self.discover_services()

    async def _authenticate(self):
        """向Router发送认证请求"""
        if self._authenticated:
            return True
            
        try:
            auth_request = {
                "api_key": self._api_key,
                "client_id": self._client_id
            }
            
            await self._socket.send_multipart([
                b"auth",
                json.dumps(auth_request).encode()
            ])
            
            multipart = await asyncio.wait_for(
                self._socket.recv_multipart(),
                timeout=self._timeout
            )
            
            message_type = multipart[0] if len(multipart) >= 1 else b""
            
            if message_type == b"auth_ack":
                response_data = multipart[-1].decode()
                response = json.loads(response_data)
                
                if response.get("type") == "reply":
                    self._authenticated = True
                    self._logger.info("客户端认证成功")
                    return True
                elif response.get("type") == "error":
                    self._logger.error(f"认证失败: {response.get('error')}")
                    return False
            elif message_type == b"error":
                error_msg = multipart[-1].decode() if len(multipart) > 1 else "Unknown error"
                self._logger.error(f"认证错误: {error_msg}")
                return False
                
            return False
                
        except asyncio.TimeoutError:
            self._logger.error("认证超时")
            return False
        except Exception as e:
            self._logger.error(f"认证过程中发生错误: {e}")
            return False

    async def close(self):
        """关闭连接"""
        self._available_methods = {}
        if self._socket:
            self._socket.close()
            self._socket = None
        self._connected = False
        self._authenticated = False

    async def __aenter__(self):
        """实现异步上下文管理器入口"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """实现异步上下文管理器出口"""
        await self.close()

    async def discover_services(self, timeout: Optional[float] = None) -> Dict[str, Dict]:
        """发现可用的服务方法"""
        return await self.invoke("methods", timeout=timeout)

    async def discover_clusters(self, timeout: Optional[float] = None) -> Dict[str, Dict]:
        """发现可用的服务节点"""
        return await self.invoke("clusters", timeout=timeout)

    async def get_queue_status(self, timeout: Optional[float] = None) -> Dict[str, Dict]:
        """获取当前所有方法的队列状态
        
        返回值是一个字典，每个方法名作为键，值包含:
        - queue_length: 队列中等待处理的请求数
        - available_services: 可用且空闲的服务实例数
        - busy_services: 可用但正忙的服务实例数
        """
        if timeout is None:
            timeout = self._timeout

        if not self._connected:
            await self.connect()
        
        try:
            self._logger.info("发送queue_status请求")
            await self._socket.send_multipart([
                b"queue_status",
                b""
            ])

            multipart = await asyncio.wait_for(
                self._socket.recv_multipart(),
                timeout=timeout
            )
            
            self._logger.info(f"原始响应 (multipart长度={len(multipart)}): {multipart}")

            response_data = multipart[-1].decode()
            self._logger.info(f"响应文本: {response_data}")
            
            try:
                response = json.loads(response_data)
                self._logger.info(f"解析后的响应: {response}")
            except json.JSONDecodeError as e:
                self._logger.error(f"JSON解析错误: {e}, 原始数据: {response_data}")
                return {}
                
            if response.get("type") == "reply":
                return response.get("result", {})
            elif response.get("type") == "error":
                raise RuntimeError(response.get("error"))
            else:
                raise ValueError(f"Unexpected response type: {response.get('type')}")

        except asyncio.TimeoutError:
            raise TimeoutError(f"[{self._router_address}] Get queue status timeout")
        except Exception as e:
            self._logger.error(f"[{self._router_address}] Get queue status error: {e}")
            raise

    async def get_router_info(self, timeout: Optional[float] = None) -> Dict[str, Any]:
        """获取路由器的配置信息
        
        返回值包含:
        - mode: 路由器模式 (fifo或load_balance)
        - address: 路由器地址
        - heartbeat_timeout: 心跳超时时间
        - active_services: 当前活跃服务数量
        - total_services: 总服务数量
        - queue_stats: 当前各方法队列长度
        """
        if timeout is None:
            timeout = self._timeout

        if not self._connected:
            await self.connect()
        
        try:
            await self._socket.send_multipart([
                b"router_info",
                b""
            ])

            multipart = await asyncio.wait_for(
                self._socket.recv_multipart(),
                timeout=timeout
            )

            response_data = multipart[-1].decode()
            response = json.loads(response_data)
            
            if response.get("type") == "reply":
                return response.get("result", {})
            elif response.get("type") == "error":
                raise RuntimeError(response.get("error"))
            else:
                raise ValueError(f"Unexpected response type: {response.get('type')}")

        except asyncio.TimeoutError:
            raise TimeoutError(f"[{self._router_address}] Get router info timeout")
        except Exception as e:
            self._logger.error(f"[{self._router_address}] Get router info error: {e}")
            raise

    async def invoke(self, method: str, *args, timeout: Optional[float] = None, **kwargs) -> Dict[str, Dict]:
        """直接返回结果的调用
        内部方法不会包含分组所需的间隔句点。
        """
        if "." in method:
            results = []
            async for chunk in self._service_stream(method, *args, timeout=timeout, **kwargs):
                results.append(chunk)
            return results
        else:
            self._logger.info(f"Invoke method: {method}")
            return await self._inner_invoke(method, timeout)
    
    async def stream(self, method: str, *args, timeout: Optional[float] = None, **kwargs) -> AsyncGenerator[Any, None]:
        """返回异步生成器"""
        if "." in method:
            async for chunk in self._service_stream(method, *args, timeout=timeout, **kwargs):
                yield chunk
        else:
            result = await self._inner_invoke(method, timeout)
            yield result

    async def _inner_invoke(self, method: str, timeout: Optional[float] = None) -> Dict[str, Dict]:
        """直接内部服务调用"""
        if timeout is None:
            timeout = self._timeout

        if not self._connected:
            await self.connect()
        
        try:
            await self._socket.send_multipart([
                method.encode(),
                b""
            ])

            multipart = await asyncio.wait_for(
                self._socket.recv_multipart(),
                timeout=timeout
            )

            response_data = multipart[-1].decode()
            response = json.loads(response_data)
            self._logger.info(f"Received invoke method response: {response}")

            if response.get("type") == "reply":
                self._available_methods = response.get("result", {})
                return self._available_methods
            elif response.get("type") == "error":
                raise RuntimeError(response.get("error"))
            else:
                raise ValueError(f"Unexpected response type: {response.get('type')}")

        except asyncio.TimeoutError:
            raise TimeoutError(f"[{self._router_address}] Invoke '{method}' timeout")
        except Exception as e:
            self._logger.error(f"[{self._router_address}] Invoke '{method}' error: {e}")
            raise

    async def _service_stream(
        self,
        method: str,
        *args,
        timeout: Optional[float] = None,
        **kwargs
    ) -> AsyncGenerator[Any, None]:
        """调用 DEALER 服务，返回异步生成器"""
        if not self._connected:
            await self.connect()

        if method not in self._available_methods:
            # 如果方法不在缓存中，尝试更新一次
            await self.discover_services()
            if method not in self._available_methods:
                raise RuntimeError(
                    f"[{self._router_address}] Streaming method '{method}' not found. "
                    f"[{self._router_address}] Available methods: {list(self._available_methods.keys())}"
                )

        request_id = str(uuid.uuid4())
        
        # 处理参数，需要确保所有参数都是可序列化的
        serializable_args = []
        for arg in args:
            # 只保留基本类型，复杂对象需要进行特殊处理
            serializable_args.append(arg)
        
        serializable_kwargs = {}
        for key, value in kwargs.items():
            # 只保留基本类型，复杂对象需要进行特殊处理
            serializable_kwargs[key] = value
        
        request = {
            "type": "request",
            "request_id": request_id,
            "func_name": method,
            "request_step": "READY",
            "args": serializable_args,
            "kwargs": serializable_kwargs
        }

        self._logger.debug(f"Request payload: {request}")
        
        if timeout is None:
            timeout = self._timeout

        try:
            # 将请求转换为JSON
            json_request = json.dumps(request)
            
            # 发送请求
            await self._socket.send_multipart([
                b"call_from_client",  # 添加消息类型
                method.encode(),  # 服务名称
                json_request.encode()  # 请求数据
            ])

            # 接收响应流
            while True:
                try:
                    multipart = await asyncio.wait_for(
                        self._socket.recv_multipart(),
                        timeout=timeout
                    )

                    response_data = multipart[-1].decode()
                    response = json.loads(response_data)
                    self._logger.debug(f"Received response: {response}")

                    # 根据消息类型处理响应
                    msg_type = response.get("type")
                    
                    if msg_type == "streaming":
                        yield response.get("data")
                    elif msg_type == "end":
                        return
                    elif msg_type == "reply":
                        yield response.get("result")
                        return
                    elif msg_type == "error":
                        raise RuntimeError(response.get("error"))
                    else:
                        yield response

                except asyncio.TimeoutError:
                    raise TimeoutError(f"[{self._router_address}] Streaming '{method}' timeout")

        except Exception as e:
            self._logger.error(f"[{self._router_address}] Streaming '{method}' error: {e}")
            raise
