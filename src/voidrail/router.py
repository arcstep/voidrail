from typing import Dict, Any, List, Optional, Union, Set, Deque
from pydantic import BaseModel, Field
from enum import Enum
from time import time
from collections import defaultdict, deque
import os

import zmq
import zmq.asyncio
import asyncio
import logging
import json
import uuid

class RouterMode(str, Enum):
    """路由器模式枚举"""
    LOAD_BALANCE = "load_balance"  # 默认负载均衡模式
    FIFO = "fifo"                  # 先进先出模式

class ServiceState(str, Enum):
    """服务状态枚举"""
    ACTIVE = "active"       # 正常运行
    OVERLOAD = "overload"   # 接近满载，不再接受新请求
    INACTIVE = "inactive"   # 无响应/超时
    SHUTDOWN = "shutdown"   # 主动下线

class RouterState(str, Enum):
    """ROUTER状态枚举"""
    INIT = "init"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"

class ServiceInfo(BaseModel):
    """服务信息模型"""
    service_id: str
    group: str = Field(default="default")
    methods: Dict[str, Any]
    state: ServiceState = ServiceState.ACTIVE
    max_concurrent: int = 100
    current_load: int = 0
    request_count: int = 0
    reply_count: int = 0
    last_heartbeat: float = Field(default_factory=time)

    @property
    def load_ratio(self) -> float:
        """负载率"""
        return self.current_load / self.max_concurrent

    def accept_request(self):
        """接受请求"""
        self.current_load += 1
        self.request_count += 1

    def complete_request(self):
        """完成请求"""
        self.current_load -= 1
        self.reply_count += 1

        if self.current_load < 0:
            self.current_load = 0

    def model_dump(self, **kwargs) -> dict:
        """自定义序列化方法"""
        data = super().model_dump(**kwargs)
        data['state'] = data['state'].value  # 将枚举转换为字符串
        return data

class ServiceRouter:
    """ZMQ ROUTER 实现，负责消息路由和服务发现"""
    def __init__(
        self, 
        address: str, 
        context: Optional[zmq.asyncio.Context] = None,
        heartbeat_timeout: float = 30.0,     # 心跳超时时间（秒）
        router_mode: RouterMode = RouterMode.FIFO,  # 默认为FIFO模式
        hwm: int = 1000,
        require_auth: bool = None,           # 是否要求认证
        dealer_api_keys: List[str] = None,   # DEALER 端 API 密钥列表
        client_api_keys: List[str] = None,   # CLIENT 端 API 密钥列表
        logger_level: int = logging.INFO,
    ):
        self._context = context or zmq.asyncio.Context()
        self._address = address
        self._socket = self._context.socket(zmq.ROUTER)
        self._socket.set_hwm(hwm)  # 设置高水位标记
        self._socket.bind(self._address)
        self._running = False
        self._services: Dict[str, ServiceInfo] = {}
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logger_level)
        
        # 可配置的超时参数
        self._HEARTBEAT_TIMEOUT = heartbeat_timeout
        
        # 状态管理
        self._state = RouterState.INIT
        self._state_lock = asyncio.Lock()  # 状态锁
        self._reconnect_in_progress = False

        # 心跳日志控制
        self._service_lock = asyncio.Lock()  # 服务状态修改锁
        self._last_heartbeat_logs = {}  # 记录每个服务上次的心跳日志状态
        self._last_health_check = time()     # 最后检查时间戳

        # 消息处理任务
        self._message_task = None

        # 服务健康检查任务
        self._service_health_check_task = None
        
        # 路由模式
        self._router_mode = router_mode
        
        # FIFO模式相关
        self._method_queues = defaultdict(deque)  # 为每个方法创建请求队列
        self._dealer_processing = defaultdict(int)  # 记录每个DEALER端当前处理的请求数

        # API密钥认证配置
        # 首先检查环境变量中是否要求认证
        env_require_auth = os.environ.get("VOIDRAIL_REQUIRE_AUTH", "").lower()
        self._require_auth = require_auth if require_auth is not None else (env_require_auth in ('1', 'true', 'yes'))
        
        # 加载DEALER端密钥
        self._dealer_api_keys = dealer_api_keys or []
        env_dealer_keys = os.environ.get("VOIDRAIL_DEALER_API_KEYS", "")
        if env_dealer_keys:
            self._dealer_api_keys.extend([k.strip() for k in env_dealer_keys.split(",") if k.strip()])
        
        # 加载CLIENT端密钥
        self._client_api_keys = client_api_keys or []
        env_client_keys = os.environ.get("VOIDRAIL_CLIENT_API_KEYS", "")
        if env_client_keys:
            self._client_api_keys.extend([k.strip() for k in env_client_keys.split(",") if k.strip()])
        
        # 记录已认证的客户端ID
        self._authenticated_clients = set()
        
        if self._require_auth:
            self._logger.info(f"API密钥认证已启用 (dealer_keys={len(self._dealer_api_keys)}, client_keys={len(self._client_api_keys)})")
        else:
            self._logger.info("API密钥认证未启用")

    async def _force_reconnect(self):
        """强制完全重置连接"""
        self._logger.info("Initiating forced reconnection...")
        
        # 重新初始化socket
        self._socket = self._context.socket(zmq.ROUTER)
        self._socket.setsockopt(zmq.LINGER, 0)  # 设置无等待关闭
        self._socket.setsockopt(zmq.IMMEDIATE, 1)  # 禁用缓冲
        self._socket.bind(self._address)
        
        # 重置心跳状态
        self._last_heartbeat_logs = {}  # 记录每个服务上次的心跳日志状态
        self._last_health_check = time()     # 最后检查时间戳

    async def _reconnect(self):
        """尝试重新连接到路由器"""
        self._logger.info(f"开始执行重连...")
        
        try:
            # 关闭现有连接
            if self._socket and not self._socket.closed:
                self._socket.close()
            
            await self._force_reconnect()

            # 重连状态
            self._reconnect_in_progress = False
            self._logger.info(f"重连成功")
            return True
            
        except Exception as e:
            self._logger.error(f"重连过程中发生错误: {e}", exc_info=True)            
            return False

    async def start(self):
        """启动路由器"""
        async with self._state_lock:
            if self._state not in [RouterState.INIT, RouterState.STOPPED]:
                self._logger.warning(f"Cannot start from {self._state} state")
                return False
                
            self._state = RouterState.RUNNING

        # 重建连接
        if not await self._reconnect():
            self._logger.error(f"网络连接失败")
            return False

        self._message_task = asyncio.create_task(self._route_messages(), name="router-route_messages")
        self._service_health_check_task = asyncio.create_task(self._check_service_health(), name="router-check_service_health")
        self._logger.info(f"Router started at {self._address}")

    async def stop(self):
        """停止路由器"""
        async with self._state_lock:
            if self._state == RouterState.STOPPED:
                return
                
            self._state = RouterState.STOPPING

        tasks = []
        if self._message_task:
            self._message_task.cancel()
            tasks.append(self._message_task)
        if self._service_health_check_task:
            self._service_health_check_task.cancel()
            tasks.append(self._service_health_check_task)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            
        self._socket.close(linger=0)
        self._socket = None
            
        async with self._state_lock:
            self._state = RouterState.STOPPED
            self._logger.info(f"Router stopped")

    def _verify_dealer_api_key(self, api_key: str) -> bool:
        """验证DEALER端API密钥是否有效"""
        if not self._require_auth:
            return True
        return api_key in self._dealer_api_keys

    def _verify_client_api_key(self, api_key: str) -> bool:
        """验证CLIENT端API密钥是否有效"""
        if not self._require_auth:
            return True
        return api_key in self._client_api_keys

    def _check_client_auth(self, client_id: str) -> bool:
        """检查客户端是否已认证"""
        if not self._require_auth:
            return True
        return client_id in self._authenticated_clients

    def register_service(self, service_id: str, service_info: Dict[str, Any]):
        """注册服务"""
        # 验证API密钥
        api_key = service_info.get('api_key', '')
        if self._require_auth and not self._verify_dealer_api_key(api_key):
            self._logger.warning(f"服务认证失败: {service_id}, 提供的API密钥无效")
            return False
            
        max_concurrent = service_info.get('max_concurrent', 100)  # 默认最大并发数
        # 保留所有方法信息，不仅仅是metadata
        methods = {
            f"{service_info.get('group', 'default')}.{name}": info 
            for name, info in service_info.get('methods', {}).items()
        }
        self._services[service_id] = ServiceInfo(
            service_id=service_id,
            group=service_info.get('group', 'default'),
            methods=methods,
            max_concurrent=max_concurrent,
            current_load=service_info.get('current_load', 0),
            request_count=service_info.get('request_count', 0),
            reply_count=service_info.get('reply_count', 0)
        )
        self._logger.info(f"Registered service: {service_id} with max_concurrent={max_concurrent}: {methods}")
        return True

    def unregister_service(self, service_id: str):
        """注销服务"""
        if service_id in self._services:
            del self._services[service_id]
            self._logger.info(f"Unregistered service: {service_id}")

    async def _send_error(self, from_id: bytes, error: str):
        """发送错误消息"""
        error_response = {
            "type": "error",
            "request_id": str(uuid.uuid4()),
            "error": error,
            "state": "error"
        }
        await self._socket.send_multipart([
            from_id,
            b"error",
            json.dumps(error_response).encode()
        ])
        self._logger.error(f"Error sending to {from_id}: {error}")

    async def _route_messages(self):
        """消息路由主循环
        通信协议要求：
        1. identiy_id 必须为 UTF-8 编码的字符串
        2. 统一使用 multipart 格式
            - multipart[0] 为 identiy_id
            - multipart[1] 为消息类型
            - multipart[2:] 根据消息类型各自约定
        """
        self._logger.info(f"Routing messages handler started on {self._address}, {self._socket}")
        while self._state == RouterState.RUNNING:
            try:
                multipart = await self._socket.recv_multipart()
                if len(multipart) < 2:
                    self._logger.warning(f"Received invalid message format: {multipart}")
                    continue
                
                from_id_bytes = multipart[0]  # 消息来源ID (bytes)
                from_id = from_id_bytes.decode()  # 消息来源ID (str)
                message_type_bytes = multipart[1]  # 消息类型 (bytes)
                message_type = message_type_bytes.decode()  # 消息类型 (str)
                
                # 处理客户端认证请求
                if message_type == "auth":
                    if len(multipart) < 3:
                        await self._send_error(from_id_bytes, "Invalid auth format")
                        continue
                        
                    auth_data = json.loads(multipart[2].decode())
                    api_key = auth_data.get("api_key", "")
                    
                    if self._verify_client_api_key(api_key):
                        self._authenticated_clients.add(from_id)
                        response = {
                            "type": "reply",
                            "request_id": str(uuid.uuid4()),
                            "result": {"status": "authenticated"}
                        }
                        await self._socket.send_multipart([
                            from_id_bytes,
                            b"auth_ack",
                            json.dumps(response).encode()
                        ])
                        self._logger.info(f"Client 认证成功: {from_id}")
                    else:
                        await self._send_error(from_id_bytes, "Authentication failed")
                        self._logger.warning(f"Client 认证失败: {from_id}")
                    continue
                
                # 将锁的范围缩小到关键部分
                async with self._service_lock:
                    # 更新心跳时间（所有消息类型）
                    if from_id in self._services.keys():
                        service = self._services[from_id]
                        if service.state == ServiceState.INACTIVE:
                            service.state = ServiceState.ACTIVE
                            self._logger.info(f"Service {from_id} reactivated after receiving message of type {message_type}")
                        service.last_heartbeat = time()

                # 然后再处理特定消息类型
                if message_type == "router_monitor":
                    await self._socket.send_multipart([
                        from_id_bytes,
                        b"heartbeat_ack",
                        b""
                    ])

                elif message_type == "register":
                    if len(multipart) < 3:
                        await self._send_error(from_id_bytes, "Invalid register format")
                        continue
                        
                    async with self._service_lock:  # 单独加锁
                        service_info = json.loads(multipart[2].decode())
                        registration_success = self.register_service(from_id, service_info)
                        
                        if registration_success:
                            # 只在首次注册时设置ACTIVE状态
                            if from_id not in self._services:
                                self._services[from_id].state = ServiceState.ACTIVE
                                
                            await self._socket.send_multipart([
                                from_id_bytes,
                                b"register_ack",
                                b""
                            ])
                        else:
                            await self._send_error(from_id_bytes, "Registration failed: invalid API key")
                    
                elif message_type == "heartbeat":
                    # 处理心跳消息
                    heartbeat_data = {}
                    if len(multipart) >= 3:
                        try:
                            heartbeat_data = json.loads(multipart[2].decode())
                        except:
                            pass
                    
                    api_key = heartbeat_data.get("api_key", "")
                    
                    # 如果需要认证，则验证API密钥
                    if self._require_auth and from_id not in self._services and not self._verify_dealer_api_key(api_key):
                        await self._send_error(from_id_bytes, "Heartbeat authentication failed")
                        continue
                    
                    if from_id in self._services.keys():
                        # 发送心跳确认消息 (已在上面更新了状态)
                        await self._socket.send_multipart([
                            from_id_bytes,  # 目标服务的ID
                            b"heartbeat_ack",  # 心跳确认类型
                            b""  # 空负载
                        ])
                    else:
                        # 未注册的服务发送心跳
                        self._logger.warning(f"Received heartbeat from unregistered service: {from_id}")
                
                elif message_type == "clusters":
                    # 客户端认证检查
                    if self._require_auth and not self._check_client_auth(from_id):
                        await self._send_error(from_id_bytes, "Not authenticated")
                        continue
                        
                    # 收集所有可用的 DEALERS 节点信息
                    response = {
                        "type": "reply",
                        "request_id": str(uuid.uuid4()),
                        "result": {
                            k: v.model_dump() for k, v in self._services.items()
                        }
                    }
                    await self._socket.send_multipart([
                        from_id_bytes,
                        b"clusters_ack",
                        json.dumps(response).encode()
                    ])
                    
                elif message_type == "methods":
                    # 客户端认证检查
                    if self._require_auth and not self._check_client_auth(from_id):
                        await self._send_error(from_id_bytes, "Not authenticated")
                        continue
                        
                    # 收集所有可用的方法信息
                    available_methods = {}
                    for service in self._services.values():
                        if service.state == ServiceState.ACTIVE:
                            for method_name, method_info in service.methods.items():
                                if method_name not in available_methods:
                                    available_methods[method_name] = method_info
                    
                    self._logger.info(f"Handling discovery request, available methods: {list(available_methods.keys())}")
                    response = {
                        "type": "reply",
                        "request_id": str(uuid.uuid4()),
                        "result": available_methods
                    }
                    await self._socket.send_multipart([
                        from_id_bytes,
                        b"methods_ack",
                        json.dumps(response).encode()
                    ])
                    
                elif message_type == "call_from_client":
                    # 客户端认证检查
                    if self._require_auth and not self._check_client_auth(from_id):
                        await self._send_error(from_id_bytes, "Not authenticated")
                        continue
                        
                    if len(multipart) < 3:
                        self._logger.error(f"Invalid call message format")
                        continue
                        
                    service_name = multipart[2].decode()
                    
                    # FIFO模式: 将请求加入队列并尝试处理
                    if self._router_mode == RouterMode.FIFO:
                        # 将请求加入队列
                        queue_item = {
                            'from_id_bytes': from_id_bytes,
                            'multipart': multipart[2:],
                        }
                        self._method_queues[service_name].append(queue_item)
                        self._logger.info(f"FIFO: 已将请求加入 {service_name} 队列，当前长度: {len(self._method_queues[service_name])}")
                        
                        # 尝试处理队列中的请求
                        await self._process_fifo_queue(service_name)
                    else:
                        # 原有负载均衡模式
                        target_service = self._select_best_service(service_name)
                        if target_service and target_service.state == ServiceState.ACTIVE:
                            target_service.accept_request()
                            self._services[target_service.service_id].accept_request()
                            service_dealer_id = target_service.service_id.encode()
                            await self._socket.send_multipart([
                                service_dealer_id,
                                b"call_from_router",
                                from_id_bytes,
                                *multipart[2:]
                            ])
                        else:
                            error_msg = f"No available service for method {service_name}"
                            self._logger.error(f"{error_msg}")
                            error = {
                                "type": "error",
                                "request_id": str(uuid.uuid4()),
                                "error": error_msg
                            }
                            await self._socket.send_multipart([
                                from_id_bytes,
                                b"reply_from_router",
                                json.dumps(error).encode()
                            ])

                elif message_type in ["overload", "resume", "shutdown"]:
                    # 处理服务状态变更消息
                    if from_id in self._services:
                        if message_type == "shutdown":
                            self._services[from_id].state = ServiceState.SHUTDOWN
                            await self._socket.send_multipart([
                                from_id_bytes,
                                b"shutdown_ack",
                            ])
                        elif message_type == "overload":
                            # 不必回复
                            self._services[from_id].state = ServiceState.OVERLOAD
                        elif message_type == "resume":
                            # 不必回复
                            self._services[from_id].state = ServiceState.ACTIVE

                # 如果是已注册服务的回复消息，直接转发给客户端
                elif from_id in self._services and message_type == "reply_from_dealer":
                    if len(multipart) < 3:
                        await self._send_error(from_id_bytes, "Invalid reply format")
                        continue
                        
                    target_client_id = multipart[2]  # 目标客户端ID
                    response_data = multipart[3] if len(multipart) > 3 else b""
                    
                    # 直接转发响应给客户端
                    await self._socket.send_multipart([
                        target_client_id,
                        b"reply_from_router",
                        response_data
                    ])
                    
                    # 更新服务状态
                    self._services[from_id].complete_request()
                    
                    # FIFO模式: 处理完成后更新状态并尝试处理下一个请求
                    if self._router_mode == RouterMode.FIFO:
                        # 减少处理计数
                        self._dealer_processing[from_id] -= 1
                        if self._dealer_processing[from_id] < 0:
                            self._dealer_processing[from_id] = 0
                            
                        # 处理所有队列中的请求
                        for method_name in self._method_queues.keys():
                            if method_name in self._services[from_id].methods and len(self._method_queues[method_name]) > 0:
                                await self._process_fifo_queue(method_name)

                elif message_type == "queue_status":
                    # 客户端认证检查
                    if self._require_auth and not self._check_client_auth(from_id):
                        await self._send_error(from_id_bytes, "Not authenticated")
                        continue
                        
                    # 收集队列状态信息
                    queue_stats = {}
                    for method_name, queue in self._method_queues.items():
                        queue_stats[method_name] = {
                            "queue_length": len(queue),
                            "available_services": len([
                                s for s in self._services.values()
                                if method_name in s.methods and 
                                s.state == ServiceState.ACTIVE and
                                self._dealer_processing.get(s.service_id, 0) == 0
                            ]),
                            "busy_services": len([
                                s for s in self._services.values()
                                if method_name in s.methods and 
                                s.state == ServiceState.ACTIVE and
                                self._dealer_processing.get(s.service_id, 0) > 0
                            ])
                        }
                    
                    self._logger.info(f"Handling queue_status request, response: {queue_stats}")
                    
                    response = {
                        "type": "reply",
                        "request_id": str(uuid.uuid4()),
                        "result": queue_stats
                    }
                    
                    await self._socket.send_multipart([
                        from_id_bytes,
                        b"queue_status_ack",  # 消息类型标识
                        json.dumps(response).encode()
                    ])

                elif message_type == "router_info":
                    # 客户端认证检查
                    if self._require_auth and not self._check_client_auth(from_id):
                        await self._send_error(from_id_bytes, "Not authenticated")
                        continue
                        
                    # 提供路由器的配置信息
                    router_info = {
                        "mode": self._router_mode.value,  # FIFO或LOAD_BALANCE
                        "address": self._address,
                        "heartbeat_timeout": self._HEARTBEAT_TIMEOUT,
                        "active_services": len([s for s in self._services.values() if s.state == ServiceState.ACTIVE]),
                        "total_services": len(self._services),
                        "queue_stats": {
                            method: len(queue) for method, queue in self._method_queues.items()
                        },
                        "auth_required": self._require_auth
                    }
                    
                    response = {
                        "type": "reply",
                        "request_id": str(uuid.uuid4()),
                        "result": router_info
                    }
                    
                    await self._socket.send_multipart([
                        from_id_bytes,
                        b"router_info_ack",
                        json.dumps(response).encode()
                    ])

                else:
                    await self._send_error(from_id_bytes, f"Unknown message type: {message_type}")

            except Exception as e:
                self._logger.error(f"Router error: {e}", exc_info=True)
                try:
                    await self._send_error(from_id_bytes, f"Service Router Error")
                except:
                    pass

    async def _process_fifo_queue(self, method_name: str):
        """处理FIFO模式下的请求队列"""
        # 如果队列为空则不处理
        if not self._method_queues[method_name]:
            return
            
        # 尝试处理队列中的所有请求，直到没有可用的服务或队列为空
        while self._method_queues[method_name]:
            # 获取一个空闲的服务
            target_service = self._select_best_service_fifo(method_name)
            if not target_service:
                # 没有可用服务，等待下次有服务完成任务后再处理
                self._logger.info(f"FIFO: 没有空闲的DEALER处理 {method_name} 队列中的请求，队列长度: {len(self._method_queues[method_name])}")
                break
                
            # 弹出队列中的第一个请求
            queue_item = self._method_queues[method_name].popleft()
            from_id_bytes = queue_item['from_id_bytes']
            service_dealer_id = target_service.service_id.encode()
            
            # 记录处理状态 - 增加DEALER处理计数
            self._dealer_processing[target_service.service_id] += 1
            
            # 更新服务状态
            target_service.accept_request()
            self._services[target_service.service_id].accept_request()
            
            # 转发请求
            await self._socket.send_multipart([
                service_dealer_id,
                b"call_from_router",
                from_id_bytes,
                *queue_item['multipart']
            ])
            
            self._logger.info(f"FIFO: 已将 {method_name} 请求分配给 {target_service.service_id}，"
                            f"队列剩余: {len(self._method_queues[method_name])}，"
                            f"DEALER当前处理数: {self._dealer_processing[target_service.service_id]}")

    def _select_best_service_fifo(self, method_name: str) -> Optional[ServiceInfo]:
        """FIFO模式下选择最佳服务实例 - 优先选择空闲的服务"""
        available_services = [
            service for service in self._services.values()
            if (method_name in service.methods and 
                service.state == ServiceState.ACTIVE and
                # 在FIFO模式下，只有当前没有处理任务的服务才能被选中
                self._dealer_processing.get(service.service_id, 0) == 0)
        ]
        
        if not available_services:
            return None
            
        # 随机选择一个可用服务，防止总是选同一个
        return available_services[0]

    def _select_best_service(self, method_name: str) -> Optional[ServiceInfo]:
        """选择最佳服务实例"""
        # 根据路由模式选择不同的策略
        if self._router_mode == RouterMode.FIFO:
            return self._select_best_service_fifo(method_name)
        
        # 原有负载均衡模式
        available_services = [
            service for service in self._services.values()
            if (method_name in service.methods and 
                service.state == ServiceState.ACTIVE and
                service.current_load < service.max_concurrent)
        ]
        for service in available_services:
            s = self._services[service.service_id]
            self._logger.info(f"Available service for [{method_name}]: {s.service_id} current_load: {s.current_load} / max_concurrent: {s.max_concurrent}")
        
        if not available_services:
            return None
            
        # 选择负载最小的服务
        return min(available_services, key=lambda s: s.current_load)

    async def _check_service_health(self):
        """检查服务健康状态"""
        self._logger.info(f"Dealer service health check handler started")
        while self._state == RouterState.RUNNING:
            current_time = time()
            
            # 检查服务心跳
            for service_id, service in list(self._services.items()):
                if service.state != ServiceState.SHUTDOWN:  # 不检查已主动下线的服务
                    if current_time - service.last_heartbeat > self._HEARTBEAT_TIMEOUT:
                        if service.state != ServiceState.INACTIVE:
                            service.state = ServiceState.INACTIVE
                            # 服务变为不可用时记录日志
                            self._logger.warning(
                                f"Service {service_id} marked as inactive: "
                                f"last heartbeat was {current_time - service.last_heartbeat:.1f}s ago"
                            )
                            service.current_load = 0
                    else:
                        self._logger.debug(f"Service {service_id} is living!")
            
            await asyncio.sleep(self._HEARTBEAT_TIMEOUT)
