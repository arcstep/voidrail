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
import zmq.auth

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
    current_load: int = 0
    request_count: int = 0
    reply_count: int = 0
    last_heartbeat: float = Field(default_factory=time)

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
    """ZMQ ROUTER 实现，负责消息路由和服务发现
    仅使用FIFO模式，移除负载均衡模式"""
    def __init__(
        self, 
        address: str, 
        heartbeat_interval: float = 1.0,  # 用户唯一需配置的心跳"检查"周期
        hwm: int = 1000,
        dealer_api_keys: List[str] = None,     # DEALER 端 API 密钥列表
        client_api_keys: List[str] = None,     # CLIENT 端 API 密钥列表
        curve_server_key_file: str = None,  # 保留此参数
        logger: logging.Logger = None,
    ):
        self._context = zmq.asyncio.Context()
        self._address = address
        self._socket = self._context.socket(zmq.ROUTER)
        self._socket.set_hwm(hwm)  # 设置高水位标记
        self._socket.bind(self._address)
        self._running = False
        self._services: Dict[str, ServiceInfo] = {}
        self._logger = logger or logging.getLogger(__name__)
        
        # 基准心跳检查周期
        I = heartbeat_interval

        # 空闲服务判定超时 = I + 3
        T_idle = I + 3
        # 心跳健康检查间隔 = I
        check_interval = I

        # 忙碌服务可以设更宽松
        T_busy = T_idle * 60

        self._IDLE_HEARTBEAT_TIMEOUT = T_idle
        self._IDLE_HEARTBEAT_CHECK   = check_interval
        self._BUSY_HEARTBEAT_TIMEOUT = T_busy
        
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
        
        # FIFO模式相关
        self._method_queues = defaultdict(deque)    # 为每个方法创建请求队列
        self._dealer_processing = defaultdict(int)  # 记录每个DEALER端当前处理的请求数

        # API密钥认证配置 - 分开处理 Dealer 和 Client
        self._dealer_api_keys = dealer_api_keys or []
        env_dealer_keys = os.environ.get("VOIDRAIL_DEALER_API_KEYS", "")
        if env_dealer_keys:
            self._dealer_api_keys.extend([k.strip() for k in env_dealer_keys.split(",") if k.strip()])

        self._client_api_keys = client_api_keys or []
        env_client_keys = os.environ.get("VOIDRAIL_CLIENT_API_KEYS", "")
        if env_client_keys:
            self._client_api_keys.extend([k.strip() for k in env_client_keys.split(",") if k.strip()])

        # 记录已认证的客户端ID
        self._authenticated_clients = set()

        # 更新日志，分别说明两种认证状态
        if self._dealer_api_keys:
             self._logger.info(f"DEALER API密钥认证已启用 (keys={len(self._dealer_api_keys)})")
        else:
             self._logger.info("DEALER API密钥认证未启用")

        if self._client_api_keys:
             self._logger.info(f"CLIENT API密钥认证已启用 (keys={len(self._client_api_keys)})")
        else:
             self._logger.info("CLIENT API密钥认证未启用")

        # 统计信息
        self._total_request_count = 0
        self._total_response_count = 0
        self._start_time = time()
        
        # 源地址跟踪
        self._service_sources = {}
        
        # 最后一次服务清理时间
        self._last_service_cleanup = time()

        # 记录DEALER最后一次报告忙碌的时间
        self._service_busy_since = {}  # service_id -> timestamp
        
        # 服务重连监控
        self._dealer_reconnect_attempts = {}  # 记录服务重连尝试次数

        # 新增：连续失败计数阈值
        self._CONSECUTIVE_FAILURES_THRESHOLD = 2

        # 新增: 存储加载的密钥
        self._curve_server_public = None
        self._curve_server_secret = None

        # 自动证书管理
        self._curve_enabled = bool(curve_server_key_file)
        if self._curve_enabled:
            if not os.path.exists(curve_server_key_file):
                raise ValueError(f"CURVE密钥文件不存在: {curve_server_key_file}")

            # 加载服务器密钥并存储
            try:
                self._curve_server_public, self._curve_server_secret = zmq.auth.load_certificate(curve_server_key_file)
                self._logger.info(f"CURVE服务器密钥已加载自: {curve_server_key_file}")
            except Exception as e:
                self._logger.error(f"加载CURVE密钥失败: {e}", exc_info=True)
                raise

            # 在初始 socket 上应用 CURVE 配置
            self._apply_curve_config(self._socket)

            # 添加这些详细的调试日志
            try:
                # 测试验证器是否正常工作的简单方法
                test_client_public, test_client_secret = zmq.curve_keypair()
                self._logger.debug(f"CURVE测试: 生成临时客户端密钥对成功")
                self._logger.debug(f"CURVE socket 配置完成: server_public={self._curve_server_public.hex()[:16]}")
                
                # 保存重要的状态信息供诊断
                self.curve_enabled = True
                self.curve_server_public = self._curve_server_public
            except Exception as e:
                self._logger.error(f"CURVE配置诊断失败: {e}")

    # 新增: 辅助方法来应用CURVE配置
    def _apply_curve_config(self, socket_instance):
        """将CURVE配置应用到给定的套接字实例"""
        if self._curve_enabled and self._curve_server_public and self._curve_server_secret:
            try:
                socket_instance.curve_secretkey = self._curve_server_secret
                socket_instance.curve_publickey = self._curve_server_public
                socket_instance.curve_server = True
                self._logger.debug(f"成功将 CURVE 配置应用到套接字 {socket_instance}")
            except Exception as e:
                self._logger.error(f"将 CURVE 配置应用到套接字失败: {e}", exc_info=True)
        elif self._curve_enabled:
            self._logger.error("尝试应用 CURVE 配置，但密钥未成功加载")

    async def _force_reconnect(self):
        """强制完全重置连接"""
        self._logger.info("Initiating forced reconnection...")
        
        # 重新初始化socket
        # 确保在创建新 socket 之前旧 socket 已关闭 (虽然 _reconnect 也会做，但这里更保险)
        if self._socket and not self._socket.closed:
            self._socket.close(linger=0)

        self._socket = self._context.socket(zmq.ROUTER)
        self._logger.debug(f"新套接字实例已创建: {self._socket}")
        # 设置选项应该在应用 CURVE 和绑定之前
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.setsockopt(zmq.IMMEDIATE, 1) # 注意: IMMEDIATE 可能影响性能/可靠性，根据需要调整

        # !!! 关键修改: 在新套接字上重新应用 CURVE 配置 !!!
        self._apply_curve_config(self._socket) # <--- 调用辅助方法

        # 绑定新套接字
        try:
            self._socket.bind(self._address)
            self._logger.info(f"新套接字已成功绑定到 {self._address}")
        except zmq.ZMQError as e:
            self._logger.error(f"新套接字绑定失败: {e}", exc_info=True)
            # 绑定失败是严重问题，可能需要停止或重试
            raise # 重新抛出异常

        # 重置心跳状态
        self._last_heartbeat_logs = {}
        self._last_health_check = time()

    async def _reconnect(self):
        """尝试重新连接到路由器"""
        self._logger.info(f"开始执行重连...")
        
        try:
            # 关闭现有连接 (如果存在且未关闭)
            if self._socket and not self._socket.closed:
                self._socket.close(linger=0) # linger=0 避免阻塞
                self._logger.debug("旧套接字已关闭")
            else:
                self._logger.debug("没有需要关闭的旧套接字")

            # 调用强制重连（现在会正确配置新 socket）
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
        
        # 新增：向所有DEALER发送关闭通知
        try:
            for service_id in self._services.keys():
                try:
                    await asyncio.wait_for(
                        self._socket.send_multipart([
                            service_id.encode(),
                            b"router_shutdown",
                            b""
                        ]),
                        timeout=0.2
                    )
                except Exception as e:
                    self._logger.warning(f"通知服务{service_id}关闭失败: {e}")
        except Exception as e:
            self._logger.error(f"发送关闭通知出错: {e}")
        
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
        """验证DEALER端API密钥是否有效 (仅当被调用时检查列表)"""
        return api_key in self._dealer_api_keys

    def _verify_client_api_key(self, api_key: str) -> bool:
        """验证CLIENT端API密钥是否有效 (仅当被调用时检查列表)"""
        return api_key in self._client_api_keys

    def _check_client_auth(self, client_id: str) -> bool:
        """检查客户端是否已认证 (仅当被调用时检查集合)"""
        return client_id in self._authenticated_clients

    def register_service(self, service_id: str, service_info: Dict[str, Any]):
        """注册服务"""
        api_key = service_info.get('api_key', '')
        # 检查 self._dealer_api_keys 是否非空
        if self._dealer_api_keys and not self._verify_dealer_api_key(api_key):
            self._logger.warning(f"服务注册认证失败: {service_id}, 提供的 DEALER API 密钥无效")
            return False

        methods = {
            f"{service_info.get('group', 'default')}.{name}": info
            for name, info in service_info.get('methods', {}).items()
        }

        is_reconnect = service_id in self._services

        if 'remote_addr' in service_info:
            remote_addr = service_info.get('remote_addr')
            host_info = service_info.get('host_info', {})
            
            # 如果地址格式不正确（缺少PID等信息），尝试重新格式化
            if ':' in remote_addr and '[' not in remote_addr:
                ip_part = remote_addr.split(':')[0]
                pid = host_info.get('pid', 'unknown')
                uuid_part = service_id.split('-')[-1] if '-' in service_id else service_id[-8:]
                remote_addr = f"{ip_part} [PID:{pid}, ID:{uuid_part}]"
                service_info['remote_addr'] = remote_addr
            
            self._service_sources[service_id] = remote_addr
            self._logger.debug(f"服务 {service_id} 的远程地址: {remote_addr}")
        
        self._services[service_id] = ServiceInfo(
            service_id=service_id,
            group=service_info.get('group', 'default'),
            methods=methods,
            current_load=service_info.get('current_load', 0),
            request_count=service_info.get('request_count', 0),
            reply_count=service_info.get('reply_count', 0)
        )

        if is_reconnect:
            self._logger.info(f"Reconnected service: {service_id}")
        else:
            self._logger.info(f"Registered new service: {service_id}")

        return True # 添加明确的返回 True

    def unregister_service(self, service_id: str):
        """注销服务"""
        if service_id in self._services:
            # 从活跃服务中移除
            del self._services[service_id]
            
            # 清理相关资源
            if service_id in self._dealer_processing:
                del self._dealer_processing[service_id]
            
            if service_id in self._service_sources:
                del self._service_sources[service_id]
            
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
        """消息路由主循环"""
        self._logger.info(f"路由消息处理器启动于 {self._address}")
        while self._state == RouterState.RUNNING:
            try:
                multipart = await self._socket.recv_multipart()
                if len(multipart) < 2:
                    self._logger.warning(f"收到无效消息格式: {multipart}")
                    continue
                
                from_id_bytes = multipart[0]  # 消息来源ID (bytes)
                from_id = from_id_bytes.decode()  # 消息来源ID (str)
                message_type_bytes = multipart[1]  # 消息类型 (bytes)
                message_type = message_type_bytes.decode()  # 消息类型 (str)
                
                # 处理客户端认证请求
                if message_type == "auth":
                    # 仅当配置了 client_api_keys 时才处理 auth 消息
                    if not self._client_api_keys:
                        self._logger.warning(f"收到来自 {from_id} 的 auth 请求，但未配置客户端 API Keys，忽略。")
                        await self._send_error(from_id_bytes, "No need to authenticate")

                    # API_KEY认证请求数据内容已通过CURVE加密
                    if len(multipart) < 3:
                        await self._send_error(from_id_bytes, "Invalid auth format")
                        continue
                        
                    auth_data = json.loads(multipart[2].decode())
                    api_key = auth_data.get("api_key", "")
                    
                    # API_KEY验证，作为应用层认证
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
                            self._logger.info(f"服务 {from_id} 在收到 {message_type} 类型消息后重新激活")
                        service.last_heartbeat = time()
                        
                        # 重置重连计数
                        if from_id in self._dealer_reconnect_attempts:
                            del self._dealer_reconnect_attempts[from_id]

                # 处理特定消息类型
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
                        
                        # 尝试从zmq获取远程地址信息
                        try:
                            # 这里使用zmq的get_last_endpoint或类似方法获取对等端地址
                            # 如果不可用，设置一个默认值表示"未知来源"
                            if 'remote_addr' not in service_info:
                                service_info['remote_addr'] = f"客户端-{from_id[:8]}"
                        except:
                            pass
                        
                        registration_success = self.register_service(from_id, service_info)
                        
                        if registration_success:
                            await self._socket.send_multipart([
                                from_id_bytes,
                                b"register_ack",
                                b""
                            ])
                        else:
                            await self._send_error(from_id_bytes, "Registration failed: invalid API key or other issue")
                    
                elif message_type == "heartbeat":
                    if from_id in self._services.keys():
                        # 正常处理...
                        heartbeat_data = {}
                        if len(multipart) >= 3:
                            try:
                                heartbeat_data = json.loads(multipart[2].decode())
                            except:
                                pass
                        
                        # 如果心跳数据中包含处理中请求数
                        if from_id in self._services.keys():
                            service = self._services[from_id]
                            
                            # 更新处理负载信息
                            current_load = heartbeat_data.get("processing_requests", 0)
                            if current_load > 0:
                                # 记录忙碌开始时间(如果之前非忙碌)
                                if service.current_load == 0:
                                    self._service_busy_since[from_id] = time()
                                service.current_load = current_load
                            else:
                                # 清除忙碌状态
                                if from_id in self._service_busy_since:
                                    del self._service_busy_since[from_id]
                                service.current_load = 0
                            
                            # 发送心跳确认
                            await self._socket.send_multipart([
                                from_id_bytes,
                                b"heartbeat_ack",
                                b""
                            ])
                        else:
                            # 服务未注册但发送了心跳 - 告知它需要重新注册
                            await self._socket.send_multipart([
                                from_id_bytes,
                                b"reregister_required",
                                b""
                            ])
                            self._logger.warning(f"Received heartbeat from unregistered service: {from_id}, instructed to re-register")
                    else:
                        # 服务未注册但发送了心跳 - 告知它需要重新注册
                        await self._socket.send_multipart([
                            from_id_bytes,
                            b"reregister_required",
                            b""
                        ])
                        self._logger.warning(f"Received heartbeat from unregistered service: {from_id}, instructed to re-register")
                
                elif message_type == "clusters":
                    # 客户端认证检查 - 检查 self._client_api_keys 是否非空
                    if self._client_api_keys and not self._check_client_auth(from_id):
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
                    # 客户端认证检查 - 检查 self._client_api_keys 是否非空
                    if self._client_api_keys and not self._check_client_auth(from_id):
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
                    # 客户端认证检查 - 检查 self._client_api_keys 是否非空
                    if self._client_api_keys and not self._check_client_auth(from_id):
                        await self._send_error(from_id_bytes, "未认证")
                        continue
                        
                    if len(multipart) < 3:
                        self._logger.error(f"无效的调用消息格式")
                        continue
                        
                    service_name = multipart[2].decode()
                    
                    # 更新全局请求计数
                    self._total_request_count += 1
                    
                    # 将请求加入队列
                    queue_item = {
                        'from_id_bytes': from_id_bytes,
                        'multipart': multipart[2:],
                    }
                    self._method_queues[service_name].append(queue_item)
                    self._logger.info(f"FIFO: 已将请求加入 {service_name} 队列，当前长度: {len(self._method_queues[service_name])}")
                    
                    # 尝试处理队列中的请求
                    await self._process_fifo_queue(service_name)

                elif message_type in ["overload", "resume", "shutdown"]:
                    # 处理服务状态变更消息
                    if from_id in self._services:
                        # 完全移除服务而不仅标记状态
                        self.unregister_service(from_id)  
                        await self._socket.send_multipart([from_id_bytes, b"shutdown_ack"])
                        self._logger.info(f"Service {from_id} has been completely unregistered")

                # 如果是已注册服务的回复消息，直接转发给客户端
                elif from_id in self._services and message_type == "reply_from_dealer":
                    if len(multipart) < 3:
                        await self._send_error(from_id_bytes, "无效的回复格式")
                        continue
                        
                    target_client_id = multipart[2]  # 目标客户端ID
                    response_data = multipart[3] if len(multipart) > 3 else b""
                    
                    # 更新全局响应计数
                    self._total_response_count += 1
                    
                    # 直接转发响应给客户端
                    await self._socket.send_multipart([
                        target_client_id,
                        b"reply_from_router",
                        response_data
                    ])
                    
                    # 更新服务状态
                    self._services[from_id].complete_request()
                    
                    # 减少处理计数
                    self._dealer_processing[from_id] -= 1
                    if self._dealer_processing[from_id] < 0:
                        self._dealer_processing[from_id] = 0
                        
                    # 处理所有队列中的请求
                    for method_name in list(self._method_queues.keys()):
                        if len(self._method_queues[method_name]) > 0 and \
                           method_name in self._services[from_id].methods:
                            await self._process_fifo_queue(method_name)

                elif message_type == "queue_status":
                    # 客户端认证检查 - 检查 self._client_api_keys 是否非空
                    if self._client_api_keys and not self._check_client_auth(from_id):
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
                    # 客户端认证检查 - 检查 self._client_api_keys 是否非空
                    if self._client_api_keys and not self._check_client_auth(from_id):
                        await self._send_error(from_id_bytes, "Not authenticated")
                        continue
                        
                    # 服务分组统计 - 按组和来源统计
                    service_by_group = {}
                    for service in self._services.values():
                        if service.state == ServiceState.ACTIVE:
                            group = service.group
                            service_by_group.setdefault(group, {"count": 0, "sources": {}})
                            service_by_group[group]["count"] += 1
                            
                            # 将服务源地址添加到分组统计中
                            source = self._service_sources.get(service.service_id, "未知")
                            service_by_group[group]["sources"].setdefault(source, 0)
                            service_by_group[group]["sources"][source] += 1
                    
                    # 按源地址合并服务信息
                    services_by_source = {}
                    for service_id, source in self._service_sources.items():
                        if service_id in self._services:
                            service = self._services[service_id]
                            
                            # 跳过非活跃服务
                            if service.state != ServiceState.ACTIVE:
                                continue
                                
                            # 使用更有意义的键，包含组名
                            source_key = f"{source} ({service.group})"
                            if source_key not in services_by_source:
                                services_by_source[source_key] = []
                            services_by_source[source_key].append(service_id)
                    
                    # 改进的信息响应
                    router_info = {
                        "mode": "fifo",
                        "address": self._address,
                        "idle_heartbeat_timeout": self._IDLE_HEARTBEAT_TIMEOUT,
                        "busy_heartbeat_timeout": self._BUSY_HEARTBEAT_TIMEOUT,
                        "max_busy_without_heartbeat": self._BUSY_HEARTBEAT_TIMEOUT * 2,
                        "active_services_count": len([s for s in self._services.values() if s.state == ServiceState.ACTIVE]),
                        "busy_services_count": len([s for s in self._services.values() if s.state == ServiceState.ACTIVE and s.current_load > 0]),
                        "idle_services_count": len([s for s in self._services.values() if s.state == ServiceState.ACTIVE and s.current_load == 0]),
                        "inactive_services_count": len([s for s in self._services.values() if s.state == ServiceState.INACTIVE]),
                        "total_services_count": len(self._services),
                        "uptime": int(time() - self._start_time),
                        "total_requests": self._total_request_count,
                        "total_responses": self._total_response_count,
                        "requests_in_process": sum(s.current_load for s in self._services.values() if s.state == ServiceState.ACTIVE),
                        "requests_in_queue": sum(len(queue) for queue in self._method_queues.values()),
                        "service_by_group": service_by_group,
                        "service_sources": services_by_source,
                        "client_api_keys_require": bool(self._client_api_keys),
                        "dealer_api_keys_require": bool(self._dealer_api_keys),
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
                self._logger.error(f"Router错误: {e}", exc_info=True)
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
            target_service = self._select_best_service(method_name)
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

    def _select_best_service(self, method_name: str) -> Optional[ServiceInfo]:
        """选择最佳服务实例 - 只保留FIFO策略"""
        # 严格筛选只选择ACTIVE状态服务
        available_services = [
            service for service in self._services.values()
            if (method_name in service.methods and 
                service.state == ServiceState.ACTIVE and
                self._dealer_processing.get(service.service_id, 0) == 0)
        ]
        
        # 调试日志
        self._logger.debug(f"For method {method_name}: Found {len(available_services)} active services")
        
        if not available_services:
            return None
            
        return min(available_services, key=lambda s: self._dealer_processing.get(s.service_id, 0))

    async def _check_service_health(self):
        """检查服务健康状态 - 加入连续失败计数"""
        self._logger.info(f"服务健康检查处理器启动")
        self._service_heartbeat_failures = defaultdict(int)  # 记录连续心跳失败次数
        
        while self._state == RouterState.RUNNING:
            now = time()
            
            # 检查服务心跳
            for service_id, service in list(self._services.items()):
                if service.state == ServiceState.SHUTDOWN:
                    continue  # 跳过已主动下线的服务
                    
                age = now - service.last_heartbeat
                
                # 根据服务状态确定超时时间
                if service.current_load > 0:
                    # 忙碌服务使用宽松的超时时间
                    timeout = self._BUSY_HEARTBEAT_TIMEOUT
                    # 重置空闲状态下的连续失败计数
                    self._service_heartbeat_failures[service_id] = 0
                else:
                    # 空闲服务使用严格的超时时间(5秒)
                    timeout = self._IDLE_HEARTBEAT_TIMEOUT
                
                # 检查心跳是否超时
                if age > timeout:
                    # 增加连续失败计数
                    self._service_heartbeat_failures[service_id] += 1
                    consecutive_failures = self._service_heartbeat_failures[service_id]
                    
                    # 连续失败次数超过阈值才标记为不活跃
                    if consecutive_failures >= self._CONSECUTIVE_FAILURES_THRESHOLD:
                        if service.state != ServiceState.INACTIVE:
                            service.state = ServiceState.INACTIVE
                            self._logger.warning(
                                f"服务 {service_id} 标记为不活跃: "
                                f"连续 {consecutive_failures} 次心跳超时 "
                                f"(上次心跳: {age:.1f}秒前, 超时阈值: {timeout:.1f}秒)"
                            )
                            service.current_load = 0
                            
                            # 记录重连尝试
                            self._dealer_reconnect_attempts[service_id] = \
                                self._dealer_reconnect_attempts.get(service_id, 0) + 1
                    else:
                        # 未达到连续失败阈值，记录警告但不改变状态
                        self._logger.info(
                            f"服务 {service_id} 心跳延迟 ({age:.1f}秒), "
                            f"连续失败次数: {consecutive_failures}/{self._CONSECUTIVE_FAILURES_THRESHOLD}"
                        )
                else:
                    # 心跳正常，重置失败计数
                    self._service_heartbeat_failures[service_id] = 0
            
            # 其他检查代码...
            
            # 使用更短的检查间隔
            await asyncio.sleep(self._IDLE_HEARTBEAT_CHECK)
