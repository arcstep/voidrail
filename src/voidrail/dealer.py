from typing import Dict, Any, Optional, Callable, Awaitable, AsyncGenerator, Union
import zmq
import zmq.asyncio
import asyncio
import logging
import json
import inspect
import uuid
import time
import os
from enum import Enum
import socket

from functools import wraps
from pydantic import BaseModel

# 新增全局装饰器
def service_method(_func=None, *, name: str = None, description: str = None, params: dict = None, **metadata):
    """支持两种调用方式的装饰器"""
    def decorator(func):
        # 分析方法类型
        is_coroutine = inspect.iscoroutinefunction(func)
        is_async_gen = inspect.isasyncgenfunction(func)
        is_generator = inspect.isgeneratorfunction(func)
        is_stream = is_generator or is_async_gen
        
        # 存储元数据（保持原有逻辑）
        func.__service_metadata__ = {
            'name': name or func.__name__,
            'stream': is_stream,
            'is_coroutine': is_coroutine,
            'is_async_gen': is_async_gen,
            'is_generator': is_generator,
            'description': description,
            'params': params,
            'metadata': metadata
        }
        
        # 保持包装逻辑
        if is_stream:
            @wraps(func)
            async def wrapper(self, *args, **kwargs):
                try:
                    if is_async_gen:
                        async for item in func(self, *args, **kwargs):
                            yield item
                    else:
                        for item in func(self, *args, **kwargs):
                            yield item
                            await asyncio.sleep(0)
                except Exception as e:
                    self._logger.error(f"<{getattr(self, '_service_name', '__class__.__name__')}> Stream handler error: {e}")
                    raise
            return wrapper
        
        if is_coroutine:
            @wraps(func)
            async def async_wrapper(self, *args, **kwargs):
                try:
                    return await func(self, *args, **kwargs)
                except Exception as e:
                    self._logger.error(f"<{getattr(self, '_service_name', '__class__.__name__')}> Handler error: {e}")
                    raise
            return async_wrapper
        
        return func
    
    # 处理无参数调用
    if _func is None:
        return decorator
    return decorator(_func)

class ServiceDealerMeta(type):
    """元类处理独立注册表"""
    def __new__(cls, name, bases, namespace):
        klass = super().__new__(cls, name, bases, namespace)
        
        # 创建独立注册表
        klass._registry = {}
        
        # 添加继承日志
        logging.debug(f"<{name}> Processing class: {name}")
        logging.debug(f"<{name}> Base classes: {[b.__name__ for b in bases]}")
        
        # 合并继承链（保持深度优先）
        for base in bases:
            if hasattr(base, '_registry'):
                logging.debug(f"<{name}> Inheriting from {base.__name__}: {base._registry.keys()}")
                klass._registry.update(base._registry.copy())
        
        # 收集当前类方法
        methods_found = []
        for attr_name in dir(klass):
            attr = getattr(klass, attr_name)
            if hasattr(attr, '__service_metadata__'):
                meta = attr.__service_metadata__
                methods_found.append(meta['name'])
                logging.debug(f"<{name}> Found service method: {attr_name} -> {meta['name']}")
                klass._registry[meta['name']] = {
                    'method_name': attr_name,
                    'stream': meta['stream'],
                    'is_coroutine': meta['is_coroutine'],
                    'is_async_gen': meta['is_async_gen'],
                    'is_generator': meta['is_generator'],
                    'description': meta['description'],
                    'params': meta['params'],
                    'metadata': meta['metadata']
                }
        
        logging.info(f"<{name}> Final registry: {klass._registry.keys()}")
        return klass

class DealerState(Enum):
    INIT = 0       # 初始化状态
    RUNNING = 1    # 正常运行
    RECONNECTING = 2 # 重连中
    STOPPING = 3   # 停止中
    STOPPED = 4    # 已停止

class ServiceDealer(metaclass=ServiceDealerMeta):
    """服务端 DEALER 实现，用于处理具体服务请求"""
    
    _registry = {}  # 保持原有类属性
    
    def __init__(
        self,
        router_address: str,
        context: Optional[zmq.asyncio.Context] = None,
        hwm: int = 1000,        # 网络层面的背压控制
        max_concurrent: int = 100,  # 应用层面的背压控制
        group: str = None,
        service_name: str = None,
        heartbeat_interval: float = 0.5,
        heartbeat_timeout: float = 5.0,
        service_id: str = None,
        api_key: str = None,     # 新增: API密钥
        port: int = None,        # 新增: 服务端口
        logger_level: int = logging.INFO,
        disable_reconnect: bool = False,
        max_consecutive_reconnects=5,
        use_curve: bool = False,               # 是否启用CURVE加密
        curve_client_key_file: str = None,     # 客户端密钥文件
        curve_server_key: bytes = None,        # 服务器公钥
    ):
        self._router_address = router_address
        self._hwm = hwm
        self._max_concurrent = max_concurrent
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logger_level)
        self._service_name = service_name or self.__class__.__name__

        # 记录是否需要自行创建context
        self._context = context or zmq.asyncio.Context()
        self._socket = None
        self._semaphore = None
        self._current_load = 0
        self._is_overload = False
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_timeout = heartbeat_timeout
        self._group = group or self._service_name

        self._heartbeat_task = None
        self._process_messages_task = None
        self._reconnect_monitor_task = None
        self._pending_tasks = set({})
        
        # 从类注册表中复制服务方法到实例
        self._handlers = {}
        for name, info in self.__class__._registry.items():
            self._handlers[name] = {
                'handler': getattr(self, info['method_name']),
                'metadata': info['metadata']
            }

        # 生成一个随机的 UUID 作为服务标识
        self._service_id = service_id or f'{self._service_name}-{str(uuid.uuid4().hex[:8])}'

        # 状态管理
        self._state = DealerState.INIT
        self._state_lock = asyncio.Lock()  # 状态锁
        self._reconnect_in_progress = False
        
        # 心跳状态
        self._heartbeat_status = False  # 当前心跳状态
        self._last_successful_heartbeat = time.time()  # 最后一次成功心跳时间
        self._heartbeat_history = []  # 心跳历史记录
        self._consecutive_reconnects = 0  # 连续重连次数
        self._last_reconnect_time = 0  # 上次重连时间
        self._max_consecutive_reconnects = max_consecutive_reconnects  # 最大连续重连次数
        self._connection_state = "INIT"  # 连接状态: INIT, CONNECTED, RECONNECTING, PROTECTED
        self._heartbeat_ack_count = 0  # 心跳确认计数
        
        # 重连保护锁和同步变量
        self._reconnect_lock = asyncio.Lock()  # 重连操作锁
        self._reconnect_protected_until = 0  # 重连保护期结束时间
        
        # 网络诊断
        self._network_failures = 0  # 网络失败次数
        self._diagnostics = {
            "last_error": None,
            "connection_history": [],
            "received_messages": 0,
            "sent_messages": 0,
        }

        # API密钥设置
        self._api_key = api_key or os.environ.get("VOIDRAIL_API_KEY")
        if not self._api_key:
            self._logger.warning(f"<{self._service_id}> 未设置API密钥，可能无法连接到开启了验证的Router")

        # 保存端口信息（如果提供）
        self._port = port

        self._disable_reconnect = disable_reconnect

        # CURVE加密设置
        self._use_curve = use_curve
        self._curve_server_key = curve_server_key
        
        # 创建socket时应用CURVE设置
    
    async def _force_reconnect(self):
        """强制完全重置连接"""
        self._logger.info("Initiating forced reconnection...")
        
        # 重新初始化socket
        self._socket = self._context.socket(zmq.DEALER)
        self._socket.identity = self._service_id.encode()
        self._socket.set_hwm(self._hwm)
        self._socket.setsockopt(zmq.LINGER, 0)  # 设置无等待关闭
        self._socket.setsockopt(zmq.IMMEDIATE, 1)  # 禁用缓冲
        self._socket.connect(self._router_address)
        
        # 重置心跳状态
        self._last_successful_heartbeat = time.time()
        self._heartbeat_sent_count = 0
        self._heartbeat_ack_count = 0
        self._heartbeat_status = True

    async def _reconnect(self):
        """重新连接到路由器 - 修复清理顺序"""
        if self._connection_state == "PROTECTED":
            self._logger.info(f"<{self._service_id}> 处于重连保护期，跳过重连")
            return False

        self._logger.info(f"<{self._service_id}> 开始执行重连...")

        try:
            # 1. 先关闭旧 Socket
            if self._socket and not self._socket.closed:
                 self._logger.info(f"<{self._service_id}> Closing existing socket before reconnecting...")
                 try:
                     self._socket.close(linger=0)
                 except Exception as close_err:
                     self._logger.warning(f"<{self._service_id}> Error closing socket in _reconnect: {close_err}")
                 finally:
                    self._socket = None

            # 2. 只取消现有任务，不再等待
            if self._process_messages_task and not self._process_messages_task.done():
                self._logger.info(f"<{self._service_id}> Cancelling _process_messages_task in _reconnect (NO AWAIT)...")
                self._process_messages_task.cancel()
                # 只取消，不 await，直接将引用置空
                self._process_messages_task = None

            # ... (更新状态, 创建信号量, 创建并连接新 socket 等) ...
            self._consecutive_reconnects += 1
            self._last_reconnect_time = time.time()
            self._connection_state = "RECONNECTING"
            self._service_registered = False
            self._semaphore = asyncio.Semaphore(self._max_concurrent)

            self._logger.info(f"<{self._service_id}> Creating and connecting new socket...")
            self._socket = self._context.socket(zmq.DEALER)
            self._socket.identity = self._service_id.encode()
            self._socket.set_hwm(self._hwm)
            self._socket.setsockopt(zmq.LINGER, 0)
            self._socket.setsockopt(zmq.IMMEDIATE, 1)
            self._socket.connect(self._router_address)

            # 在创建新socket后设置CURVE
            if self._use_curve and self._curve_server_key:
                # 需要确保每次重连时使用相同客户端密钥(可选)
                if hasattr(self, '_curve_client_keypair'):
                    client_public, client_secret = self._curve_client_keypair
                else:
                    if hasattr(self, '_curve_client_key_file') and os.path.exists(self._curve_client_key_file):
                        client_public, client_secret = zmq.auth.load_certificate(self._curve_client_key_file)
                    else:
                        client_public, client_secret = zmq.curve_keypair()
                        # 存储密钥对以便重用
                        self._curve_client_keypair = (client_public, client_secret)
                
                # 应用CURVE设置
                self._socket.curve_secretkey = client_secret
                self._socket.curve_publickey = client_public
                self._socket.curve_serverkey = self._curve_server_key
                
                self._logger.info(f"重连时启用CURVE加密")

            now = time.time()
            self._update_heartbeat_status(True, "reconnect")
            self._heartbeat_ack_count = 0
            self._logger.info(f"<{self._service_id}> 重连成功")
            self._connection_state = "CONNECTED"
            # ... (记录历史, 设置保护期) ...
            backoff_seconds = min(3600, 5 * (2 ** min(10, self._consecutive_reconnects - 1)))
            self._reconnect_protected_until = now + backoff_seconds

            # 创建新的消息处理任务
            self._logger.info(f"<{self._service_id}> Starting new _process_messages task after reconnect.")
            self._process_messages_task = asyncio.create_task(
                self._process_messages(),
                name=f"{self._service_id}-process_messages"
            )

            # 立即进行服务注册
            await self._register_to_router()

            return True

        except Exception as e:
            self._logger.error(f"<{self._service_id}> 重连过程中发生错误: {e}", exc_info=True)
            self._diagnostics["last_error"] = str(e)
            return False

    async def start(self):
        """启动服务"""
        async with self._state_lock:
            if self._state not in [DealerState.INIT, DealerState.STOPPED]:
                self._logger.warning(f"<{self._service_id}> Cannot start from {self._state} state")
                return False
                
            self._state = DealerState.RUNNING

        # 重建连接
        if not await self._reconnect():
            self._logger.error(f"<{self._service_id}> 网络连接失败")
            return False

        # 启动核心任务
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name=f"{self._service_id}-heartbeat")
        self._process_messages_task = asyncio.create_task(self._process_messages(), name=f"{self._service_id}-process_messages")
        self._reconnect_monitor_task = asyncio.create_task(self._reconnect_monitor(), name=f"{self._service_id}-reconnect_monitor")

        await self._register_to_router()

        self._logger.info(f"<{self._service_id}> Service {self._service_id} started with {len(self._handlers)} methods")
        self._last_successful_heartbeat = time.time()
        return True

    async def stop(self):
        """停止服务"""
        async with self._state_lock:
            if self._state == DealerState.STOPPED:
                return
                
            self._state = DealerState.STOPPING
        
        # 主动通知Router服务下线（添加超时保护）
        try:
            if self._socket and not self._socket.closed:
                try:
                    await asyncio.wait_for(
                        self._socket.send_multipart([b"shutdown", b""]),
                        timeout=1.0
                    )
                    try:
                        await asyncio.wait_for(self._socket.recv_multipart(), timeout=0.5)
                    except asyncio.TimeoutError:
                        pass
                except Exception:
                    pass
        except Exception:
            pass
            
        # 取消任务
        tasks = list(self._pending_tasks)
        
        for task_attr in ['_process_messages_task', '_heartbeat_task', '_reconnect_monitor_task']:
            task = getattr(self, task_attr, None)
            if task:
                task.cancel()
                tasks.append(task)
                setattr(self, task_attr, None)  # 立即清空引用
        
        # 设置有限超时等待任务完成
        if tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=2.0
                )
            except asyncio.TimeoutError:
                pass
        
        # 确保套接字关闭
        if self._socket:
            self._socket.close(linger=0)
            self._socket = None
        
        self._state = DealerState.STOPPED

    async def _register_to_router(self):
        """向Router注册服务信息"""
        try:
            # 获取本机网络信息
            hostname = socket.gethostname()
            try:
                # 尝试获取外部可访问的IP地址
                ip_address = socket.gethostbyname(hostname)
            except:
                # 如果无法获取，使用本地回环地址
                ip_address = "127.0.0.1"
            
            # 获取进程ID作为标识
            process_id = os.getpid()
            
            # 构建地址信息 - 使用更有意义的格式
            if hasattr(self, '_port') and self._port:
                # 如果有明确指定的端口，使用它
                remote_addr = f"{ip_address}:{self._port}"
            else:
                # 否则使用进程ID和服务ID的组合
                service_uuid = self._service_id.split('-')[-1] if '-' in self._service_id else self._service_id[-8:]
                remote_addr = f"{ip_address} [PID:{process_id}, ID:{service_uuid}]"
            
            # 创建可序列化的方法信息字典
            serializable_methods = {}
            for name, info in self._registry.items():
                # 只收集元数据，不包含实际方法对象
                serializable_methods[name] = {
                    'description': info.get('description', ''),
                    'params': info.get('params', {}),
                    'stream': info.get('stream', False),
                    'metadata': info.get('metadata', {})
                }
            
            # 构建服务信息
            service_info = {
                "group": self._group or self._service_name,
                "methods": serializable_methods,  # 使用可序列化的方法信息
                "max_concurrent": self._max_concurrent,
                "current_load": self._current_load,
                "request_count": 0,
                "reply_count": 0,
                "api_key": self._api_key,
                "remote_addr": remote_addr,
                "host_info": {
                    "hostname": hostname,
                    "ip": ip_address,
                    "pid": process_id
                }
            }
            
            self._logger.info(f"<{self._service_id}> Registering service with info: {{methods: {list(serializable_methods.keys())}, group: {self._group}, addr: {remote_addr}}}")
            
            # 发送注册请求
            await self._socket.send_multipart([
                b"register",
                json.dumps(service_info).encode()
            ])
            
            self._service_registered = True
        
        except asyncio.CancelledError:
            return
        except zmq.ZMQError as e:
            self._service_registered = False
            self._logger.error(f"<{self._service_id}> Registration failed: {str(e)}")
        except Exception as e:
            self._service_registered = False
            self._logger.error(f"<{self._service_id}> Registration failed: {str(e)}", exc_info=True)

    async def _process_messages(self):
        """处理消息主循环 - 强化版"""
        last_diagnostics_time = time.time()
        error_count = 0
        
        self._logger.info(f"<{self._service_id}> 消息处理任务启动")
        
        while self._state == DealerState.RUNNING:
            try:
                await asyncio.sleep(0)
                
                # 检查socket状态
                if not self._socket or self._socket.closed:
                    self._logger.warning(f"<{self._service_id}> 消息处理发现socket已关闭或为None，中止")
                    break
                
                # 尝试接收消息
                try:
                    multipart = await asyncio.wait_for(
                        self._socket.recv_multipart(),
                        timeout=max(3.0, self._heartbeat_interval * 6)
                    )
                except asyncio.TimeoutError:
                    if error_count % 10 == 0:  # 仅每10次超时记录一次，避免日志过多
                        self._logger.warning(f"<{self._service_id}> 接收消息超时")
                    error_count += 1
                    continue
                    
                # 重置错误计数
                error_count = 0
                
                # 增加收到消息计数，用于诊断
                self._diagnostics["received_messages"] += 1
                
                # 定义消息类型
                message_type = multipart[0]

                # 更新心跳状态，任何消息都算心跳
                self._update_heartbeat_status(True, message_type.decode())
                
                # 周期性打印诊断信息
                current_time = time.time()
                if current_time - last_diagnostics_time > 30:
                    self._logger.info(f"<{self._service_id}> 消息统计：收到 {self._diagnostics['received_messages']} 条，"
                                    f"发送 {self._diagnostics['sent_messages']} 条")
                    last_diagnostics_time = current_time
                
                # 对于特定类型的消息，不严格要求目标客户端ID
                is_special_message = message_type in [b"heartbeat_ack", b"register_ack", b"error"]
                
                if len(multipart) < 2 and not is_special_message:
                    self._logger.warning(f"<{self._service_id}> Invalid message format, missing target")
                    continue
                
                target_client_id = multipart[1]
                request_json = multipart[-1].decode() if len(multipart) >= 3 else None

                if message_type == b"call_from_router" and request_json:
                    request = json.loads(request_json)
                    if request.get("type") == "request":
                        task = asyncio.create_task(self._process_request(target_client_id, request), name=f"{self._service_id}-{request.get('request_id')}")
                        self._pending_tasks.add(task)
                        task.add_done_callback(self._pending_tasks.discard)

                elif message_type == b"heartbeat_ack":
                    # 更新所有心跳状态标记
                    self._heartbeat_ack_count += 1
                    self._heartbeat_status = True
                    self._last_successful_heartbeat = time.time()
                    self._logger.debug(f"<{self._service_id}> 收到心跳确认 #{self._heartbeat_ack_count}")
                
                elif message_type == b"register_ack":
                    self._logger.info(f"<{self._service_id}> Service registered successfully.")
                
                elif message_type == b"error":
                    error_message = multipart[1].decode() if len(multipart) > 1 else "Unknown error"
                    self._logger.error(f"<{self._service_id}> error: {error_message}")

                else:
                    self._logger.error(f"<{self._service_id}> DEALER Received unknown message type: {message_type}")

            except asyncio.CancelledError:
                self._logger.info(f"<{self._service_id}> 消息处理任务被取消")
                break
            except Exception as e:
                error_count += 1
                self._logger.error(f"<{self._service_id}> 消息处理错误: {e}", exc_info=True)
                self._diagnostics["last_error"] = str(e)
                if error_count > 5:
                    await asyncio.sleep(1.0)  # 频繁错误时增加等待
        
        self._logger.info(f"<{self._service_id}> 消息处理任务结束")

    async def _process_request(self, target_client_id: bytes, request: dict):
        """处理单个请求"""
        self._logger.info(f"<{self._service_id}> DEALER Processing request: {request}")
        if self._current_load >= self._max_concurrent:
            await self._send_error(
                target_client_id,
                "Service overloaded"
            )
            self._logger.info(f"<{self._service_id}> DEALER Service overloaded, rejecting request from {self._service_id}")
            return

        try:
            # 防止信号量为None导致崩溃
            if not hasattr(self, '_semaphore') or self._semaphore is None:
                self._logger.warning(f"<{self._service_id}> 处理请求前信号量为None，重新创建")
                self._semaphore = asyncio.Semaphore(self._max_concurrent)
            
            async with self._semaphore:
                self._current_load += 1
                
                # 检查是否需要报告即将满载
                if not self._is_overload and self.check_overload():
                    self._is_overload = True
                    await self._socket.send_multipart([b"overload", b""])
                
                try:
                    # 检查方法是否注册过
                    func_name = request.get("func_name", "").split('.')[-1]
                    if func_name in self._handlers:
                        handler = self._handlers[func_name]['handler']
                        handler_info = self._registry[func_name]
                        is_stream = handler_info['stream']
                        is_coroutine = handler_info['is_coroutine']
                    else:
                        await self._send_error(
                            target_client_id,
                            f"Method {request.get('func_name')} not found"
                        )
                        return

                    try:
                        if is_stream:
                            self._logger.info(f"<{self._service_id}> Streaming response for {request.get('func_name')}")
                            # 处理流式响应
                            async for chunk in handler(*request.get("args", []), **request.get("kwargs", {})):
                                # 将Pydantic模型转换为字典
                                if isinstance(chunk, BaseModel):
                                    chunk = chunk.model_dump()
                                
                                # 创建流式响应消息
                                message = {
                                    "type": "streaming",
                                    "request_id": request.get("request_id"),
                                    "data": chunk
                                }

                                await self._socket.send_multipart([
                                    b"reply_from_dealer",
                                    target_client_id,
                                    json.dumps(message).encode()
                                ])
                            
                            # 发送结束标记
                            end_message = {
                                "type": "end",
                                "request_id": request.get("request_id")
                            }
                            await self._socket.send_multipart([
                                b"reply_from_dealer",
                                target_client_id,
                                json.dumps(end_message).encode()
                            ])
                        else:
                            # 处理普通响应
                            if is_coroutine:
                                result = await handler(*request.get("args", []), **request.get("kwargs", {}))
                            else:
                                result = handler(*request.get("args", []), **request.get("kwargs", {}))

                            # 将Pydantic模型转换为字典
                            if isinstance(result, BaseModel):
                                result = result.model_dump()
                                
                            # 创建响应消息
                            reply = {
                                "type": "reply",
                                "request_id": request.get("request_id"),
                                "result": result
                            }
                                
                            await self._socket.send_multipart([
                                b"reply_from_dealer",
                                target_client_id,
                                json.dumps(reply).encode()
                            ])
                    except zmq.ZMQError as e:
                        self._logger.error(f"<{self._service_id}> DEALER ZMQError: {e}")
                        await asyncio.sleep(2)
                    except Exception as e:
                        self._logger.error(f"<{self._service_id}> DEALER Handler error: {e}", exc_info=True)
                        # 向客户端发送错误响应
                        await self._send_error(
                            target_client_id,
                            f"Method execution error: {str(e)}"
                        )
                except Exception as e:
                    self._logger.error(f"<{self._service_id}> DEALER Request processing error: {e}", exc_info=True)
        finally:
            self._current_load -= 1
            if self._current_load < 0:
                self._current_load = 0
            
            # 检查是否可以恢复服务
            if self._is_overload and self.check_can_resume():
                self._is_overload = False
                await self._socket.send_multipart([b"resume", b""])

    async def _send_error(self, target_client_id: bytes, error_msg: str):
        """发送错误响应"""
        error = {
            "type": "error",
            "error": error_msg
        }
        await self._socket.send_multipart([
            b"reply_from_dealer",
            target_client_id,
            json.dumps(error).encode()
        ])

    async def _heartbeat_loop(self):
        """改进的心跳和健康监控循环"""
        heartbeat_count = 0
        heartbeat_misses = 0
        
        while self._state == DealerState.RUNNING:
            try:
                # 发送心跳
                if self._socket and self._state == DealerState.RUNNING:
                    # 在心跳中包含更多诊断信息
                    heartbeat_data = {
                        "api_key": self._api_key,
                        "processing_requests": self._current_load,
                        "dealer_info": {
                            "service_name": self._service_name,
                            "group": self._group,
                            "pid": os.getpid(),
                            "heartbeat_count": heartbeat_count,
                            "connection_state": self._connection_state
                        }
                    }
                    
                    await self._socket.send_multipart([
                        b"heartbeat", 
                        json.dumps(heartbeat_data).encode()
                    ])
                    heartbeat_count += 1
                    self._diagnostics["sent_messages"] += 1
                    
                    # 检查最近是否收到过心跳响应
                    elapsed = time.time() - self._last_successful_heartbeat
                    if elapsed > self._heartbeat_interval * 3:
                        heartbeat_misses += 1
                        if heartbeat_misses >= 3:
                            self._logger.warning(f"<{self._service_id}> 已连续 {heartbeat_misses} 次未收到心跳确认")
                    else:
                        # 重置计数
                        heartbeat_misses = 0
                
                # 如果服务未注册，尝试注册
                if not self._service_registered:
                    await self._register_to_router()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.error(f"<{self._service_id}> Error in heartbeat loop: {e}")
                self._diagnostics["last_error"] = str(e)
                await asyncio.sleep(2)
            finally:
                await asyncio.sleep(self._heartbeat_interval)

    async def request_reconnect(self):
        """请求重连 - 修复清理顺序"""
        # 使用锁防止并发重连操作
        async with self._reconnect_lock:
            self._logger.debug(f"<{self._service_id}> Inside request_reconnect lock")
            # 已经在重连中或保护期内，则跳过
            if self._connection_state == "RECONNECTING" or time.time() < self._reconnect_protected_until:
                self._logger.info(f"<{self._service_id}> Skipping reconnect request. State: {self._connection_state}, Protected until: {self._reconnect_protected_until}")
                return

            self._connection_state = "RECONNECTING"
            now = time.time()
            # ... (记录连接历史等) ...

            # 1. 先关闭旧 Socket
            if self._socket and not self._socket.closed:
                self._logger.info(f"<{self._service_id}> Closing existing socket in request_reconnect...")
                try:
                    self._socket.close(linger=0) # 确保快速关闭
                except Exception as close_err:
                    self._logger.warning(f"<{self._service_id}> Error closing socket in request_reconnect: {close_err}")
                finally:
                    self._socket = None
            self._service_registered = False # 标记需要重新注册

            # 2. 只取消旧的消息处理任务，不再等待
            if self._process_messages_task and not self._process_messages_task.done():
                self._logger.info(f"<{self._service_id}> Cancelling _process_messages_task in request_reconnect (NO AWAIT)...")
                self._process_messages_task.cancel()
                # 只取消，不 await，直接将引用置空
                self._process_messages_task = None

            self._logger.debug(f"<{self._service_id}> Exiting request_reconnect lock")

    async def _reconnect_monitor(self):
        """监控并处理重连请求 - 完全重构版"""        
        self._logger.info(f"<{self._service_id}> 重连监控器已启动")
        
        # 禁用重连选项（测试用）
        if self._disable_reconnect:
            self._logger.warning(f"重连监控已被禁用（仅用于测试）")
            while self._state == DealerState.RUNNING:
                await asyncio.sleep(10.0)
            return
        
        # 主循环
        last_check_time = time.time()
        
        while self._state == DealerState.RUNNING:
            check_interval = self._heartbeat_timeout / 3.0
            check_interval = max(0.1, check_interval) # 最小检查间隔 100ms
            await asyncio.sleep(check_interval)
            
            current_time = time.time()
            check_interval = current_time - last_check_time
            last_check_time = current_time
            
            # 检查是否处于重连保护期
            if current_time < self._reconnect_protected_until:
                if (current_time - last_check_time) > self._heartbeat_timeout:
                    self._logger.info(f"<{self._service_id}> 重连保护期内，剩余 "
                                     f"{int(self._reconnect_protected_until - current_time)} 秒")
                continue
            
            # 正在进行的重连操作
            if self._connection_state == "RECONNECTING":
                continue
            
            # 心跳超时检测
            not_living_interval = current_time - self._last_successful_heartbeat
            
            # 渐进式超时检测：根据连续重连次数增加宽容度
            timeout_threshold = self._heartbeat_timeout * (1 + 0.5 * min(5, self._consecutive_reconnects))
            
            # 如果超时，需要重连
            if not_living_interval > timeout_threshold:
                # 首先，确认这不是误报
                if self._connection_state == "CONNECTED" and self._consecutive_reconnects > 3:
                    # 对于已经连续重连多次的情况，采用更严格的超时检测
                    confirmation_timeout = timeout_threshold * 1.5
                    self._logger.warning(f"<{self._service_id}> 检测到潜在心跳超时 ({not_living_interval:.1f}秒)，"
                                       f"等待确认 ({confirmation_timeout-not_living_interval:.1f}秒后再决定)")
                    
                    # 简单等待一段时间再次确认，避免误判
                    await asyncio.sleep(confirmation_timeout - not_living_interval)
                    
                    # 再次检查，如果依然超时，才真正触发重连
                    current_not_living = time.time() - self._last_successful_heartbeat
                    if current_not_living <= timeout_threshold:
                        self._logger.info(f"<{self._service_id}> 心跳已恢复，取消重连")
                        continue
                
                # 执行重连
                self._logger.debug(f"<{self._service_id}> 已获取重连锁")
                if self._connection_state != "RECONNECTING" and current_time >= self._reconnect_protected_until:
                    self._logger.warning(f"<{self._service_id}> 心跳超时 ({not_living_interval:.1f}秒 > {timeout_threshold:.1f}秒)，触发重连")

                    try:
                        self._logger.info(f"<{self._service_id}> 调用 request_reconnect...")
                        await self.request_reconnect()
                        self._logger.info(f"<{self._service_id}> request_reconnect 调用完成")

                        self._logger.info(f"<{self._service_id}> 调用 _reconnect...")
                        if await self._reconnect():
                            self._logger.info(f"<{self._service_id}> _reconnect 调用成功，准备重启消息处理和注册")
                        else:
                            self._logger.error(f"<{self._service_id}> _reconnect 调用失败")
                            self._network_failures += 1
                    except Exception as e:
                        self._logger.error(f"<{self._service_id}> 在重连监控器中执行重连步骤时发生异常: {e}", exc_info=True)

                else:
                    # 添加日志，说明为什么跳过了实际重连步骤
                    self._logger.info(f"<{self._service_id}> 获取锁后跳过重连，当前状态: {self._connection_state}, 保护期到: {self._reconnect_protected_until}")

    def check_overload(self) -> bool:
        """检查是否接近满载（可重写）
        默认策略：当前负载达到最大并发的90%时认为即将满载
        """
        return self._current_load >= self._max_concurrent * 0.9

    def check_can_resume(self) -> bool:
        """检查是否可以恢复服务（可重写）
        默认策略：当前负载低于最大并发的80%时可以恢复
        """
        return self._current_load <= self._max_concurrent * 0.8

    # 集中管理心跳状态的新方法
    def _update_heartbeat_status(self, status=True, message_type=None):
        """集中式更新心跳状态"""
        now = time.time()
        
        # 记录心跳历史
        self._heartbeat_history.append({
            "time": now,
            "status": status,
            "message_type": message_type,
        })
        
        # 只保留最近的50条记录
        if len(self._heartbeat_history) > 50:
            self._heartbeat_history = self._heartbeat_history[-50:]
        
        # 更新状态
        if status:
            # 成功收到心跳或其他消息
            self._heartbeat_status = True
            self._last_successful_heartbeat = now
            
            # 如果之前有连续重连，现在重置
            if self._consecutive_reconnects > 0:
                self._logger.info(f"<{self._service_id}> 连接恢复稳定，重置连续重连计数")
                self._consecutive_reconnects = 0
        
        return status

    def _run_connection_diagnostics(self):
        """运行连接诊断"""
        self._logger.warning(f"<{self._service_id}> 检测到连接问题，执行网络诊断...")
        
        # 检查最近的心跳历史
        recent_heartbeats = self._heartbeat_history[-10:] if self._heartbeat_history else []
        heartbeat_acks = sum(1 for h in recent_heartbeats if h.get("message_type") == "heartbeat_ack")
        
        diagnostics_info = {
            "consecutive_reconnects": self._consecutive_reconnects,
            "recent_heartbeat_acks": heartbeat_acks,
            "recent_heartbeats_sent": min(10, len(recent_heartbeats)),
            "last_error": self._diagnostics.get("last_error"),
            "received_messages": self._diagnostics.get("received_messages", 0),
            "sent_messages": self._diagnostics.get("sent_messages", 0),
        }
        
        # 提供诊断结果
        if heartbeat_acks == 0 and recent_heartbeats:
            self._logger.error(f"<{self._service_id}> 诊断结果：所有心跳请求无响应，可能是网络单向通信问题")
        elif self._consecutive_reconnects > 5:
            self._logger.error(f"<{self._service_id}> 诊断结果：连续多次重连失败，可能是ROUTER不可用或网络问题")
        
        self._logger.info(f"<{self._service_id}> 连接诊断信息: {diagnostics_info}")
