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
        group: str = None,
        service_name: str = None,
        heartbeat_interval: float = 0.5,   # 闲时心跳间隔
        heartbeat_timeout: float = 5.0,    # 闲时心跳超时
        service_id: str = None,
        api_key: str = None,     # API密钥
        curve_server_key: bytes = None,  # 仅凭此参数判断是否启用加密
        logger_level: int = logging.INFO,
        disable_reconnect: bool = False,
        max_consecutive_reconnects=5,
    ):
        self._router_address = router_address
        self._hwm = hwm
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logger_level)
        self._service_name = service_name or self.__class__.__name__

        # 记录是否需要自行创建context
        self._context = context or zmq.asyncio.Context()
        self._socket = None
        self._idle_heartbeat_interval = heartbeat_interval
        self._idle_heartbeat_timeout = heartbeat_timeout
        self._busy_heartbeat_interval = 2.0    # 忙时心跳间隔，较长
        self._busy_heartbeat_timeout = 10.0    # 忙时心跳超时，较长
        
        # 当前使用的参数，初始为闲时参数
        self._heartbeat_interval = self._idle_heartbeat_interval
        self._heartbeat_timeout = self._idle_heartbeat_timeout
        
        self._group = group or self._service_name

        self._heartbeat_task = None
        self._process_messages_task = None
        self._reconnect_monitor_task = None
        self._pending_tasks = set({})  # 保留，用于任务生命周期管理
        
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
        self._reconnect_in_progress = False
        
        # 心跳状态
        self._heartbeat_status = False  # 当前心跳状态
        self._last_successful_heartbeat = time.time()  # 最后一次成功心跳时间
        self._heartbeat_history = []  # 心跳历史记录
        self._consecutive_reconnects = 0  # 连续重连次数
        self._last_reconnect_time = 0  # 上次重连时间
        self._max_consecutive_reconnects = max_consecutive_reconnects  # 最大连续重连次数
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

        self._disable_reconnect = disable_reconnect

        # 保存服务器公钥
        self._curve_server_key = curve_server_key
        if not self._curve_server_key:
            server_key_hex = os.environ.get("VOIDRAIL_CURVE_SERVER_KEY")
            if server_key_hex:
                try:
                    self._curve_server_key = bytes.fromhex(server_key_hex)
                    self._logger.info("从环境变量加载了CURVE服务器公钥")
                except ValueError:
                    self._logger.error("无效的服务器公钥格式，应为十六进制字符串")
    
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

    async def _do_reconnect(self):
        """合并重连流程为一个函数"""
        # 使用锁防止并发重连
        async with self._reconnect_lock:
            if self._state == DealerState.RECONNECTING:
                return False
            
            self._state = DealerState.RECONNECTING
            
            try:
                # 1. 关闭现有连接
                if self._socket and not self._socket.closed:
                    try:
                        self._socket.close(linger=0)
                    except Exception as e:
                        self._logger.warning(f"关闭socket错误: {e}")
                    finally:
                        self._socket = None
                    
                # 2. 取消消息处理任务但保留正在处理的请求
                if self._process_messages_task and not self._process_messages_task.done():
                    self._process_messages_task.cancel()
                    # 等待任务取消，但不要等待太长时间
                    try:
                        await asyncio.wait_for(
                            asyncio.shield(self._process_messages_task), 
                            timeout=0.5
                        )
                    except asyncio.TimeoutError:
                        pass
                    self._process_messages_task = None
                    
                # 3. 创建新连接
                self._consecutive_reconnects += 1
                self._socket = self._context.socket(zmq.DEALER)
                self._socket.identity = self._service_id.encode()
                self._socket.set_hwm(self._hwm)
                self._socket.setsockopt(zmq.LINGER, 0)
                
                # 自动检测是否需要CURVE加密
                if self._curve_server_key:
                    try:
                        # 生成临时客户端密钥对
                        client_public, client_secret = zmq.curve_keypair()
                        
                        # 应用CURVE设置
                        self._socket.curve_secretkey = client_secret
                        self._socket.curve_publickey = client_public
                        self._socket.curve_serverkey = self._curve_server_key
                        
                        self._logger.info(f"重连时启用CURVE加密，客户端公钥: {client_public.hex()[:8]}...")
                    except Exception as e:
                        self._logger.error(f"CURVE加密配置失败: {e}")
                
                self._socket.connect(self._router_address)
                
                # 5. 更新连接状态
                self._last_successful_heartbeat = time.time()
                backoff = min(3600, 5 * (2 ** min(10, self._consecutive_reconnects - 1)))
                self._reconnect_protected_until = time.time() + backoff
                
                # 6. 创建新任务并注册
                self._process_messages_task = asyncio.create_task(
                    self._process_messages(), 
                    name=f"{self._service_id}-process_messages"
                )
                await self._register_to_router()
                
                self._state = DealerState.RUNNING
                return True
                
            except Exception as e:
                self._logger.error(f"重连失败: {e}")
                self._state = DealerState.INIT  # 重置状态
                return False

    async def __aenter__(self):
        await self.start()
        return self
    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()

    async def start(self):
        """启动服务"""
        if self._state not in [DealerState.INIT, DealerState.STOPPED]:
            self._logger.warning(f"<{self._service_id}> Cannot start from {self._state} state")
            return False
            
        self._state = DealerState.RUNNING

        try:
            # 尝试启动和连接
            if not await self._do_reconnect():
                self._logger.error(f"<{self._service_id}> 网络连接失败")
                await self._cleanup_tasks() # 确保清理任务
                self._state = DealerState.STOPPED
                return False
            # 继续启动过程...
        except Exception as e:
            self._logger.error(f"启动失败: {e}")
            await self._cleanup_tasks() # 确保清理任务
            self._state = DealerState.STOPPED
            return False

    async def stop(self):
        """停止服务"""
        if self._state == DealerState.STOPPED:
            return
        
        # 添加标记防止重连
        self._disable_reconnect = True 
        self._state = DealerState.STOPPING
        
        # 主动通知Router服务下线
        try:
            if self._socket and not self._socket.closed:
                try:
                    await asyncio.wait_for(
                        self._socket.send_multipart([b"shutdown", b""]),
                        timeout=1.0
                    )
                    # 等待确认回复 - 不需要处理响应内容
                    try:
                        await asyncio.wait_for(self._socket.recv_multipart(), timeout=0.5)
                    except asyncio.TimeoutError:
                        pass
                except Exception as e:
                    self._logger.warning(f"通知Router关闭失败: {e}")
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

        for t in tasks:
            if not t.done():
                t.cancel()
                # 如果依旧没停，再给一次 very short await
                try:
                    await asyncio.wait_for(t, timeout=0.1)
                except:  # pragma: no cover
                    pass

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
            
            # 构建服务信息，移除处理能力相关字段
            service_info = {
                "group": self._group or self._service_name,
                "methods": serializable_methods,  # 使用可序列化的方法信息
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
        """处理消息主循环 - 增加超时主动触发重连"""
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
                
                # 尝试接收消息，使用较短的超时时间
                try:
                    multipart = await asyncio.wait_for(
                        self._socket.recv_multipart(),
                        timeout=min(1.0, self._heartbeat_interval * 2)  # 缩短超时时间
                    )
                except asyncio.TimeoutError:
                    # 超时时更新心跳状态
                    error_count += 1
                    self._update_heartbeat_status(False, "timeout")
                    
                    # 核心改进：连续超时过多次，主动触发重连
                    if error_count >= 5 and self._state == DealerState.RUNNING:
                        self._logger.warning(f"<{self._service_id}> 接收消息连续{error_count}次超时，主动断开重连")
                        self._state = DealerState.RECONNECTING
                        asyncio.create_task(self._do_reconnect())
                        break
                    
                    continue
                
                # 收到消息，重置错误计数
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

                elif message_type == b"router_shutdown":
                    self._logger.info(f"<{self._service_id}> Router主动通知关闭，准备重连")
                    self._state = DealerState.RECONNECTING
                    asyncio.create_task(self._do_reconnect())

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

    async def _update_busy_state(self):
        """根据当前任务数更新忙/闲状态"""
        is_busy = len(self._pending_tasks) > 0
        
        # 根据状态调整心跳参数
        if is_busy and self._heartbeat_interval != self._busy_heartbeat_interval:
            self._heartbeat_interval = self._busy_heartbeat_interval
            self._heartbeat_timeout = self._busy_heartbeat_timeout
            self._logger.debug(f"<{self._service_id}> 切换到忙时心跳模式: 间隔={self._heartbeat_interval}秒, 超时={self._heartbeat_timeout}秒")
        elif not is_busy and self._heartbeat_interval != self._idle_heartbeat_interval:
            self._heartbeat_interval = self._idle_heartbeat_interval
            self._heartbeat_timeout = self._idle_heartbeat_timeout
            self._logger.debug(f"<{self._service_id}> 切换到闲时心跳模式: 间隔={self._heartbeat_interval}秒, 超时={self._heartbeat_timeout}秒")
        
        return is_busy

    async def _heartbeat_loop(self):
        """简化后的心跳循环"""
        last_heartbeat_time = time.time()
        
        while self._state == DealerState.RUNNING:
            try:
                current_time = time.time()
                is_busy = len(self._pending_tasks) > 0
                
                # 动态调整心跳参数
                self._heartbeat_interval = self._busy_heartbeat_interval if is_busy else self._idle_heartbeat_interval
                self._heartbeat_timeout = self._busy_heartbeat_timeout if is_busy else self._idle_heartbeat_timeout
                
                # 判断是否需要发送心跳 - 简化判断逻辑
                elapsed = current_time - last_heartbeat_time
                if (elapsed >= self._heartbeat_interval or 
                    current_time - self._last_successful_heartbeat > self._heartbeat_timeout):
                    
                    # 心跳数据精简
                    heartbeat_data = {
                        "api_key": self._api_key,
                        "processing_requests": len(self._pending_tasks),
                        "is_busy": is_busy
                    }
                    
                    if self._socket and not self._socket.closed:
                        await self._socket.send_multipart([
                            b"heartbeat", 
                            json.dumps(heartbeat_data).encode()
                        ])
                        last_heartbeat_time = current_time
                        
                    # 未注册时尝试注册
                    if not self._service_registered:
                        await self._register_to_router()
                
                # 简化睡眠时间计算
                await asyncio.sleep(min(0.2, self._heartbeat_interval / 4))
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.error(f"<{self._service_id}> 心跳错误: {e}")
                await asyncio.sleep(1)  # 错误后短暂等待

    async def _reconnect_monitor(self):
        """增强的重连监控"""
        if self._disable_reconnect:
            return
        
        while self._state == DealerState.RUNNING:
            # 更积极的检查间隔
            check_interval = min(0.2, self._heartbeat_interval / 3)
            await asyncio.sleep(check_interval)
            
            # 更激进的超时判断
            not_living_interval = time.time() - self._last_successful_heartbeat
            threshold = self._heartbeat_timeout * 0.8  # 降低阈值，更早触发重连
            
            if not_living_interval > threshold:
                # 如果当前正在处理请求，延长超时时间
                if len(self._pending_tasks) > 0:
                    # 处理请求时允许更长的心跳超时
                    if not_living_interval <= threshold * 3:
                        continue
                self._logger.warning(f"心跳超时 {not_living_interval:.2f}秒 > {threshold:.2f}秒，触发重连")
                await self._do_reconnect()

    # 集中管理心跳状态的新方法
    def _update_heartbeat_status(self, status=True, message_type=None):
        """心跳状态更新 - 增加主动重连触发"""
        now = time.time()
        
        # 保留历史记录
        if len(self._heartbeat_history) > 20:
            self._heartbeat_history = self._heartbeat_history[-10:]
        self._heartbeat_history.append({"time": now, "type": message_type, "status": status})
        
        # 更新状态
        if status:
            self._heartbeat_status = True
            self._last_successful_heartbeat = now
        else:
            # 标记心跳失败状态
            self._heartbeat_status = False
            
            # 连续失败超过阈值时，主动触发重连
            failures = sum(1 for h in self._heartbeat_history[-5:] if not h.get("status", True))
            if failures >= 3 and self._state == DealerState.RUNNING:
                self._logger.warning(f"<{self._service_id}> 连续{failures}次心跳失败，立即执行重连")
                # 核心改进：主动创建异步任务执行重连
                asyncio.create_task(self._do_reconnect())
        
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

    async def _cleanup_tasks(self):
        """集中清理所有tasks的逻辑"""
        tasks = list(self._pending_tasks)
        
        for task_attr in ['_process_messages_task', '_heartbeat_task', '_reconnect_monitor_task']:
            task = getattr(self, task_attr, None)
            if task and not task.done():
                task.cancel()
                tasks.append(task)
                setattr(self, task_attr, None)
        
        for task in tasks:
            if not task.done():
                try:
                    await asyncio.wait_for(task, timeout=0.5)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
        
        self._pending_tasks.clear()
