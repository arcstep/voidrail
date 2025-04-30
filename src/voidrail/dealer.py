import zmq
import threading
import time
import json
import socket
import uuid
import logging
import inspect
import asyncio
from enum import Enum
from functools import wraps
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Optional, Generator, AsyncGenerator, Callable, Union
from pydantic import BaseModel
import os

# -----------------------------------------------------------------------------
# 装饰器：标记服务方法，支持同步、异步、生成器、异步生成器
# -----------------------------------------------------------------------------
def service_method(_func=None, *, name: str = None, description: str = None, params: dict = None, **metadata):
    """
    用于标记 Dealer 提供的 RPC 方法。
    支持：
      - 同步函数
      - 原生协程函数 (async def)
      - 同步生成器 (yield)
      - 异步生成器 (async def ... yield)
    """
    def decorator(func):
        is_coroutine   = inspect.iscoroutinefunction(func)
        is_asyncgen    = inspect.isasyncgenfunction(func)
        is_generator   = inspect.isgeneratorfunction(func)
        is_stream      = is_asyncgen or is_generator

        func.__service_metadata__ = {
            'name':         name or func.__name__,
            'stream':       is_stream,
            'is_coroutine': is_coroutine,
            'is_asyncgen':  is_asyncgen,
            'is_generator': is_generator,
            'description':  description,
            'params':       params,
            'metadata':     metadata,
        }
        return func
    
    # 支持两种用法：@service_method 与 @service_method(...)
    if _func is None:
        return decorator
    else:
        return decorator(_func)

# -----------------------------------------------------------------------------
# 元类：收集 service_method 装饰的方法到类属性 _registry
# -----------------------------------------------------------------------------
class ServiceDealerMeta(type):
    """
    通过元类在类创建时，扫描带有 __service_metadata__ 的方法，
    将其元信息汇总到 klass._registry 中，供实例化时拷贝使用。
    """
    def __new__(cls, name, bases, namespace):
        klass = super().__new__(cls, name, bases, namespace)
        
        # 建立或继承父类的注册表
        registry = {}
        for base in bases:
            if hasattr(base, '_registry'):
                registry.update(base._registry)
        # 扫描本类 namespace
        for attr_name, attr in namespace.items():
            if hasattr(attr, '__service_metadata__'):
                meta = attr.__service_metadata__
                registry[meta['name']] = {
                    'handler':      attr,
                    'stream':       meta['stream'],
                    'is_coroutine': meta['is_coroutine'],
                    'is_asyncgen':  meta['is_asyncgen'],
                    'is_generator': meta['is_generator'],
                    'description':  meta['description'],
                    'params':       meta['params'],
                    'metadata':     meta['metadata'],
                }
        klass._registry = registry
        return klass

# -----------------------------------------------------------------------------
# Dealer 状态
# -----------------------------------------------------------------------------
class DealerState(Enum):
    INIT          = 0    # 初始化
    RUNNING       = 1    # 正常运行
    RECONNECTING  = 2    # 重连中
    STOPPING      = 3    # 停止中
    STOPPED       = 4    # 已停止

# -----------------------------------------------------------------------------
# 纯线程 + 阻塞 ZMQ 的 ServiceDealer
# -----------------------------------------------------------------------------
class ServiceDealer(metaclass=ServiceDealerMeta):
    def __init__(
        self,
        router_address: str,
        hwm: int = 1000,
        group: Optional[str] = None,
        service_name: Optional[str] = None,
        heartbeat_interval: float = 1.0,
        logger: logging.Logger = None,
        api_key: Optional[str] = None,
        curve_server_key: Optional[bytes] = None,
        disable_reconnect: bool = False,
        max_consecutive_reconnects: int = 5,
        max_workers: int = 4,
    ):
        # ---------------- 基本属性 ----------------
        self._router_address = router_address
        self._hwm            = hwm
        self._logger         = logger or logging.getLogger(__name__)
        self._service_name   = service_name or self.__class__.__name__
        self._group          = group or self._service_name
        
        # 重要：先设置 service_id，确保日志中使用
        self._service_id  = f"{self._service_name}-{uuid.uuid4().hex[:8]}"
        
        # --------------- 心跳与超时参数 ----------------
        I      = heartbeat_interval         # 用户配置的心跳间隔
        T_idle = I + 3.0                    # 空闲超时
        I_busy = I * 4.0                    # 忙时心跳间隔
        T_busy = T_idle * 2.0               # 忙时超时
        T_recv = I * 5.0                    # 接收消息超时

        self._idle_heartbeat_interval = I
        self._idle_heartbeat_timeout  = T_idle
        self._busy_heartbeat_interval = I_busy
        self._busy_heartbeat_timeout  = T_busy
        self._receive_timeout         = T_recv

        # 当前有效的心跳参数（会随忙/闲切换）
        self._heartbeat_interval = self._idle_heartbeat_interval
        self._heartbeat_timeout  = self._idle_heartbeat_timeout
        
        # ----------------- 状态管理 -----------------
        self._state                    = DealerState.INIT
        self._disable_reconnect        = disable_reconnect
        self._reconnect_lock           = threading.Lock()
        self._reconnect_protected_until= 0
        self._max_consecutive_reconnects = max_consecutive_reconnects
        self._consecutive_reconnects     = 0

        # --------------- 心跳监控状态 ----------------
        self._heartbeat_status         = False
        self._last_successful_heartbeat= time.time()
        self._heartbeat_history        = []      # 记录最近心跳状况
        self._heartbeat_sent_count     = 0
        self._heartbeat_ack_count      = 0

        # ---------------- 网络诊断 ------------------
        self._network_failures = 0
        self._diagnostics      = {
            "last_error": None,
            "connection_history": [],
            "received_messages": 0,
            "sent_messages": 0,
        }

        # --------------- 安全凭证 -------------------
        self._api_key = api_key or os.environ.get("VOIDRAIL_API_KEY")
        if curve_server_key:
            self._curve_server_key = curve_server_key
        else:
            # 从环境变量读取时需要转换为字节
            curve_key_hex = os.environ.get("VOIDRAIL_CURVE_SERVER_KEY")
            if curve_key_hex:
                try:
                    self._curve_server_key = bytes.fromhex(curve_key_hex)
                    self._logger.info(f"<{self._service_id}> 从环境变量加载了CURVE服务器公钥")
                except ValueError:
                    self._logger.error(f"<{self._service_id}> 无效的服务器公钥格式，应为十六进制字符串")
            else:
                self._curve_server_key = None

        # --------------- ZMQ & 线程池 ----------------
        self._ctx         = zmq.Context()   # 同步 Context
        self._socket      = None
        self._socket_lock = threading.RLock()  # 保护 send/recv，使用可重入锁避免死锁
        self._executor     = ThreadPoolExecutor(max_workers=max_workers)
        # 追踪未完成的请求 futures
        self._futures       = set()
        self._pending_lock  = threading.Lock()

        # --------------- 停止/重连标志 --------------
        self._stop_event  = threading.Event()

        # 预解析本机地址，避免重连时因 socket.gethostbyname 阻塞
        try:
            hostname = socket.gethostname()
            ip_address = socket.gethostbyname(hostname)
        except Exception:
            ip_address = "127.0.0.1"
        self._remote_addr = (
            f"{ip_address} [PID:{os.getpid()}, ID:{self._service_id.split('-')[-1]}]"
        )

        # -------------- 从类注册表克隆方法 ------------
        self._registry    = dict(self.__class__._registry)

        # 添加全局重连状态标记
        self._reconnect_in_progress = False

    # ---------------- 启动 / 停止 ----------------
    def stop(self):
        """同步停止服务：设置停止标志、发送下线通知并彻底清理资源"""
        # 避免重复调用
        if self._state in (DealerState.STOPPING, DealerState.STOPPED):
            return
        
        # 先标记状态，防止新任务提交
        self._state = DealerState.STOPPING
        self._stop_event.set()  # 通知所有线程退出
        
        # 关闭套接字
        if self._socket:
            try:
                # 尝试发送shutdown通知
                self._socket.send_multipart([b"shutdown", b""], flags=zmq.NOBLOCK)
                # 立即关闭socket，避免等待ACK
                self._socket.close(linger=0)
                self._socket = None
            except Exception:
                pass
        
        # 关闭线程池，防止新任务被接受
        try:
            # 先设置空队列，避免因有任务而阻塞
            self._executor._work_queue.queue.clear()
            self._executor.shutdown(wait=False)
        except:
            pass
        
        # 标记最终状态
        self._state = DealerState.STOPPED
        self._logger.info(f"<{self._service_id}> 服务已停止")

    def start(self) -> bool:
        """同步启动：尝试首次连接，但即使失败也会启动后台重连线程"""
        if self._state != DealerState.INIT:
            self._logger.warning("%s 无法从状态 %s 启动", self._service_id, self._state)
            return False

        # 尝试首次连接
        first_connect_success = True
        try:
            self._connect_and_register()
        except Exception as e:
            self._logger.error(f"<{self._service_id}> 首次连接/注册失败: {e}")
            first_connect_success = False
            # 但不立即退出，继续启动重连线程

        # 启动后台循环线程
        threading.Thread(target=self._message_loop, name=f"{self._service_id}-msg", daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, name=f"{self._service_id}-heartbeat", daemon=True).start()
        threading.Thread(target=self._reconnect_loop, name=f"{self._service_id}-reconnect", daemon=True).start()

        # 设置状态
        if first_connect_success:
            self._state = DealerState.RUNNING
            self._logger.info("%s DEALER 端已启动（同步）", self._service_id)
        else:
            self._state = DealerState.RECONNECTING
            self._logger.info("%s DEALER 端启动失败，转为自动重连模式", self._service_id)
            # 主动触发首次重连
            self._trigger_reconnect()

        # 返回初始连接状态，让命令行程序可以显示适当的消息
        return first_connect_success

    def _connect_and_register(self):
        """(Re)connect to Router 并发送 register 消息"""
        # 使用套接字锁保护整个操作，确保其他线程不能访问套接字
        with self._socket_lock:
            # 1) 关闭旧连接
            if self._socket:
                try:
                    self._socket.close(linger=0)
                    self._socket = None  # 明确置空
                except Exception:
                    self._logger.warning(f"<{self._service_id}> 关闭旧 socket 失败", exc_info=True)
                    self._socket = None  # 确保置空

            # 2) 创建新 DEALER socket
            sock = self._ctx.socket(zmq.DEALER)
            sock.identity = self._service_id.encode()
            sock.set_hwm(self._hwm)
            sock.setsockopt(zmq.LINGER, 0)
            sock.setsockopt(zmq.IMMEDIATE, 1)
            sock.setsockopt(zmq.RECONNECT_IVL, 100)  # 100ms的重连间隔
            sock.setsockopt(zmq.RECONNECT_IVL_MAX, 1000)  # 最大1秒

            # 2.1) 如果配置了 CURVE，启用加密
            if self._curve_server_key:
                try:
                    client_pub, client_secret = zmq.curve_keypair()
                    sock.curve_secretkey = client_secret
                    sock.curve_publickey = client_pub
                    sock.curve_serverkey = self._curve_server_key
                    self._logger.info(f"<{self._service_id}> CURVE 加密已启用，客户端公钥: {client_pub.hex()[:8]}...")
                except Exception as e:
                    self._logger.error(f"<{self._service_id}> CURVE 配置失败: {e}", exc_info=True)

            self._logger.info(f"<{self._service_id}> Attempting to connect to {self._router_address}...")
            sock.connect(self._router_address)

            # === 加入 Poller 来限制连接等待时间 ===
            poller = zmq.Poller()
            poller.register(sock, zmq.POLLOUT) # 等待 socket 变为可写
            connect_timeout_ms = 2000 # 设置连接超时，例如 2000ms = 2秒

            try:
                events = dict(poller.poll(connect_timeout_ms))
                if sock in events and events[sock] == zmq.POLLOUT:
                    self._logger.info(f"<{self._service_id}> Connection seemingly established within {connect_timeout_ms}ms timeout.")
                else:
                    # 连接超时
                    poller.unregister(sock) # 先取消注册
                    sock.close(linger=0)    # 关闭 socket
                    raise TimeoutError(f"Connection to {self._router_address} timed out after {connect_timeout_ms}ms")
            except Exception as e:
                 # 捕获原始异常（包括上面的TimeoutError）并确保清理
                 self._logger.error(f"<{self._service_id}> Error during connection polling/establishment: {e}")
                 try:
                     poller.unregister(sock)
                     sock.close(linger=0)
                 except Exception:
                     pass # 忽略清理错误
                 raise # 重新抛出原始异常
            finally:
                # 确保 Poller 被清理
                poller.unregister(sock)
            # === 连接超时检查结束 ===

            # 3) 构建 register payload
            remote_addr = self._remote_addr

            serializable_methods = {
                name: {
                    "description": info.get("description", ""),
                    "params":      info.get("params", {}),
                    "stream":      info.get("stream", False),
                    "metadata":    info.get("metadata", {}),
                }
                for name, info in self._registry.items()
            }
            service_info = {
                "group":       self._group,
                "methods":     serializable_methods,
                "api_key":     self._api_key,
                "remote_addr": remote_addr,
                "host_info": {
                    "hostname": socket.gethostname(),
                    "ip":       socket.gethostbyname(socket.gethostname()),
                    "pid":      os.getpid()
                }
            }
            self._logger.info(f"<{self._service_id}> 注册服务: 分组={self._group} 方法={list(serializable_methods.keys())}")

            # 4) 发送 register 消息
            try:
                sock.send_multipart([b"register", json.dumps(service_info).encode()])
                self._logger.info(f"<{self._service_id}> Register message sent.")
            except zmq.Again:
                sock.close(linger=0) # 关闭 socket 如果发送超时
                raise TimeoutError(f"Sending register message timed out for {self._service_id}")
            except Exception as e:
                self._logger.error(f"<{self._service_id}> 发送注册请求失败: {e}", exc_info=True)
                # 发送失败也需要关闭 socket
                try: sock.close(linger=0)
                except: pass
                raise # 重新抛出异常

            # 5) 更新实例状态
            self._socket = sock
            self._last_successful_heartbeat = time.time()
            
            # 重要：立即发送一次心跳，确保ROUTER能尽快识别服务
            try:
                self._send_heartbeat_internal(sock)
            except Exception as e:
                self._logger.warning(f"<{self._service_id}> 重连后初始心跳发送失败: {e}")
            
            self._diagnostics["connection_history"].append(self._last_successful_heartbeat)
            self._logger.info(f"<{self._service_id}> _connect_and_register completed successfully.")

    def _send_heartbeat_internal(self, sock):
        """内部方法：使用指定套接字发送心跳"""
        data = {
            "api_key": self._api_key,
            "pending_requests": len(self._futures),
            "is_busy": self._is_busy()
        }
        sock.send_multipart([b"heartbeat", json.dumps(data).encode()])
        self._heartbeat_sent_count += 1
        self._logger.debug(f"<{self._service_id}> 心跳已发送 #{self._heartbeat_sent_count}")

    # ---------------- 消息循环 ----------------
    def _message_loop(self):
        """消息循环：安全地处理套接字变更"""
        while not self._stop_event.is_set():
            try:
                # 每次循环都安全地获取当前套接字引用
                current_socket = None
                with self._socket_lock:
                    if self._socket:
                        current_socket = self._socket
                
                if not current_socket:
                    time.sleep(0.2)  # 如果没有有效套接字，简短等待
                    continue
                    
                # 使用临时变量，避免在等待过程中套接字被替换
                poller = zmq.Poller()
                poller.register(current_socket, zmq.POLLIN)
                socks = dict(poller.poll(500))  # 减少阻塞时间，更频繁检查套接字变更
                
                if socks.get(current_socket) == zmq.POLLIN:
                    with self._socket_lock:
                        if self._socket and self._socket == current_socket:
                            parts = self._socket.recv_multipart()
                            self._dispatch(parts)
                            
            except zmq.ZMQError as e:
                self._logger.warning(f"<{self._service_id}> 消息循环套接字错误: {e}，稍后重试")
                time.sleep(0.5)  # 减少出错后的等待时间
            except Exception as e:
                self._logger.error(f"<{self._service_id}> 消息循环异常: {e}", exc_info=True)
                time.sleep(0.5)
        
        self._logger.info("%s 消息线程退出", self._service_id)

    def _dispatch(self, parts):
        """根据消息类型分发，同时更新心跳与诊断统计"""
        now = time.time()
        self._last_successful_heartbeat = now
        # 统计接收消息次数并记录心跳历史
        self._diagnostics["received_messages"] += 1
        self._heartbeat_history.append({
            "time": now,
            "type": parts[0].decode(errors="ignore"),
            "status": True
        })
        if len(self._heartbeat_history) > 20:
            self._heartbeat_history = self._heartbeat_history[-20:]

        msg_type = parts[0]
        if msg_type == b"call_from_router":
            # 普通 RPC 调用 → Router 在第三帧插入 service_name，真正的 JSON 请求体在最后一帧
            if len(parts) < 3:
                self._logger.warning(f"<{self._service_id}> Invalid call frame, 帧数 {len(parts)}: {parts}")
                return
            client_id = parts[1]
            body = parts[-1]
            if not body:
                self._logger.warning(f"<{self._service_id}> 空请求体: {parts}")
                return
            try:
                req = json.loads(body.decode())
            except Exception as e:
                self._logger.error(f"<{self._service_id}> 请求 JSON 解析失败: {e}")
                return
            # 提交到线程池执行
            with self._pending_lock:
                fut = self._executor.submit(self._process_and_reply, client_id, req)
                self._futures.add(fut)
                fut.add_done_callback(self._remove_future)

        elif msg_type == b"heartbeat_ack":
            # 心跳确认
            self._heartbeat_ack_count += 1
            self._logger.debug(f"<{self._service_id}> 收到心跳确认 #{self._heartbeat_ack_count}")

        elif msg_type == b"register_ack":
            # 注册成功确认
            self._logger.info(f"<{self._service_id}> 服务注册成功")

        elif msg_type == b"error":
            # Router 返回错误
            err = parts[1].decode() if len(parts) > 1 else "Unknown error"
            self._logger.error(f"<{self._service_id}> 接收到错误消息: {err}")

        elif msg_type == b"router_shutdown":
            # Router 主动关闭
            self._logger.warning(f"<{self._service_id}> 收到 router_shutdown，准备重连")
            # 设置特殊标记，表示Router主动关闭
            self._router_shutdown = True
            self._trigger_reconnect()

        elif msg_type == b"reregister_required":
            self._logger.warning(f"<{self._service_id}> 收到需要重新注册的通知")
            self._trigger_reconnect()

        else:
            # 未知消息类型
            self._logger.warning(f"<{self._service_id}> 未知消息类型: {msg_type}")

    # ---------------- 处理 & 回复 ----------------
    def _process_and_reply(self, client_id: bytes, req: Dict[str, Any]):
        func_name  = req.get("func_name", "").split('.')[-1]
        request_id = req.get("request_id")
        info = self._registry.get(func_name)

        if not info:
            reply = {"type": "error", "error": f"方法 {func_name} 未注册"}
            self._send(client_id, reply)
            return

        handler      = info['handler']
        is_stream    = info['stream']
        is_coro      = info['is_coroutine']
        is_asyncgen  = info['is_asyncgen']
        is_generator = info['is_generator']

        # 流式
        if is_stream:
            try:
                if is_asyncgen:
                    # 正确的异步生成器处理
                    agen = handler(self, *req.get("args", []), **req.get("kwargs", {}))
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    async def collect_results():
                        results = []
                        async for item in agen:
                            results.append(item)
                        return results
                        
                    try:
                        # 获取所有结果后逐个发送
                        results = loop.run_until_complete(collect_results())
                        for chunk in results:
                            self._send(client_id, {"type":"streaming","request_id":request_id,"data":chunk})
                    finally:
                        loop.close()
                else:
                    # sync generator
                    try:
                        for chunk in handler(self, *req.get("args", []), **req.get("kwargs", {})):
                            self._send(client_id, {"type":"streaming","request_id":request_id,"data":chunk})
                    except Exception as e:
                        self._logger.error(f"同步生成器异常: {e}", exc_info=True)
                        self._send(client_id, {"type":"error","error": str(e)})
                        return  # 防止发送 end 标记
                
                # 发送结束标记
                self._send(client_id, {"type":"end","request_id":request_id})
            except Exception as e:
                # 完全异常处理
                self._logger.error("流方法处理异常: %s", e, exc_info=True)
                self._send(client_id, {"type":"error","error": str(e)})
            return

        # 非流式，单次调用
        try:
            if is_coro:
                result = asyncio.run(handler(self, *req.get("args", []), **req.get("kwargs", {})))
            else:
                result = handler(self, *req.get("args", []), **req.get("kwargs", {}))
                if isinstance(result, BaseModel):
                    result = result.model_dump()
            reply = {"type":"reply", "request_id":request_id, "result": result}
        except Exception as e:
            self._logger.error("方法执行异常: %s", e, exc_info=True)
            reply = {"type":"error", "error": str(e)}

        self._send(client_id, reply)

    def _send(self, client_id: bytes, message: Dict[str, Any]):
        """线程安全地发送 multipart"""
        with self._socket_lock:
            try:
                self._socket.send_multipart([
                    b"reply_from_dealer",
                    client_id,
                    json.dumps(message).encode()
                ])
            except Exception as e:
                self._logger.error("发送消息失败: %s", e)

    # -------------- pending futures 管理 ------------
    def _remove_future(self, fut):
        """当 future 完成时从 pending 集合移除"""
        with self._pending_lock:
            self._futures.discard(fut)

    def _is_busy(self) -> bool:
        """判断当前是否有未完成的请求"""
        with self._pending_lock:
            return bool(self._futures)

    # ---------------- 心跳线程 ----------------
    def _heartbeat_loop(self):
        # 使用本地变量控制间隔动态切换，首次立即发一次
        interval = self._idle_heartbeat_interval
        self._send_heartbeat()
        consecutive_missing_acks = 0

        while not self._stop_event.wait(interval):
            # 根据当前 pending 状态切换 busy/idle
            if self._is_busy():
                interval = self._busy_heartbeat_interval
                self._heartbeat_timeout = self._busy_heartbeat_timeout
                # 忙碌时不要过于积极地重连，重置计数
                consecutive_missing_acks = 0
            else:
                interval = self._idle_heartbeat_interval
                self._heartbeat_timeout = self._idle_heartbeat_timeout

            # 发送心跳
            current_ack_count = self._heartbeat_ack_count
            self._send_heartbeat()
            
            # 短暂等待确认(但不阻塞主循环)
            time.sleep(min(0.1, interval * 0.3))
            
            # 只在空闲时检查心跳确认
            if current_ack_count == self._heartbeat_ack_count and not self._is_busy():
                consecutive_missing_acks += 1
                if consecutive_missing_acks >= 3:
                    self._logger.warning(f"空闲状态下连续{consecutive_missing_acks}次心跳未收到确认，触发重连检查")
                    # 使用无等待的方式尝试获取锁
                    if self._socket_lock.acquire(blocking=False):
                        try:
                            self._trigger_reconnect()
                        finally:
                            self._socket_lock.release()
                    else:
                        self._logger.debug("无法立即获取锁进行重连，将稍后重试")
            else:
                # 重置计数
                consecutive_missing_acks = 0

    def _send_heartbeat(self):
        """线程安全地发送一次心跳，用于首次和需要时立即触发"""
        data = {
            "api_key":          self._api_key,
            "pending_requests": len(self._futures),
            "is_busy":          self._is_busy()
        }
        with self._socket_lock:
            try:
                self._socket.send_multipart([b"heartbeat", json.dumps(data).encode()])
                self._heartbeat_sent_count += 1
                self._logger.debug(f"<{self._service_id}> 心跳已发送 #{self._heartbeat_sent_count}")
            except Exception:
                pass

    # ---------------- 重连线程 ----------------
    def _reconnect_loop(self):
        """重连监控：捕获所有异常，确保线程永不退出。"""
        check_interval = 0.2
        while not self._stop_event.wait(check_interval):
            try:
                now = time.time()
                timeout = (self._busy_heartbeat_timeout if self._is_busy()
                           else self._idle_heartbeat_timeout)
                if now - self._last_successful_heartbeat > timeout * 0.8 and not self._disable_reconnect:
                    self._trigger_reconnect()
            except Exception as e:
                self._logger.error(f"<{self._service_id}> 重连监控异常: {e}", exc_info=True)

    def _trigger_reconnect(self):
        """线程安全触发一次重连"""
        with self._reconnect_lock:
            # 检查前置条件
            if (self._disable_reconnect or 
                self._state == DealerState.STOPPING or
                self._state == DealerState.STOPPED or
                self._reconnect_in_progress):
                return
            
            # 设置全局标志
            self._reconnect_in_progress = True
            self._state = DealerState.RECONNECTING
            self._consecutive_reconnects += 1
            self._logger.info(f"<{self._service_id}> 正在进行重连 #{self._consecutive_reconnects}")

            # 尝试重连
            try:
                self._connect_and_register()
                
                # 如果是Router主动关闭导致的重连，保持RECONNECTING状态
                if hasattr(self, '_router_shutdown') and self._router_shutdown:
                    self._logger.info(f"<{self._service_id}> 重连暂时成功，但保持重连状态直到确认Router可用")
                    # 安排下一次重连检查
                    self._schedule_next_reconnect(2.0)
                else:
                    self._state = DealerState.RUNNING
                    self._logger.info(f"<{self._service_id}> 重连成功")
                    self._consecutive_reconnects = 0
            except Exception as e:
                self._logger.error(f"<{self._service_id}> 重连失败: {e}")
                # 重连失败时，安排下一次重连
                self._schedule_next_reconnect(min(10.0, 1.0 * self._consecutive_reconnects))
            finally:
                # 无论结果如何，重置标志
                self._reconnect_in_progress = False
            
    def _schedule_next_reconnect(self, delay):
        """安排下一次重连尝试"""
        self._logger.info(f"<{self._service_id}> 安排 {delay:.1f} 秒后进行下一次重连")
        threading.Timer(delay, self._trigger_reconnect).start()
