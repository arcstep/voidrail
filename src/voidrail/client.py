import os
from typing import Dict, Any, Optional, List, Union
from celery import Celery
from celery.result import AsyncResult

# 导入共享配置
from .config import get_config
from .factory import create_app

class CeleryClient:
    """
    Celery客户端基类，提供通用的任务调用接口
    """
    
    def __init__(self, 
                service_name: str,
                broker_url: Optional[str] = None,
                backend_url: Optional[str] = None):
        """
        初始化客户端基类
        
        参数:
            service_name: 服务名称
            broker_url: 消息代理URL，默认从配置获取
            backend_url: 结果后端URL，默认从配置获取
        """
        self.service_name = service_name
        
        # 获取统一配置
        config = get_config()
        
        # 从参数或配置获取连接信息
        self.broker_url = broker_url or config['broker_url']
        self.backend_url = backend_url or config['result_backend']
        
        # 客户端与Worker使用相同的service_name作为app名称，以保证路由到正确队列
        self.app = create_app(
            service_name,
            {'broker_url': self.broker_url, 'result_backend': self.backend_url}
        )
    
    def call(self, 
                task_name: str, 
                args: Optional[List] = None, 
                kwargs: Optional[Dict] = None,
                wait_result: bool = True,
                timeout: int = 60) -> Dict[str, Any]:
        """
        发送任务到服务端
        
        参数:
            task_name: 任务名称，可以是短名称(自动添加service_name前缀)或完整名称
            args: 位置参数
            kwargs: 关键字参数
            wait_result: 是否等待结果
            timeout: 等待超时时间(秒)
        
        返回:
            包含任务状态和ID的字典，如果wait_result为True则包含结果
        """
        # 如果任务名称不包含点，则添加服务名称前缀
        if '.' not in task_name:
            full_task_name = f'{self.service_name}.{task_name}'
        else:
            full_task_name = task_name
        
        # 指定发送到与service_name同名的队列
        task = self.app.send_task(
            full_task_name,
            args=args or [],
            kwargs=kwargs or {},
            queue=self.service_name
        )
        
        if wait_result:
            try:
                result = task.get(timeout=timeout)
                return {
                    "status": "completed",
                    "task_id": task.id,
                    "result": result
                }
            except Exception as e:
                return {
                    "status": "error",
                    "task_id": task.id,
                    "error": str(e)
                }
        else:
            return {
                "status": "submitted",
                "task_id": task.id
            }
    
    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """获取任务状态"""
        task = AsyncResult(task_id, app=self.app)
        
        if task.state == 'PENDING':
            return {"status": "pending", "task_id": task_id}
        elif task.state in ('STARTED', 'PROGRESS'):
            return {
                "status": "processing", 
                "task_id": task_id,
                "info": task.info
            }
        elif task.state == 'SUCCESS':
            return {
                "status": "completed",
                "task_id": task_id,
                "result": task.result
            }
        else:
            return {
                "status": "failed",
                "task_id": task_id,
                "error": str(task.result) if task.result else "Unknown error"
            }
    
    def get_task_result(self, task_id: str, timeout: Optional[int] = None) -> Any:
        """获取任务结果"""
        task = AsyncResult(task_id, app=self.app)
        
        if timeout is not None:
            return task.get(timeout=timeout)
        
        if task.ready():
            return task.result
        return None
    
    def list_registered_tasks(self, include_workers=False) -> Union[List[str], Dict[str, List[str]]]:
        """
        列出服务端所有已注册的任务
        
        参数:
            include_workers: 是否包含worker信息
            
        返回:
            如果include_workers为False，返回任务列表
            如果include_workers为True，返回{worker名称: [任务列表]}字典
        """
        try:
            inspection = self.app.control.inspect()
            registered = inspection.registered() or {}
            
            if not include_workers:
                # 旧行为：返回所有任务的列表（去重）
                tasks_set = set()
                for worker_tasks in registered.values():
                    tasks_set.update(worker_tasks)
                return sorted([task for task in tasks_set if not task.startswith('celery.')])
            else:
                # 新行为：返回带worker信息的字典
                result = {}
                for worker_name, worker_tasks in registered.items():
                    # 过滤掉Celery内部任务
                    filtered_tasks = [task for task in worker_tasks if not task.startswith('celery.')]
                    if filtered_tasks:  # 只包含有任务的worker
                        result[worker_name] = sorted(filtered_tasks)
                return result
        except Exception as e:
            if include_workers:
                return {"error": [str(e)]}
            return [f"Error listing tasks: {str(e)}"]
    
    def get_worker_stats(self) -> Dict[str, Any]:
        """获取Worker统计信息"""
        try:
            inspection = self.app.control.inspect()
            stats = inspection.stats() or {}
            active = inspection.active() or {}
            scheduled = inspection.scheduled() or {}
            reserved = inspection.reserved() or {}
            
            return {
                "workers": list(stats.keys()),
                "stats": stats,
                "active_tasks": active,
                "scheduled_tasks": scheduled,
                "reserved_tasks": reserved
            }
        except Exception as e:
            return {"error": str(e)}
