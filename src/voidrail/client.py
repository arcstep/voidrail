import os
from typing import Dict, Any, Optional, List, Union
from celery import Celery
from celery.result import AsyncResult

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
            broker_url: 消息代理URL，默认从环境变量CELERY_BROKER_URL获取
            backend_url: 结果后端URL，默认从环境变量CELERY_RESULT_BACKEND获取
        """
        self.service_name = service_name
        
        # 从环境变量获取配置
        self.broker_url = broker_url or os.environ.get(
            'CELERY_BROKER_URL', 'redis://localhost:6379/0')
        self.backend_url = backend_url or os.environ.get(
            'CELERY_RESULT_BACKEND', self.broker_url)
        
        # 创建轻量级Celery应用
        self.app = Celery(f'{service_name}_client',
                        broker=self.broker_url,
                        backend=self.backend_url)
        
        # 最小化配置
        self.app.conf.update(
            task_serializer='json',
            accept_content=['json'],
            result_serializer='json',
            result_expires=86400,
        )
    
    def send_task(self, 
                 task_name: str, 
                 args: Optional[List] = None, 
                 kwargs: Optional[Dict] = None,
                 wait_result: bool = False,
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
        
        # 发送任务
        task = self.app.send_task(
            full_task_name,
            args=args or [],
            kwargs=kwargs or {}
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
    
    def list_registered_tasks(self) -> List[str]:
        """列出服务端所有已注册的任务"""
        try:
            inspection = self.app.control.inspect()
            registered = inspection.registered() or {}
            
            # 提取所有任务名称
            tasks = []
            for worker_tasks in registered.values():
                tasks.extend(worker_tasks)
            
            # 过滤掉Celery内部任务
            return [task for task in tasks if not task.startswith('celery.')]
        except Exception as e:
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
