import os
import inspect
import logging
from celery import Celery
from typing import Optional, Dict, Any, List, Type

class CeleryWorker:
    """
    Celery Worker基类，提供通用的Worker框架
    
    子类只需继承此类并使用self.celery_app.task装饰器来定义任务
    """
    
    def __init__(self, 
                 service_name: Optional[str] = None,
                 broker_url: Optional[str] = None,
                 backend_url: Optional[str] = None):
        """
        初始化Worker基类
        
        参数:
            service_name: 服务名称，默认使用类名
            broker_url: 消息代理URL，默认从环境变量CELERY_BROKER_URL获取
            backend_url: 结果后端URL，默认从环境变量CELERY_RESULT_BACKEND获取
        """
        # 设置服务名称
        self.service_name = service_name or self.__class__.__name__.lower()
        
        # 从环境变量获取配置
        self.broker_url = broker_url or os.environ.get(
            'CELERY_BROKER_URL', 'redis://localhost:6379/0')
        self.backend_url = backend_url or os.environ.get(
            'CELERY_RESULT_BACKEND', self.broker_url)
        
        # 设置日志
        self.logger = logging.getLogger(self.service_name)
        
        # 创建Celery应用
        self.celery_app = self._create_celery_app()
        
        # 自动注册任务
        self._register_tasks()
    
    def _create_celery_app(self) -> Celery:
        """创建并配置Celery应用"""
        app = Celery(self.service_name)
        
        # 基础配置
        app.conf.update(
            broker_url=self.broker_url,
            result_backend=self.backend_url,
            task_serializer='json',
            accept_content=['json'],
            result_serializer='json',
            worker_concurrency=int(os.environ.get('CELERY_CONCURRENCY', 4)),
            task_time_limit=int(os.environ.get('CELERY_TASK_TIME_LIMIT', 3600)),
            task_soft_time_limit=int(os.environ.get('CELERY_TASK_SOFT_TIME_LIMIT', 3000)),
            worker_prefetch_multiplier=1,
            task_acks_late=True,
            task_track_started=True,
            result_expires=86400,  # 1天
        )
        
        return app
    
    def _register_tasks(self):
        """自动注册子类中的任务方法"""
        # 此方法为空，因为任务会通过celery_app.task装饰器自动注册
        pass
    
    def get_registered_tasks(self) -> List[str]:
        """获取所有已注册的任务名称"""
        # 排除Celery内部任务
        return [
            task for task in self.celery_app.tasks.keys()
            if not task.startswith('celery.')
        ]
    
    def start_worker(self, argv: Optional[List[str]] = None):
        """启动Worker进程"""
        if argv is None:
            argv = [
                'worker',
                f'--loglevel={os.environ.get("CELERY_LOG_LEVEL", "info")}',
                f'--concurrency={os.environ.get("CELERY_CONCURRENCY", "4")}',
                f'--pool={os.environ.get("CELERY_POOL", "solo")}'
            ]
        
        # 设置macOS兼容性
        if not os.environ.get('OBJC_DISABLE_INITIALIZE_FORK_SAFETY'):
            os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'
        
        self.logger.info(f"启动 {self.service_name} worker...")
        self.celery_app.worker_main(argv)
    
    @classmethod
    def get_instance(cls, *args, **kwargs) -> 'CeleryWorkerBase':
        """获取Worker单例"""
        if not hasattr(cls, '_instance'):
            cls._instance = cls(*args, **kwargs)
        return cls._instance
