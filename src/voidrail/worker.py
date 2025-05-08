import os
import logging
from typing import Optional, Dict, Any, List
from celery import Celery

# 导入共享配置
from voidrail.config import get_config, get_worker_argv, create_celery_app, default_app, task

class CeleryWorker:
    """Celery Worker基类，提供通用的Worker框架"""
    
    def __init__(self, 
                service_name: Optional[str] = None,
                broker_url: Optional[str] = None,
                backend_url: Optional[str] = None,
                use_default_app: bool = True):
        """初始化Worker基类"""
        # 设置服务名称
        self.service_name = service_name or self.__class__.__name__.lower()
        
        # 获取统一配置
        config = get_config()
        
        # 从参数或配置获取连接信息
        self.broker_url = broker_url or config['broker_url']
        self.backend_url = backend_url or config['result_backend']
        
        # 设置日志
        self.logger = logging.getLogger(self.service_name)
        
        # 使用默认app或创建新app
        if use_default_app and self.broker_url == default_app.conf.broker_url and self.backend_url == default_app.conf.result_backend:
            self.logger.info(f"服务 {self.service_name} 使用全局默认Celery应用")
            self.celery_app = default_app
        else:
            self.logger.info(f"服务 {self.service_name} 创建独立Celery应用")
            self.celery_app = create_celery_app(
                self.service_name,
                {'broker_url': self.broker_url, 'result_backend': self.backend_url}
            )
    
    def get_registered_tasks(self) -> List[str]:
        """获取所有已注册的任务名称"""
        return [
            task for task in self.celery_app.tasks.keys()
            if not task.startswith('celery.')
        ]
    
    def start_worker(self, argv: Optional[List[str]] = None):
        """启动Worker进程处理当前app的任务"""
        if argv is None:
            argv = get_worker_argv()
        
        # 设置macOS兼容性
        if not os.environ.get('OBJC_DISABLE_INITIALIZE_FORK_SAFETY'):
            os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'
        
        self.logger.info(f"启动 {self.service_name} worker...")
        self.celery_app.worker_main(argv)
    
    @classmethod
    def get_instance(cls, *args, **kwargs) -> 'CeleryWorker':
        """获取Worker单例"""
        if not hasattr(cls, '_instance'):
            cls._instance = cls(*args, **kwargs)
        return cls._instance
