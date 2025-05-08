import os
from typing import Dict, Any, List, Optional
from celery import Celery

def get_config():
    """获取统一的配置项"""
    return {
        'broker_url': os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0'),
        'result_backend': os.environ.get('CELERY_RESULT_BACKEND', 
                          os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')),
        'worker_concurrency': int(os.environ.get('CELERY_CONCURRENCY', 4)),
        'task_time_limit': int(os.environ.get('CELERY_TASK_TIME_LIMIT', 3600)),
        'task_soft_time_limit': int(os.environ.get('CELERY_TASK_SOFT_TIME_LIMIT', 3000)),
        'log_level': os.environ.get('CELERY_LOG_LEVEL', 'info'),
        'pool': os.environ.get('CELERY_POOL', 'solo'),
        'result_expires': int(os.environ.get('CELERY_RESULT_EXPIRES', 86400)),
    }

def get_worker_argv():
    """获取worker启动参数"""
    config = get_config()
    return [
        'worker',
        f'--loglevel={config["log_level"]}',
        f'--concurrency={config["worker_concurrency"]}',
        f'--pool={config["pool"]}'
    ]

def create_celery_app(name: str, custom_config: Optional[Dict[str, Any]] = None) -> Celery:
    """
    创建并配置Celery应用
    
    参数:
        name: 应用名称
        custom_config: 自定义配置项，覆盖默认配置
    
    返回:
        配置好的Celery应用实例
    """
    config = get_config()
    
    # 合并自定义配置
    if custom_config:
        for key, value in custom_config.items():
            config[key] = value
    
    app = Celery(name)
    app.conf.update(
        broker_url=config['broker_url'],
        result_backend=config['result_backend'],
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        worker_concurrency=config['worker_concurrency'],
        task_time_limit=config['task_time_limit'],
        task_soft_time_limit=config['task_soft_time_limit'],
        worker_prefetch_multiplier=1,
        task_acks_late=True,
        task_track_started=True,
        result_expires=config['result_expires'],
    )
    
    return app

# 创建默认的全局Celery应用
default_app = create_celery_app('voidrail')

# 提供简化的任务装饰器
def task(*args, **kwargs):
    """简化的任务装饰器，使用默认的全局Celery应用"""
    return default_app.task(*args, **kwargs)
