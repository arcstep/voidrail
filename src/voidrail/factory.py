from typing import Dict, Any, Optional
from celery import Celery

# 导入共享配置
from .config import get_config

def create_app(app_name: str, custom_config: Optional[Dict[str, Any]] = None) -> Celery:
    """
    创建并配置Celery服务
    
    参数:
        service_name: 服务名称
        custom_config: 自定义配置项，覆盖默认配置
    
    返回:
        配置好的Celery服务实例
    """
    config = get_config()
    
    # 合并自定义配置
    if custom_config:
        for key, value in custom_config.items():
            config[key] = value
    
    app = Celery(app_name)
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
