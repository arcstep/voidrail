from typing import Dict, Any, Optional
from celery import Celery
from kombu import Queue

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
    
    # 确保app_name不为空
    if not app_name:
        raise ValueError("app_name不能为空")
        
    # 创建应用实例
    app = Celery(app_name)
    
    # 明确设置main属性
    app.conf.main = app_name
    
    # 设置worker节点名称格式
    app.conf.worker_proc_name = f"{app_name}-%n"
    app.conf.worker_hostname_format = f"{app_name}-%n@%h"
    
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
    
    # ===== 新增：每个服务用自己的队列，队列名与app_name保持一致 =====
    queue_name = app_name
    app.conf.task_default_queue = queue_name
    app.conf.task_queues = (Queue(queue_name, routing_key=queue_name),)
    app.conf.task_routes = {
        f'{app_name}.*': {'queue': queue_name, 'routing_key': queue_name}
    }
    
    return app
