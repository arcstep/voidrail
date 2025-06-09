import os

def get_config():
    """获取统一的配置项"""
    default_broker_url = 'redis://localhost:6379/0'
    default_result_backend = default_broker_url
    return {
        'broker_url': os.environ.get('CELERY_BROKER_URL', default_broker_url),
        'result_backend': os.environ.get('CELERY_RESULT_BACKEND', default_result_backend),
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
