from .factory import create_app
from .client import CeleryClient
from .config import (
    get_config,
    get_worker_argv
)

# 移除默认app和全局task装饰器
# 保留实用工具函数

def start(app, argv=None, worker_name=None):
    """启动worker"""
    import os
    import uuid
    import socket
    
    if not os.environ.get('OBJC_DISABLE_INITIALIZE_FORK_SAFETY'):
        os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'
    
    if argv is None:
        argv = get_worker_argv()
    
    # 获取Celery应用的名称
    app_name = app.main
    
    # 确定最终的worker节点名称：优先使用user提供，否则使用app.main@hostname
    if worker_name:
        hostname = worker_name
    else:
        hostname = f"{app.main}@{socket.gethostname().split('.')[0]}"
    argv.extend(['--hostname', hostname])
    
    # 输出明确的启动信息
    print(f"启动 Celery 应用: {app_name}")
    print(f"Worker 节点名称: {worker_name}")
    
    # 设置进程名称 (如果可能)
    try:
        import setproctitle
        setproctitle.setproctitle(f"{app_name}_worker")
    except ImportError:
        pass
    
    # 启动worker
    app.worker_main(argv)

# 导出所有公共API
__all__ = ["CeleryClient", "create_app", "get_config", "get_worker_argv", "start_worker"]
