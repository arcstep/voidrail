from .factory import create_app
from .client import CeleryClient
from .config import (
    get_config,
    get_worker_argv
)

# 移除默认app和全局task装饰器
# 保留实用工具函数

def start(app, argv=None):
    """帮助启动特定app的worker"""
    import os
    if not os.environ.get('OBJC_DISABLE_INITIALIZE_FORK_SAFETY'):
        os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'
    
    if argv is None:
        argv = get_worker_argv()
    
    app.worker_main(argv)

# 导出所有公共API
__all__ = ["CeleryClient", "create_app", "get_config", "get_worker_argv", "start_worker"]
