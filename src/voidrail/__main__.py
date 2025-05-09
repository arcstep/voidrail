import os
import sys
import click
import logging
import importlib

from voidrail import CeleryClient, start, create_app, get_config

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("voidrail")

def load_app_from_module(module_path):
    """动态加载指定模块中的app对象"""
    try:
        module = importlib.import_module(module_path)
        if not hasattr(module, 'app'):
            raise AttributeError(f"模块 {module_path} 没有导出 'app' 对象")
        return module.app
    except ImportError:
        click.echo(f"错误: 无法导入模块 {module_path}", err=True)
        sys.exit(1)
    except AttributeError as e:
        click.echo(f"错误: {str(e)}", err=True)
        click.echo(f"确保模块 {module_path} 包含一个名为 'app' 的Celery应用实例", err=True)
        sys.exit(1)

@click.group(invoke_without_command=True)
@click.option("--module", "-m", default='voidrail.echo', help="要加载的模块路径，例如'myproject.tasks'")
@click.pass_context
def cli(ctx, module):
    """VoidRail分布式任务处理框架命令行工具"""
    # 保存module参数以供后续子命令使用
    ctx.ensure_object(dict)
    ctx.obj['module'] = module
    
    # 修改点2: 当没有指定子命令时，默认执行worker命令
    if ctx.invoked_subcommand is None:
        ctx.invoke(start_celery_worker)

@cli.command("worker")
@click.option("--module", "-m", default=None, help="要加载的模块路径，例如'myproject.tasks'")
@click.option("--app-name", default=None, help="Celery应用名称，覆盖模块中定义的名称")
@click.option("--node-name", "-n", default=None, help="Worker节点名称，默认基于应用名称自动生成")
@click.pass_context
def start_celery_worker(ctx, module, app_name, node_name):
    """启动Worker服务"""
    # 加载模块
    module_path = module or ctx.obj.get('module', 'voidrail.echo')
    app = load_app_from_module(module_path)
    
    # 如果指定了应用名称，则覆盖模块中的定义
    if app_name:
        app.main = app_name
        print(f"覆盖应用名称为: {app_name}")
    
    # 显示配置信息（包括服务独立队列）
    config = get_config()
    click.echo(f"Broker URL: {config['broker_url']}")
    click.echo(f"Result Backend: {config['result_backend']}")
    click.echo(f"Concurrency: {config['worker_concurrency']}")
    # 显示本服务使用的队列
    queue_name = app.conf.task_default_queue
    click.echo(f"使用队列: {queue_name}")

    # 显示将使用的节点名称
    hostname_arg = node_name or app.conf.worker_hostname_format
    click.echo(f"节点名称模式: {hostname_arg}")
    
    # 显示已注册的任务
    tasks = [t for t in app.tasks.keys() if not t.startswith('celery.')]
    if tasks:
        click.echo(f"已注册任务 (来自模块 {module_path}):")
        for task_name in tasks:
            click.echo(f"  - {task_name}")
    else:
        click.echo("没有找到注册的任务")
    
    # 启动worker；若node_name=None，则start()会自动生成 app.main@hostname
    start(app, worker_name=node_name)

@cli.command("call")
@click.argument("task_name", required=True)
@click.option("--args", "-a", multiple=True, help="位置参数，可多次使用")
@click.option("--kwargs", "-k", multiple=True, help="关键字参数，格式为key=value")
@click.option("--service", "-s", default=None, help="服务名称，如果不指定则从任务名前缀推断")
@click.option("--wait/--no-wait", default=True, help="是否等待结果")
@click.option("--timeout", default=60, type=int, help="等待超时时间(秒)")
def call_task(task_name, args, kwargs, service, wait, timeout):
    """
    调用指定的任务
    
    TASK_NAME: 要调用的任务名称
    """
    # 如果未指定service，则尝试从task_name前缀推断
    if not service and "." in task_name:
        inferred = task_name.split(".", 1)[0]
        click.echo(f"未指定服务，已从任务名推断 service='{inferred}'")
        service = inferred
    # 创建客户端
    client = CeleryClient(service_name=service or "")
    
    # 处理关键字参数
    kwargs_dict = {}
    for kv in kwargs:
        if "=" in kv:
            key, value = kv.split("=", 1)
            # 尝试转换数值类型
            try:
                if value.isdigit():
                    value = int(value)
                elif value.replace(".", "", 1).isdigit() and value.count(".") <= 1:
                    value = float(value)
            except:
                pass
            kwargs_dict[key] = value
    
    # 显示调用信息
    click.echo(f"调用任务: {task_name}")
    if args:
        click.echo(f"位置参数: {args}")
    if kwargs_dict:
        click.echo(f"关键字参数: {kwargs_dict}")
    
    # 客户端默认队列
    queue = client.app.conf.task_default_queue or service
    click.echo(f"\n→ 发送到队列 '{queue}' 上的任务 '{task_name}'")
    
    try:
        result = client.call(
            task_name=task_name,
            args=args,
            kwargs=kwargs_dict,
            wait_result=wait,
            timeout=timeout
        )
    except Exception as e:
        click.echo(f"调用异常: {str(e)}", err=True)
        sys.exit(1)
    
    # 处理结果
    if wait and result.get("status") == "completed":
        click.echo("任务完成!")
        click.echo(f"结果: \n{result['result']}")
    elif wait and result.get("status") == "error":
        click.echo(f"任务执行错误: {result.get('error')}", err=True)
    else:
        click.echo(f"任务已提交，ID: {result['task_id']}")
        click.echo("使用以下命令查看任务状态:")
        click.echo(f"  python -m voidrail status {result['task_id']} -s {service}")

@cli.command("status")
@click.argument("task_id")
@click.option("--service", "-s", default="echo", help="服务名称")
@click.option("--wait/--no-wait", default=False, help="是否等待任务完成")
def check_status(task_id, service, wait):
    """查询任务状态"""
    # 创建客户端
    client = CeleryClient(service_name=service)
    
    if wait:
        # 等待任务完成
        click.echo(f"等待任务 {task_id} 完成...")
        try:
            result = client.get_task_result(task_id)
            click.echo("任务已完成!")
            click.echo(f"结果: {_preview(result)}")
        except Exception as e:
            click.echo(f"获取任务结果失败: {str(e)}", err=True)
    else:
        # 获取当前状态
        status = client.get_task_status(task_id)
        click.echo(f"任务ID: {task_id}")
        click.echo(f"状态: {status['status']}")
        
        if status['status'] == 'processing' and 'info' in status:
            click.echo(f"进度信息: {status['info']}")
        elif status['status'] == 'completed':
            click.echo("任务已完成")
            if 'result' in status:
                click.echo(f"结果: {_preview(status['result'])}")
        elif status['status'] == 'failed':
            click.echo(f"错误信息: {status.get('error', '未知错误')}")

@cli.command("list")
@click.option("--service", "-s", default="echo", help="服务名称")
@click.option("--group-by-worker/--no-group", default=True, help="是否按worker分组显示任务")
@click.option("--detailed/--no-detailed", default=False, help="是否显示详细信息")
def list_tasks(service, group_by_worker, detailed):
    """列出服务中的可用任务"""
    client = CeleryClient(service_name=service)
    
    try:
        # 获取worker状态信息
        stats = client.app.control.inspect().stats() or {}
        if detailed and stats:
            click.echo("Worker信息详情:")
            for worker_name, info in stats.items():
                click.echo(f"\n• Worker: {worker_name}")
                click.echo(f"  - Celery版本: {info.get('sw_ver', 'unknown')}")
                click.echo(f"  - 应用名称: {info.get('sw_sys', 'unknown')}")  # 这通常显示Python版本，但可能包含应用信息
                click.echo(f"  - 主机名: {info.get('hostname', 'unknown')}")
                click.echo(f"  - 进程ID: {info.get('pid', 'unknown')}")
        
        if group_by_worker:
            # 获取按worker分组的任务
            tasks_by_worker = client.list_registered_tasks(include_workers=True)
            
            if not tasks_by_worker:
                click.echo("没有找到可用任务，请确保服务已启动")
                return
                
            click.echo("按Worker分组的可用任务:")
            
            # 计算任务总数
            total_tasks = set()
            for worker_tasks in tasks_by_worker.values():
                total_tasks.update(worker_tasks)
            
            # 显示摘要信息
            click.echo(f"发现 {len(tasks_by_worker)} 个worker, {len(total_tasks)} 个唯一任务")
            
            # 显示详细信息
            for worker, tasks in tasks_by_worker.items():
                # 提取worker名称的核心部分
                worker_name = worker.split('@')[0] if '@' in worker else worker
                worker_host = worker.split('@')[1] if '@' in worker else 'unknown'
                
                click.echo(f"\n• Worker: {worker_name} (主机: {worker_host})")
                for task in tasks:
                    click.echo(f"  - {task}")
        else:
            # 旧的行为：只显示任务列表
            tasks = client.list_registered_tasks()
            if tasks:
                click.echo("可用任务:")
                for task in tasks:
                    click.echo(f"  - {task}")
            else:
                click.echo("没有找到可用任务，请确保服务已启动")
    except Exception as e:
        click.echo(f"获取任务列表失败: {str(e)}", err=True)

@cli.command("info")
@click.option("--module", "-m", default=None, help="要加载的模块路径，例如'myproject.tasks'")
@click.pass_context
def show_info(ctx, module):
    """显示指定模块的配置信息"""
    # 使用命令行参数或从父命令获取模块路径
    module_path = module or ctx.obj.get('module', 'voidrail.echo')
    
    # 加载app
    app = load_app_from_module(module_path)
    
    config = get_config()
    click.echo("VoidRail配置信息:")
    for key, value in config.items():
        click.echo(f"  {key}: {value}")
    
    # 显示已注册的任务
    tasks = [t for t in app.tasks.keys() if not t.startswith('celery.')]
    if tasks:
        click.echo(f"\n已注册任务 (来自模块 {module_path}):")
        for task_name in tasks:
            click.echo(f"  - {task_name}")

@cli.command("diagnose")
def diagnose_environment():
    """显示详细的环境和配置诊断信息"""
    import sys
    import celery
    
    click.echo("VoidRail诊断信息:")
    click.echo(f"Python版本: {sys.version}")
    click.echo(f"Celery版本: {celery.__version__}")
    
    # 导入临时应用进行测试
    test_app = create_app("test_diagnose")
    click.echo(f"\nCelery应用配置:")
    click.echo(f"应用名称 (main): {test_app.main}")
    click.echo(f"应用配置:")
    for key in ['broker_url', 'result_backend', 'worker_proc_name', 'worker_hostname_format']:
        value = test_app.conf.get(key, "未设置")
        click.echo(f"  {key}: {value}")

# 添加一个辅助函数来生成内容摘要
def _preview(content, max_length=100):
    """生成内容的简短预览"""
    if not content:
        return ""
    
    # 转换为字符串
    if not isinstance(content, str):
        content = str(content)
        
    if len(content) <= max_length:
        return content
    return content[:max_length] + "... [内容已截断]"

if __name__ == "__main__":
    # 修改点3: 确保传递对象字典
    cli(obj={})
