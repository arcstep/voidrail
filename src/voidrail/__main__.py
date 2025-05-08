import os
import sys
import click
import logging

from voidrail import CeleryClient, start, create_app, get_config

from .example.hello_service import app

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("voidrail")

@click.group()
def cli():
    """VoidRail分布式任务处理框架命令行工具"""
    pass

@cli.command("worker")
def start_celery_worker():
    """启动Worker服务"""

    # 显示配置信息
    config = get_config()
    click.echo(f"Broker URL: {config['broker_url']}")
    click.echo(f"Result Backend: {config['result_backend']}")
    click.echo(f"Concurrency: {config['worker_concurrency']}")
    
    # 显示已注册的任务
    tasks = [t for t in app.tasks.keys() if not t.startswith('celery.')]
    if tasks:
        click.echo("已注册任务:")
        for task_name in tasks:
            click.echo(f"  - {task_name}")
    else:
        click.echo("没有找到注册的任务")
    
    click.echo("启动Worker...")
    start(app)

@cli.command("call")
@click.argument("task_name", required=True)
@click.option("--args", "-a", multiple=True, help="位置参数，可多次使用")
@click.option("--kwargs", "-k", multiple=True, help="关键字参数，格式为key=value")
@click.option("--service", "-s", default="default", help="服务名称")
@click.option("--wait/--no-wait", default=True, help="是否等待结果")
@click.option("--timeout", default=60, type=int, help="等待超时时间(秒)")
def call_task(task_name, args, kwargs, service, wait, timeout):
    """
    调用指定的任务
    
    TASK_NAME: 要调用的任务名称
    """
    # 创建客户端
    client = CeleryClient(service_name=service)
    
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
    
    # 调用任务
    result = client.call(
        task_name=task_name,
        args=args,
        kwargs=kwargs_dict,
        wait_result=wait,
        timeout=timeout
    )
    
    # 处理结果
    if wait:
        if result["status"] == "completed":
            click.echo("任务完成!")
            click.echo(f"结果: {_preview(result['result'])}")
        else:
            click.echo(f"任务失败: {result.get('error', '未知错误')}", err=True)
    else:
        click.echo(f"任务已提交，ID: {result['task_id']}")
        click.echo("使用以下命令查看任务状态:")
        click.echo(f"  python -m voidrail status {result['task_id']} -s {service}")

@cli.command("status")
@click.argument("task_id")
@click.option("--service", "-s", default="default", help="服务名称")
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
@click.option("--service", "-s", default="default", help="服务名称")
def list_tasks(service):
    """列出服务中的可用任务"""
    # 创建客户端
    client = CeleryClient(service_name=service)
    
    try:
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
def show_info():
    """显示VoidRail配置信息"""
    config = get_config()
    click.echo("VoidRail配置信息:")
    for key, value in config.items():
        click.echo(f"  {key}: {value}")
    
    # 显示已注册的任务
    tasks = [t for t in app.tasks.keys() if not t.startswith('celery.')]
    if tasks:
        click.echo("\n已注册任务:")
        for task_name in tasks:
            click.echo(f"  - {task_name}")

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
    cli()
