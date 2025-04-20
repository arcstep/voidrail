import click
import asyncio
import importlib
import sys
import json
import logging
from typing import List, Optional
from pathlib import Path

from .router import ServiceRouter, RouterMode
from .dealer import ServiceDealer
from .client import ClientDealer
from .api_key import ApiKeyManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

@click.group()
@click.option('--debug', is_flag=True, help='启用调试模式')
def cli(debug):
    """VoidRail 命令行工具 - 基于ZeroMQ的轻量级微服务框架"""
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)

@cli.command()
@click.option('--host', '-h', default='127.0.0.1', help='路由器监听地址')
@click.option('--port', '-p', default=5555, help='路由器监听端口')
@click.option('--mode', type=click.Choice(['fifo', 'load_balance']), default='fifo', help='路由器分发模式')
@click.option('--heartbeat', default=30.0, help='心跳超时时间（秒）')
@click.option('--require-auth/--no-auth', default=False, help='是否启用认证')
@click.option('--dealer-keys', multiple=True, help='允许的服务端API密钥 (可指定多次)')
@click.option('--client-keys', multiple=True, help='允许的客户端API密钥 (可指定多次)')
@click.option('--generate-keys', is_flag=True, help='生成并显示新的API密钥')
@click.option('--logger-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']), default='INFO', help='日志级别')
def router(host, port, mode, heartbeat, require_auth, dealer_keys, client_keys, generate_keys, logger_level):
    """启动VoidRail Router服务"""
    if generate_keys:
        dealer_key = ApiKeyManager.generate_key(prefix="dealer")
        client_key = ApiKeyManager.generate_key(prefix="client")
        click.echo(f"生成的服务端密钥: {dealer_key}")
        click.echo(f"生成的客户端密钥: {client_key}")
        click.echo("\n可以使用以下命令启动认证Router:")
        cmd = f"voidrail router --host {host} --port {port} --require-auth --dealer-keys {dealer_key} --client-keys {client_key}"
        click.echo(f"  {cmd}")
        return

    address = f"tcp://{host}:{port}"
    router_mode = RouterMode.FIFO if mode == 'fifo' else RouterMode.LOAD_BALANCE
    
    async def start_router():
        router = ServiceRouter(
            address=address,
            router_mode=router_mode,
            heartbeat_timeout=heartbeat,
            require_auth=require_auth,
            dealer_api_keys=list(dealer_keys) if dealer_keys else None,
            client_api_keys=list(client_keys) if client_keys else None,
            logger_level=logger_level
        )
        await router.start()
        click.echo(f"Router 已启动: {address}") 
        click.echo(f"模式: {mode}")
        click.echo(f"认证: {'已启用' if require_auth else '未启用'}")
        
        try:
            # 保持运行直到中断
            while True:
                await asyncio.sleep(0.5)
        except KeyboardInterrupt:
            click.echo("正在关闭Router...")
        finally:
            await router.stop()
            click.echo("Router已停止")
    
    try:
        asyncio.run(start_router())
    except KeyboardInterrupt:
        click.echo("已中断Router服务")

@cli.command()
@click.option('--host', '-h', default='127.0.0.1', help='Router地址')
@click.option('--port', '-p', default=5555, help='Router端口')
@click.option('--list', '-l', is_flag=True, help='列出所有可用服务和方法')
@click.option('--router-info', is_flag=True, help='显示路由器信息')
@click.option('--queue-status', is_flag=True, help='显示队列状态')
@click.option('--call', '-c', help='调用服务方法，格式: ServiceName.method')
@click.option('--args', '-a', help='方法参数，使用JSON格式，例如: \'["Hello", "World"]\'')
@click.option('--timeout', '-t', default=30.0, help='请求超时时间（秒）')
@click.option('--api-key', help='API认证密钥')
@click.option('--logger-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']), default='INFO', help='日志级别')
def client(host, port, list, router_info, queue_status, call, args, timeout, api_key, logger_level):
    """VoidRail客户端命令行接口"""
    address = f"tcp://{host}:{port}"
    
    async def run_client():
        client = ClientDealer(router_address=address, timeout=timeout, api_key=api_key, logger_level=logger_level)
        try:
            await client.connect()
            click.echo(f"已连接到Router: {address}")
            
            if list:
                # 列出所有可用服务
                services = await client.discover_services()
                if not services:
                    click.echo("未发现任何服务方法")
                else:
                    click.echo("可用服务方法:")
                    for method_name, details in services.items():
                        desc = details.get('description', '无描述')
                        params = details.get('params', {})
                        param_desc = ", ".join([f"{k}: {v}" for k, v in params.items()]) if params else "无参数说明"
                        click.echo(f"  {method_name}")
                        click.echo(f"    描述: {desc}")
                        click.echo(f"    参数: {param_desc}")
                
                # 获取集群信息
                clusters = await client.discover_clusters()
                active_services = {k: v for k, v in clusters.items() if v.get('state') == 'active'}
                click.echo(f"\n活跃服务实例: {len(active_services)}")
                for instance_id, info in active_services.items():
                    click.echo(f"  {instance_id} (组: {info.get('group', '未知')})")
            
            if router_info:
                # 获取路由器信息
                info = await client.get_router_info()
                click.echo("\n路由器信息:")
                click.echo(f"  模式: {info.get('mode', '未知')}")
                click.echo(f"  地址: {info.get('address', '未知')}")
                click.echo(f"  心跳超时: {info.get('heartbeat_timeout', '未知')}秒")
                click.echo(f"  活跃服务数: {info.get('active_services', 0)}")
                click.echo(f"  总服务数: {info.get('total_services', 0)}")
                click.echo(f"  认证要求: {info.get('auth_required', False)}")
            
            if queue_status:
                # 获取队列状态
                queues = await client.get_queue_status()
                click.echo("\n方法队列状态:")
                if not queues:
                    click.echo("  无队列信息")
                for method, status in queues.items():
                    click.echo(f"  {method}:")
                    click.echo(f"    队列长度: {status.get('queue_length', 0)}")
                    click.echo(f"    空闲服务数: {status.get('available_services', 0)}")
                    click.echo(f"    繁忙服务数: {status.get('busy_services', 0)}")
            
            if call:
                # 调用指定服务方法
                if not args:
                    args_list = []
                else:
                    try:
                        args_list = json.loads(args)
                        if not isinstance(args_list, list):
                            args_list = [args_list]
                    except json.JSONDecodeError:
                        # 非JSON格式，视为单个字符串参数
                        args_list = [args]
                
                click.echo(f"调用: {call}")
                click.echo(f"参数: {args_list}")
                
                try:
                    # 使用流式API调用服务方法
                    click.echo("响应:")
                    async for response in client.stream(call, *args_list):
                        click.echo(f"  {response}")
                except Exception as e:
                    click.echo(f"调用出错: {e}", err=True)
            
            if not any([list, router_info, queue_status, call]):
                click.echo("请指定至少一个操作: --list, --router-info, --queue-status, 或 --call")
        
        finally:
            await client.close()
    
    try:
        asyncio.run(run_client())
    except KeyboardInterrupt:
        click.echo("操作已中断")
    except Exception as e:
        click.echo(f"错误: {e}", err=True)

@cli.command()
@click.option('--host', '-h', default='127.0.0.1', help='Router地址')
@click.option('--port', '-p', default=5555, help='Router端口')
@click.option('--module', '-m', required=True, help='包含ServiceDealer类的Python模块路径')
@click.option('--class', 'class_name', required=True, help='ServiceDealer类名')
@click.option('--max-concurrent', default=100, help='最大并发请求数')
@click.option('--heartbeat', default=0.5, help='心跳间隔（秒）')
@click.option('--api-key', help='API认证密钥')
@click.option('--logger-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']), default='INFO', help='日志级别')
def dealer(host, port, module, class_name, max_concurrent, heartbeat, api_key, logger_level):
    """启动VoidRail Dealer服务实例"""
    address = f"tcp://{host}:{port}"
    
    try:
        # 添加当前目录到模块搜索路径
        sys.path.insert(0, str(Path.cwd()))
        
        # 动态导入模块和类
        click.echo(f"导入模块: {module}")
        dealer_module = importlib.import_module(module)
        
        click.echo(f"加载类: {class_name}")
        if not hasattr(dealer_module, class_name):
            click.echo(f"错误: 模块 {module} 中未找到类 {class_name}", err=True)
            available_classes = [name for name in dir(dealer_module) 
                               if isinstance(getattr(dealer_module, name), type) 
                               and issubclass(getattr(dealer_module, name), ServiceDealer)
                               and getattr(dealer_module, name) != ServiceDealer]
            
            if available_classes:
                click.echo(f"可用的ServiceDealer类: {', '.join(available_classes)}")
            return
        
        dealer_class = getattr(dealer_module, class_name)
        
        # 检查类是否是ServiceDealer的子类
        if not issubclass(dealer_class, ServiceDealer):
            click.echo(f"错误: {class_name} 不是 ServiceDealer 的子类", err=True)
            return
        
        # 获取服务方法信息
        service_methods = dealer_class._registry.keys()
        click.echo(f"服务方法: {', '.join(service_methods)}")
        
    except ImportError as e:
        click.echo(f"错误: 无法导入模块 {module}: {e}", err=True)
        return
    
    async def start_dealer():
        service = dealer_class(
            router_address=address,
            max_concurrent=max_concurrent,
            heartbeat_interval=heartbeat,
            api_key=api_key,
            logger_level=logger_level
        )
        
        await service.start()
        click.echo(f"服务 {class_name} 已启动并连接到 {address}")
        click.echo(f"服务ID: {service._service_id}")
        click.echo(f"最大并发数: {max_concurrent}")
        
        try:
            # 保持运行直到中断
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            click.echo("正在停止服务...")
        finally:
            await service.stop()
            click.echo("服务已停止")
    
    try:
        asyncio.run(start_dealer())
    except KeyboardInterrupt:
        click.echo("服务已中断")
    except Exception as e:
        click.echo(f"服务运行错误: {e}", err=True)

if __name__ == "__main__":
    cli() 