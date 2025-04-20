import click
import asyncio
import importlib
import sys
import json
import logging
import multiprocessing
import os
import signal
import time
from typing import List, Optional, Dict, Any
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
@click.option('--args', '-a', help='方法参数，使用JSON格式，例如: \'["Hello", "World"]\' 或 \'{"name": "World", "age": 30}\'')
@click.option('--timeout', '-t', default=30.0, help='请求超时时间（秒）')
@click.option('--api-key', help='API认证密钥')
@click.option('--logger-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']), default='WARNING', help='日志级别')
def client(host, port, list, router_info, queue_status, call, args, timeout, api_key, logger_level):
    """VoidRail客户端命令行接口"""
    address = f"tcp://{host}:{port}"
    
    # 设置日志级别
    logging.getLogger("voidrail").setLevel(getattr(logging, logger_level))
    
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
                parsed_args = []
                parsed_kwargs = {}
                
                if args:
                    try:
                        # 解析JSON参数
                        json_data = json.loads(args)
                        
                        # 直接检查是否是字典类型
                        if type(json_data) == dict:
                            parsed_kwargs = json_data
                            click.echo(f"使用关键字参数: {parsed_kwargs}")
                        elif type(json_data) == list:
                            parsed_args = json_data
                            click.echo(f"使用位置参数: {parsed_args}")
                        else:
                            # 非列表或字典，作为单个位置参数处理
                            parsed_args = [json_data]
                            click.echo(f"使用单一值作为位置参数: {parsed_args}")
                    except json.JSONDecodeError:
                        # 非JSON格式，视为单个字符串参数
                        parsed_args = [args]
                        click.echo(f"使用字符串作为位置参数: {parsed_args}")
                
                click.echo(f"调用: {call}")
                
                try:
                    # 使用流式API调用服务方法
                    click.echo("响应:")
                    # 先展示要发送的参数
                    if parsed_kwargs:
                        click.echo(f"将发送关键字参数: {parsed_kwargs}")
                    else:
                        click.echo(f"将发送位置参数: {parsed_args}")
                        
                    # 调用服务
                    async for response in client.stream(call, *parsed_args, **parsed_kwargs):
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
@click.option('--class', 'class_names', required=True, multiple=True, help='ServiceDealer类名(可多次指定)')
@click.option('--instances', '-n', default=1, help='每个类启动的实例数量')
@click.option('--max-concurrent', default=100, help='最大并发请求数')
@click.option('--heartbeat', default=0.5, help='心跳间隔（秒）')
@click.option('--api-key', help='API认证密钥')
@click.option('--logger-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']), default='INFO', help='日志级别')
def dealer(host, port, module, class_names, instances, max_concurrent, heartbeat, api_key, logger_level):
    """启动VoidRail Dealer服务实例(支持多进程)"""
    address = f"tcp://{host}:{port}"
    
    # 设置日志级别
    logging.getLogger("voidrail").setLevel(getattr(logging, logger_level))
    
    try:
        # 添加当前目录到模块搜索路径
        sys.path.insert(0, str(Path.cwd()))
        
        # 动态导入模块和类
        click.echo(f"导入模块: {module}")
        dealer_module = importlib.import_module(module)
        
        # 获取所有可用的ServiceDealer子类
        available_classes = {}
        for name in dir(dealer_module):
            attr = getattr(dealer_module, name)
            if isinstance(attr, type) and issubclass(attr, ServiceDealer) and attr != ServiceDealer:
                available_classes[name] = attr
        
        # 验证指定的类是否存在
        dealer_classes = {}
        for class_name in class_names:
            if class_name not in available_classes:
                click.echo(f"错误: 模块 {module} 中未找到类 {class_name}", err=True)
                if available_classes:
                    click.echo(f"可用的ServiceDealer类: {', '.join(available_classes.keys())}")
                return
            
            dealer_classes[class_name] = available_classes[class_name]
        
        # 检查是否至少有一个类
        if not dealer_classes:
            click.echo("错误: 未指定任何可用的ServiceDealer类", err=True)
            return
        
        # 为每个类的每个实例创建进程
        processes = []
        
        # 生成子进程启动配置
        process_configs = []
        for class_name, dealer_class in dealer_classes.items():
            for i in range(instances):
                process_configs.append({
                    "class_name": class_name,
                    "dealer_class": dealer_class,
                    "instance_id": i+1,
                    "address": address,
                    "max_concurrent": max_concurrent,
                    "heartbeat": heartbeat,
                    "api_key": api_key,
                    "logger_level": logger_level
                })
        
        # 显示启动信息
        click.echo(f"将启动 {len(dealer_classes)} 个类型的Dealer服务，每个类型 {instances} 个实例，"
                   f"共 {len(process_configs)} 个实例")
        for config in process_configs:
            click.echo(f"  - {config['class_name']} (实例 {config['instance_id']})")
        
        # 启动子进程
        for config in process_configs:
            p = multiprocessing.Process(
                target=start_dealer_process,
                args=(config,),
                name=f"{config['class_name']}-{config['instance_id']}"
            )
            p.daemon = True
            p.start()
            processes.append(p)
            click.echo(f"启动子进程: {p.name} (PID: {p.pid})")
            # 稍微延迟启动，避免瞬时资源压力
            time.sleep(0.1)
        
        click.echo(f"所有 {len(processes)} 个Dealer服务进程已启动")
        click.echo("按Ctrl+C终止所有服务")
        
        # 主进程等待所有子进程
        try:
            # 保持运行直到中断
            while any(p.is_alive() for p in processes):
                time.sleep(0.5)
        except KeyboardInterrupt:
            click.echo("正在终止所有Dealer服务进程...")
            # 终止所有子进程
            for p in processes:
                if p.is_alive():
                    click.echo(f"终止进程: {p.name} (PID: {p.pid})")
                    p.terminate()
            
            # 等待所有进程结束
            for p in processes:
                p.join(timeout=1.0)
            
            click.echo("所有Dealer服务进程已终止")
        
    except ImportError as e:
        click.echo(f"错误: 无法导入模块 {module}: {e}", err=True)
        return
    except Exception as e:
        click.echo(f"启动服务时发生错误: {e}", err=True)
        return

def start_dealer_process(config: Dict[str, Any]):
    """在子进程中启动一个Dealer服务实例"""
    try:
        # 设置进程名称
        process_name = f"{config['class_name']}-{config['instance_id']}"
        
        # 配置日志
        logging.basicConfig(
            level=getattr(logging, config['logger_level']),
            format=f'%(asctime)s - %(name)s - {process_name}[%(process)d] - %(levelname)s - %(message)s'
        )
        logger = logging.getLogger("voidrail.dealer")
        logger.info(f"启动 {process_name} 服务进程")
        
        # 启动服务实例
        asyncio.run(start_dealer_service(
            dealer_class=config['dealer_class'],
            class_name=config['class_name'],
            address=config['address'],
            max_concurrent=config['max_concurrent'],
            heartbeat_interval=config['heartbeat'],
            api_key=config['api_key'],
            instance_id=config['instance_id'],
            logger_level=config['logger_level']
        ))
    except Exception as e:
        logger = logging.getLogger("voidrail.dealer")
        logger.error(f"服务进程 {process_name} 发生错误: {e}")
        raise

async def start_dealer_service(dealer_class, class_name, address, max_concurrent, 
                              heartbeat_interval, api_key, instance_id, logger_level):
    """异步启动一个Dealer服务实例"""
    # 创建服务实例
    service = dealer_class(
        router_address=address,
        max_concurrent=max_concurrent,
        heartbeat_interval=heartbeat_interval,
        api_key=api_key,
        logger_level=logger_level
    )
    
    # 启动服务
    await service.start()
    logger = logging.getLogger("voidrail.dealer")
    logger.info(f"服务 {class_name} (实例 {instance_id}) 已启动并连接到 {address}")
    logger.info(f"服务ID: {service._service_id}")
    logger.info(f"最大并发数: {max_concurrent}")
    
    # 设置信号处理
    stop_event = asyncio.Event()
    
    def signal_handler():
        logger.info(f"收到终止信号，准备停止服务 {class_name} (实例 {instance_id})")
        stop_event.set()
    
    # 注册SIGINT和SIGTERM处理
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_event_loop().add_signal_handler(
            sig, signal_handler
        )
    
    try:
        # 等待终止信号
        await stop_event.wait()
    finally:
        # 停止服务
        logger.info(f"正在停止服务 {class_name} (实例 {instance_id})")
        await service.stop()
        logger.info(f"服务 {class_name} (实例 {instance_id}) 已停止")

if __name__ == "__main__":
    cli() 