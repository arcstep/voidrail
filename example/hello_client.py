import sys
import os
import time
import json
from voidrail.client import CeleryClient

def main():
    """示例客户端脚本"""
    # 创建客户端
    client = CeleryClient(service_name="hello")
    
    # 列出可用任务
    print("可用任务:")
    tasks = client.list_registered_tasks()
    for task in tasks:
        print(f"  - {task}")
    
    # 异步调用
    print("\n异步调用测试:")
    async_result = client.send_task(
        task_name="say_hello",
        args=["Async World"],
        wait_result=False
    )
    print(f"任务ID: {async_result['task_id']}")
    print(f"状态: {async_result['status']}")
    
    # 稍等片刻
    time.sleep(1)
    
    # 获取状态
    status = client.get_task_status(async_result['task_id'])
    print(f"任务状态: {status['status']}")
    if 'result' in status:
        print(f"任务结果: {status['result']}")
    
    # 同步调用
    print("\n同步调用测试:")
    sync_result = client.send_task(
        task_name="say_hello",
        args=["Sync World"],
        wait_result=True
    )
    print(f"任务ID: {sync_result['task_id']}")
    print(f"状态: {sync_result['status']}")
    print(f"结果: {sync_result['result']}")
    
    # 带进度的任务
    print("\n带进度的任务测试:")
    progress_result = client.send_task(
        task_name="say_hello_delay",
        args=["Progress World"],
        kwargs={"delay": 2},
        wait_result=False
    )
    task_id = progress_result['task_id']
    print(f"任务ID: {task_id}")
    
    # 监控进度
    for _ in range(12):
        status = client.get_task_status(task_id)
        status_str = status['status']
        
        if status_str == 'processing' and 'info' in status:
            progress = status['info'].get('progress', 0)
            message = status['info'].get('message', '')
            print(f"进度: {progress}% - {message}")
        elif status_str == 'completed':
            print(f"完成: {status['result']}")
            break
        elif status_str == 'failed':
            print(f"失败: {status.get('error', '未知错误')}")
            break
        
        time.sleep(0.5)
    
    # 获取Worker统计信息
    print("\nWorker统计信息:")
    stats = client.get_worker_stats()
    print(f"在线Worker: {stats.get('workers', [])}")
    print(f"活动任务数: {sum(len(tasks) for tasks in stats.get('active_tasks', {}).values())}")

if __name__ == "__main__":
    main()
