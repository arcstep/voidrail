import sys
import os
import time
from voidrail import create_app, start, get_config

app = create_app('hello')

@app.task(name='hello.say_hello')
def say_hello(name):
    """简单的问候任务"""
    return f"Hello, {name}! Current time: {time.ctime()}"

@app.task(name='hello.say_hello_delay', bind=True)
def say_hello_delay(self, name, delay=3):
    """带延迟的问候任务，演示任务状态更新"""
    self.update_state(state='PROGRESS', meta={'progress': 0, 'message': '开始处理'})
    
    # 模拟处理过程
    for i in range(10):
        time.sleep(delay / 10)
        self.update_state(state='PROGRESS', meta={
            'progress': (i + 1) * 10, 
            'message': f'处理中 {(i + 1) * 10}%'
        })
    
    return f"Hello after {delay} seconds, {name}! Time: {time.ctime()}"

def main():
    """命令行入口点"""
    # 显示服务信息
    config = get_config()
    print(f"Broker URL: {config['broker_url']}")
    print(f"后端 URL: {config['result_backend']}")
    
    # 获取已注册任务
    tasks = [t for t in app.tasks.keys() if not t.startswith('celery.')]
    print(f"已注册任务: {tasks}")
    
    # 启动Worker (使用新的start函数)
    start(app)

if __name__ == "__main__":
    main()
