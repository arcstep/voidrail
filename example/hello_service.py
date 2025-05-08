import sys
import os
import time
from voidrail import task, CeleryWorker

class HelloService(CeleryWorker):
    """示例服务，提供简单的问候功能"""
    
    def __init__(self):
        # 初始化父类
        super().__init__(service_name="hello")
        
        # 注册任务
        @task(name='hello.say_hello')
        def say_hello(name):
            """简单的问候任务"""
            return f"Hello, {name}! Current time: {time.ctime()}"
        
        @task(name='hello.say_hello_delay', bind=True)
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
    service = HelloService()
    
    # 显示服务信息
    print(f"服务名称: {service.service_name}")
    print(f"Broker URL: {service.broker_url}")
    print(f"后端 URL: {service.backend_url}")
    print(f"已注册任务: {service.get_registered_tasks()}")
    
    # 启动Worker
    service.start_worker()

if __name__ == "__main__":
    main()
