import time
from voidrail import create_app

app = create_app('echo')

@app.task(name='echo.say_hello')
def say_hello(name):
    """简单的问候任务"""
    return f"Hello, {name}! Current time: {time.ctime()}"

@app.task(name='echo.say_hello_delay', bind=True)
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
