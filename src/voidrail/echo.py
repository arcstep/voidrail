from voidrail import ServiceDealer, service_method

class EchoService(ServiceDealer):
    @service_method
    async def hello(self, name: str) -> str:
        """向给定名字打招呼"""
        return f"Hello, {name}!"
    
    @service_method(
        name="greet",
        description="高级问候服务",
        params={
            "name": "人名",
            "title": "称呼(可选)"
        }
    )
    async def advanced_greeting(self, name: str, title: str = "") -> str:
        """支持称呼的问候服务"""
        if title:
            return f"Hello, {title} {name}!"
        return f"Hello, {name}!" 
