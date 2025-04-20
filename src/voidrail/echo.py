from voidrail import ServiceDealer, service_method

class EchoService(ServiceDealer):
    @service_method
    async def hello(self, name: str) -> str:
        return f"Hello, {name}!"