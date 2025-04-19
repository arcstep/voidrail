import secrets
import os
from datetime import datetime

class ApiKeyManager:
    @staticmethod
    def generate_key(prefix="vr"):
        """生成带时间戳前缀的API密钥"""
        timestamp = datetime.now().strftime("%y%m%d")
        random_part = secrets.token_urlsafe(16)
        return f"{prefix}_{timestamp}_{random_part}"
    
    @staticmethod
    def add_dealer_key(key=None, env_file=".env"):
        """将DEALER API密钥添加到环境变量文件
        
        如果未提供密钥，会自动生成一个新密钥
        """
        key = key or ApiKeyManager.generate_key(prefix="dealer")
        with open(env_file, "a") as f:
            f.write(f"\n# DEALER服务端API密钥\nVOIDRAIL_API_KEY={key}\n")
        return key
    
    @staticmethod
    def add_client_key(key=None, env_file=".env"):
        """将CLIENT API密钥添加到环境变量文件
        
        如果未提供密钥，会自动生成一个新密钥
        """
        key = key or ApiKeyManager.generate_key(prefix="client")
        with open(env_file, "a") as f:
            f.write(f"\n# CLIENT客户端API密钥\nVOIDRAIL_API_KEY={key}\n")
        return key
    
    @staticmethod
    def add_router_dealer_key(key=None, env_file="router.env"):
        """将DEALER API密钥添加到Router的有效密钥列表"""
        key = key or ApiKeyManager.generate_key(prefix="dealer")
        with open(env_file, "a") as f:
            f.write(f"\n# 新增DEALER密钥\nVOIDRAIL_DEALER_API_KEYS={key}\n")
        return key
    
    @staticmethod
    def add_router_client_key(key=None, env_file="router.env"):
        """将CLIENT API密钥添加到Router的有效密钥列表"""
        key = key or ApiKeyManager.generate_key(prefix="client")
        with open(env_file, "a") as f:
            f.write(f"\n# 新增CLIENT密钥\nVOIDRAIL_CLIENT_API_KEYS={key}\n")
        return key
    
    @staticmethod
    def enable_router_auth(env_file="router.env"):
        """在Router环境变量文件中启用API密钥认证"""
        with open(env_file, "a") as f:
            f.write("\n# 启用API密钥认证\nVOIDRAIL_REQUIRE_AUTH=true\n")
    
    @staticmethod
    def create_dealer_env(output_file="dealer.env"):
        """创建一个完整的DEALER环境变量文件"""
        key = ApiKeyManager.generate_key(prefix="dealer")
        with open(output_file, "w") as f:
            f.write(f"# VoidRail DEALER服务端环境变量\n")
            f.write(f"VOIDRAIL_API_KEY={key}\n")
        return key
    
    @staticmethod
    def create_client_env(output_file="client.env"):
        """创建一个完整的CLIENT环境变量文件"""
        key = ApiKeyManager.generate_key(prefix="client")
        with open(output_file, "w") as f:
            f.write(f"# VoidRail CLIENT客户端环境变量\n")
            f.write(f"VOIDRAIL_API_KEY={key}\n")
        return key
    
    @staticmethod
    def create_router_env(output_file="router.env", require_auth=True):
        """创建一个完整的ROUTER环境变量文件"""
        dealer_key = ApiKeyManager.generate_key(prefix="dealer")
        client_key = ApiKeyManager.generate_key(prefix="client")
        
        with open(output_file, "w") as f:
            f.write(f"# VoidRail ROUTER路由器环境变量\n")
            f.write(f"VOIDRAIL_REQUIRE_AUTH={'true' if require_auth else 'false'}\n")
            f.write(f"VOIDRAIL_DEALER_API_KEYS={dealer_key}\n")
            f.write(f"VOIDRAIL_CLIENT_API_KEYS={client_key}\n")
            
        return {
            "dealer_key": dealer_key,
            "client_key": client_key
        }
    
