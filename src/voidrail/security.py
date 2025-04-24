import os
import zmq.auth

def generate_curve_certificates(keys_dir="./keys"):
    """生成CURVE密钥对并保存到文件"""
    os.makedirs(keys_dir, exist_ok=True)
    
    # 创建服务器密钥
    server_public_file, server_secret_file = zmq.auth.create_certificates(
        keys_dir, "server")
    
    # 创建客户端密钥
    client_public_file, client_secret_file = zmq.auth.create_certificates(
        keys_dir, "client")
    
    return {
        "server_public": f"{keys_dir}/server.key",
        "server_secret": f"{keys_dir}/server.key_secret",
        "client_public": f"{keys_dir}/client.key",
        "client_secret": f"{keys_dir}/client.key_secret",
    }

def load_curve_keys(server_key_path=None, client_key_path=None):
    """加载CURVE密钥"""
    result = {}
    
    # 尝试从环境变量获取密钥
    server_key_hex = os.environ.get("VOIDRAIL_CURVE_SERVER_KEY")
    client_key_hex = os.environ.get("VOIDRAIL_CURVE_CLIENT_KEY")
    
    # 从十六进制字符串加载服务器密钥
    if server_key_hex:
        result["server_key"] = bytes.fromhex(server_key_hex)
    
    # 从十六进制字符串加载客户端密钥
    if client_key_hex:
        result["client_key"] = bytes.fromhex(client_key_hex)
    
    # 从文件加载服务器密钥
    if server_key_path and os.path.exists(server_key_path):
        server_public, server_secret = zmq.auth.load_certificate(server_key_path)
        result["server_public"] = server_public
        result["server_secret"] = server_secret
    
    # 从文件加载客户端密钥
    if client_key_path and os.path.exists(client_key_path):
        client_public, client_secret = zmq.auth.load_certificate(client_key_path)
        result["client_public"] = client_public
        result["client_secret"] = client_secret
    
    return result
