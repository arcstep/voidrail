import os
import zmq.auth

def generate_curve_keys(output_dir="./keys", key_name="server"):
    """生成CURVE密钥对，仅保留基本功能"""
    os.makedirs(output_dir, exist_ok=True)
    
    # 创建密钥
    public_file, secret_file = zmq.auth.create_certificates(output_dir, key_name)
    
    # 加载生成的密钥以获取实际值
    public_key, secret_key = zmq.auth.load_certificate(secret_file)
    
    print(f"密钥已生成到: {output_dir}")
    print(f"公钥文件: {public_file}")
    print(f"私钥文件: {secret_file}")
    print(f"公钥指纹: {public_key.hex()[:16]}")
    print(f"公钥(完整): {public_key.hex()}")
    
    return {
        "public_file": public_file,
        "secret_file": secret_file,
        "public_key": public_key,
        "secret_key": secret_key,
        "public_key_hex": public_key.hex(),
        "fingerprint": public_key.hex()[:16]
    }
