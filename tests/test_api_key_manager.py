import os
import sys
import pytest
import tempfile
import re
from datetime import datetime
from pathlib import Path

# 添加父目录到路径中，以便能够导入src目录下的模块
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.voidrail.api_key import ApiKeyManager

@pytest.fixture
def temp_dir():
    """创建临时目录"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir

@pytest.fixture
def temp_env_file(temp_dir):
    """创建临时.env文件路径"""
    return os.path.join(temp_dir, ".env")

@pytest.fixture
def temp_router_file(temp_dir):
    """创建临时router.env文件路径"""
    return os.path.join(temp_dir, "router.env")

def test_generate_key():
    """测试密钥生成功能"""
    # 测试默认前缀
    key = ApiKeyManager.generate_key()
    assert key.startswith("vr_")
    
    # 测试自定义前缀
    key = ApiKeyManager.generate_key(prefix="test")
    assert key.startswith("test_")
    
    # 测试密钥格式
    today = datetime.now().strftime("%y%m%d")
    pattern = re.compile(rf"test_{today}_[A-Za-z0-9_-]+")
    assert pattern.match(key)
    
    # 测试唯一性
    key2 = ApiKeyManager.generate_key(prefix="test")
    assert key != key2

def test_add_dealer_key(temp_env_file):
    """测试添加DEALER密钥到环境文件"""
    # 测试自动生成密钥
    key = ApiKeyManager.add_dealer_key(env_file=temp_env_file)
    assert key.startswith("dealer_")
    
    # 验证文件内容
    with open(temp_env_file, "r") as f:
        content = f.read()
        assert f"VOIDRAIL_API_KEY={key}" in content
        assert "# DEALER服务端API密钥" in content
    
    # 测试指定密钥
    custom_key = "custom_dealer_key"
    returned_key = ApiKeyManager.add_dealer_key(key=custom_key, env_file=temp_env_file)
    assert returned_key == custom_key
    
    # 验证文件内容
    with open(temp_env_file, "r") as f:
        content = f.read()
        assert f"VOIDRAIL_API_KEY={custom_key}" in content

def test_add_client_key(temp_env_file):
    """测试添加CLIENT密钥到环境文件"""
    # 测试自动生成密钥
    key = ApiKeyManager.add_client_key(env_file=temp_env_file)
    assert key.startswith("client_")
    
    # 验证文件内容
    with open(temp_env_file, "r") as f:
        content = f.read()
        assert f"VOIDRAIL_API_KEY={key}" in content
        assert "# CLIENT客户端API密钥" in content

def test_enable_router_auth(temp_router_file):
    """测试启用Router认证功能"""
    ApiKeyManager.enable_router_auth(env_file=temp_router_file)
    
    # 验证文件内容
    with open(temp_router_file, "r") as f:
        content = f.read()
        assert "VOIDRAIL_REQUIRE_AUTH=true" in content
        assert "# 启用API密钥认证" in content

def test_multiple_operations(temp_env_file, temp_router_file):
    """测试多个操作组合"""
    # 1. 添加DEALER密钥
    dealer_key = ApiKeyManager.add_dealer_key(env_file=temp_env_file)
    
    # 2. 添加CLIENT密钥
    client_key = ApiKeyManager.add_client_key(env_file=temp_env_file)
    
    # 3. 启用Router认证
    ApiKeyManager.enable_router_auth(env_file=temp_router_file)
    
    # 验证.env文件
    with open(temp_env_file, "r") as f:
        content = f.read()
        assert dealer_key in content
        assert client_key in content
    
    # 验证router.env文件
    with open(temp_router_file, "r") as f:
        content = f.read()
        assert "VOIDRAIL_REQUIRE_AUTH=true" in content

def test_create_router_env(temp_dir):
    """测试创建完整的Router环境文件（如果类中有此方法）"""
    # 这个测试只有当ApiKeyManager有create_router_env方法时才运行
    if not hasattr(ApiKeyManager, 'create_router_env'):
        pytest.skip("ApiKeyManager没有create_router_env方法")
        
    output_file = os.path.join(temp_dir, "router.env")
    keys = ApiKeyManager.create_router_env(output_file=output_file)
    
    # 验证返回值
    assert 'dealer_key' in keys
    assert 'client_key' in keys
    
    # 验证文件内容
    with open(output_file, "r") as f:
        content = f.read()
        assert f"VOIDRAIL_DEALER_API_KEYS={keys['dealer_key']}" in content
        assert f"VOIDRAIL_CLIENT_API_KEYS={keys['client_key']}" in content

def test_create_dealer_env(temp_dir):
    """测试创建DEALER环境文件（如果类中有此方法）"""
    # 这个测试只有当ApiKeyManager有create_dealer_env方法时才运行
    if not hasattr(ApiKeyManager, 'create_dealer_env'):
        pytest.skip("ApiKeyManager没有create_dealer_env方法")
        
    output_file = os.path.join(temp_dir, "dealer.env")
    key = ApiKeyManager.create_dealer_env(output_file=output_file)
    
    # 验证文件内容
    with open(output_file, "r") as f:
        content = f.read()
        assert f"VOIDRAIL_API_KEY={key}" in content
        assert key.startswith("dealer_")

def test_create_client_env(temp_dir):
    """测试创建CLIENT环境文件（如果类中有此方法）"""
    # 这个测试只有当ApiKeyManager有create_client_env方法时才运行
    if not hasattr(ApiKeyManager, 'create_client_env'):
        pytest.skip("ApiKeyManager没有create_client_env方法")
        
    output_file = os.path.join(temp_dir, "client.env")
    key = ApiKeyManager.create_client_env(output_file=output_file)
    
    # 验证文件内容
    with open(output_file, "r") as f:
        content = f.read()
        assert f"VOIDRAIL_API_KEY={key}" in content
        assert key.startswith("client_") 