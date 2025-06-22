#!/usr/bin/env python3
"""
SQL Assistant Crew 启动脚本
支持阿里云百炼API和完整的环境检查
"""
import os
import sys
import subprocess
import importlib.util
from pathlib import Path

def check_python_version():
    """检查Python版本"""
    if sys.version_info < (3, 8):
        print("❌ 错误：需要Python 3.8或更高版本")
        print(f"当前版本：{sys.version}")
        return False
    print(f"✅ Python版本检查通过：{sys.version.split()[0]}")
    return True

def check_virtual_env():
    """检查是否在虚拟环境中"""
    in_venv = hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)
    if not in_venv:
        print("⚠️  警告：未检测到虚拟环境")
        print("建议在虚拟环境中运行此项目")
        response = input("是否继续？(y/N): ")
        if response.lower() != 'y':
            return False
    else:
        print("✅ 虚拟环境检查通过")
    return True

def check_dependencies():
    """检查关键依赖是否安装"""
    required_packages = [
        'streamlit',
        'crewai', 
        'pandas',
        'pandasai',
        'dotenv'
    ]
    
    missing_packages = []
    for package in required_packages:
        if importlib.util.find_spec(package) is None:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"❌ 缺少以下依赖包：{', '.join(missing_packages)}")
        print("请运行：pip install -r requirements.txt")
        return False
    
    print("✅ 依赖包检查通过")
    return True

def check_database():
    """检查数据库文件是否存在"""
    db_path = Path("data/sample_db.sqlite")
    if not db_path.exists():
        print("⚠️  警告：未找到数据库文件")
        print("正在初始化数据库...")
        try:
            from utils.db_simulator import initialize_database
            initialize_database()
            print("✅ 数据库初始化完成")
        except Exception as e:
            print(f"❌ 数据库初始化失败：{e}")
            return False
    else:
        print("✅ 数据库文件检查通过")
    return True

def setup_environment():
    """设置环境变量"""
    # 尝试加载.env文件
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    
    # 检查API密钥
    api_key = os.environ.get("DASHSCOPE_API_KEY")
    if not api_key:
        print("❌ 错误：未找到 DASHSCOPE_API_KEY 环境变量")
        print("\n请选择以下方式之一设置API密钥：")
        print("1. 设置环境变量：export DASHSCOPE_API_KEY=your_api_key")
        print("2. 创建 .env 文件并添加：DASHSCOPE_API_KEY=your_api_key")
        print("3. 直接输入API密钥（临时）")
        
        choice = input("\n选择方式 (1/2/3) 或按回车退出: ")
        if choice == "3":
            api_key = input("请输入您的阿里云百炼API密钥: ").strip()
            if api_key:
                os.environ["DASHSCOPE_API_KEY"] = api_key
            else:
                return False
        else:
            return False
    
    # 设置阿里云百炼API为OpenAI兼容模式
    os.environ["OPENAI_API_KEY"] = api_key
    os.environ["OPENAI_API_BASE"] = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    
    print("✅ 环境变量配置完成")
    print(f"🔑 使用API密钥：{api_key[:8]}***")
    print("🌐 API基础URL：https://dashscope.aliyuncs.com/compatible-mode/v1")
    return True

def start_streamlit():
    """启动Streamlit应用"""
    print("\n📊 启动Streamlit应用...")
    print("🌐 应用将在浏览器中自动打开：http://localhost:8501")
    print("🔄 按 Ctrl+C 停止应用\n")
    
    try:
        # 使用subprocess启动streamlit，可以捕获错误
        result = subprocess.run([
            sys.executable, "-m", "streamlit", "run", "app.py"
        ], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Streamlit启动失败：{e}")
        return False
    except KeyboardInterrupt:
        print("\n👋 应用已停止")
        return True
    
    return True

def main():
    """主函数"""
    print("🚀 SQL Assistant Crew 启动程序")
    print("=" * 50)
    
    # 环境检查流程
    checks = [
        ("Python版本", check_python_version),
        ("虚拟环境", check_virtual_env),
        ("依赖包", check_dependencies),
        ("数据库", check_database),
        ("环境配置", setup_environment)
    ]
    
    for check_name, check_func in checks:
        print(f"\n🔍 检查{check_name}...")
        if not check_func():
            print(f"\n❌ {check_name}检查失败，启动中止")
            sys.exit(1)
    
    print("\n" + "=" * 50)
    print("🎉 所有检查通过，准备启动应用！")
    
    # 启动应用
    if not start_streamlit():
        sys.exit(1)

if __name__ == "__main__":
    main() 