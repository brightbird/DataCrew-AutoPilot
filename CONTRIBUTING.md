# 🤝 贡献指南 - Contributing Guide

感谢您对 DataCrew AutoPilot 项目的关注！我们欢迎所有形式的贡献。

## 🚀 快速开始

### 1. Fork 项目
点击页面右上角的 "Fork" 按钮创建您的项目副本。

### 2. 克隆代码
```bash
git clone https://github.com/brightbird/DataCrew-AutoPilot.git
cd DataCrew-AutoPilot
```

### 3. 创建开发环境
```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
pip install -r requirements.txt
```

### 4. 配置环境变量
```bash
cp .env.example .env
# 编辑 .env 文件，填入您的API密钥
```

## 🎯 贡献类型

### 🐛 Bug 修复
- 在 Issues 中搜索相关问题
- 如果没有相关问题，请创建新的 Issue
- Fork 项目并创建修复分支
- 提交 Pull Request

### 💡 新功能开发
- 先在 Issues 中讨论您的想法
- 获得维护者同意后开始开发
- 遵循现有代码风格
- 添加必要的测试和文档

### 📝 文档改进
- 修正错别字和语法错误
- 完善使用说明
- 添加示例代码
- 翻译文档

### 🧪 测试用例
- 添加单元测试
- 增加集成测试
- 性能测试
- 边界条件测试

## 📋 开发规范

### 代码风格
- 使用 Python PEP 8 规范
- 函数和变量使用下划线命名
- 类名使用驼峰命名
- 添加必要的注释和文档字符串

### 提交规范
```
<type>(<scope>): <subject>

<body>

<footer>
```

类型说明：
- `feat`: 新功能
- `fix`: Bug修复
- `docs`: 文档更新
- `style`: 代码格式化
- `refactor`: 代码重构
- `test`: 测试相关
- `chore`: 构建过程或辅助工具的变动

### Pull Request 规范
- 标题清晰描述更改内容
- 详细说明更改原因和影响
- 关联相关的 Issues
- 确保所有测试通过
- 代码审查通过后合并

## 🛠️ 开发环境

### 推荐工具
- Python 3.8+
- VS Code 或 PyCharm
- Git 2.20+

### 调试技巧
```bash
# 启动调试模式
export DEBUG_MODE=true
python app.py

# 运行测试
python -m pytest tests/

# 代码格式化
black . --line-length 88
```

## ❓ 获得帮助

- 查看 [项目文档](README.md)
- 搜索 [Issues](../../issues)
- 加入讨论区交流
- 联系维护者

## 📄 行为准则

请遵守我们的 [行为准则](CODE_OF_CONDUCT.md)，保持友好和专业的交流环境。

---

再次感谢您的贡献！让我们一起打造更好的数据分析自动驾驶系统！🚗✨ 