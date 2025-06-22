"""
PandasAI集成模块 - 用于数据可视化和进一步分析
"""
import os
import pandas as pd
import pandasai as pai
from typing import Optional, Union
import base64
from io import BytesIO
import sqlite3
import glob

class PandasAIAnalyzer:
    """PandasAI分析器类"""
    
    def __init__(self, db_path: str):
        """
        初始化PandasAI分析器
        
        Args:
            db_path: SQLite数据库路径
        """
        self.db_path = db_path
        self._setup_pandasai()
    
    def _setup_pandasai(self):
        """设置PandasAI配置"""
        try:
            # 使用正确的PandasAI 3.0 + OpenAI扩展包导入
            from pandasai_openai.openai import OpenAI
        except ImportError:
            raise ImportError("无法导入OpenAI LLM，请安装 pandasai-openai: pip install pandasai-openai")
        
        # 使用环境变量中的配置
        api_key = os.environ.get("OPENAI_API_KEY", os.environ.get("DASHSCOPE_API_KEY"))
        api_base = os.environ.get("OPENAI_API_BASE", "https://dashscope.aliyuncs.com/compatible-mode/v1")
        
        if not api_key:
            raise ValueError("未找到API密钥，请设置DASHSCOPE_API_KEY环境变量")
        
        # 配置LLM - 重点修复model参数
        try:
            llm = OpenAI(
                api_token=api_key,
                base_url=api_base,
                model="qwen-plus"  # 确保使用正确的模型名称
            )
            # 明确设置模型名称（解决PandasAI内部默认问题）
            llm.model = "qwen-plus"
        except Exception as e:
            print(f"LLM配置警告: {e}")
            # 简化配置作为后备
            llm = OpenAI(api_token=api_key)
            llm.model = "qwen-plus"  # 确保设置正确的模型
        
        # 配置PandasAI
        try:
            pai.config.set({
                "llm": llm,
                "save_charts": True,
                "save_charts_path": "charts/",
                "verbose": True
            })
        except Exception as e:
            print(f"警告：PandasAI配置可能不完全成功: {e}")
            # 基本配置
            pai.config.set({"llm": llm})
    
    def query_to_dataframe(self, sql_query: str) -> pd.DataFrame:
        """
        执行SQL查询并返回pandas DataFrame
        
        Args:
            sql_query: SQL查询语句
            
        Returns:
            pandas DataFrame
        """
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query(sql_query, conn)
            conn.close()
            return df
        except Exception as e:
            raise Exception(f"SQL查询执行失败: {e}")
    
    def analyze_with_natural_language(self, df: pd.DataFrame, question: str) -> str:
        """
        使用自然语言分析DataFrame
        
        Args:
            df: pandas DataFrame
            question: 自然语言问题
            
        Returns:
            分析结果
        """
        try:
            # 将DataFrame转换为PandasAI DataFrame
            pai_df = pai.DataFrame(df)
            
            # 使用自然语言进行分析
            result = pai_df.chat(question)
            
            return str(result)
        except Exception as e:
            return f"分析失败: {e}"
    
    def create_visualization(self, df: pd.DataFrame, chart_request: str) -> Optional[dict]:
        """
        创建数据可视化
        
        Args:
            df: pandas DataFrame
            chart_request: 图表请求（自然语言）
            
        Returns:
            包含图表信息的字典，格式：
            {
                "type": "image",
                "path": "图片文件路径",
                "base64": "base64编码的图片数据",
                "message": "提示信息"
            }
            或者
            {
                "type": "text",
                "content": "文本结果",
                "message": "提示信息"
            }
        """
        try:
            # 确保charts目录存在
            charts_dir = "charts"
            exports_charts_dir = "exports/charts"  # PandasAI默认目录
            
            for dir_path in [charts_dir, exports_charts_dir]:
                if not os.path.exists(dir_path):
                    os.makedirs(dir_path)
            
            # 记录生成图表前的文件列表（检查两个目录）
            existing_files = set()
            for pattern in ["charts/*.png", "charts/*.jpg", "charts/*.jpeg",
                           "exports/charts/*.png", "exports/charts/*.jpg", "exports/charts/*.jpeg"]:
                existing_files.update(glob.glob(pattern))
            
            # 将DataFrame转换为PandasAI DataFrame
            pai_df = pai.DataFrame(df)
            
            # 创建可视化
            result = pai_df.chat(chart_request)
            
            # 检查是否生成了新的图片文件
            new_files = set()
            for pattern in ["charts/*.png", "charts/*.jpg", "charts/*.jpeg",
                           "exports/charts/*.png", "exports/charts/*.jpg", "exports/charts/*.jpeg"]:
                new_files.update(glob.glob(pattern))
            
            # 找到新生成的文件
            new_chart_files = new_files - existing_files
            
            if new_chart_files:
                # 取最新的文件
                latest_chart = max(new_chart_files, key=os.path.getctime)
                
                # 读取并转换为base64
                with open(latest_chart, 'rb') as f:
                    img_data = f.read()
                base64_img = base64.b64encode(img_data).decode()
                
                return {
                    "type": "image",
                    "path": latest_chart,
                    "base64": base64_img,
                    "message": f"图表已生成：{os.path.basename(latest_chart)}"
                }
            
            # 如果结果是文件路径字符串
            elif isinstance(result, str):
                # 检查是否是图片文件路径
                if result.endswith(('.png', '.jpg', '.jpeg')):
                    if os.path.exists(result):
                        with open(result, 'rb') as f:
                            img_data = f.read()
                        base64_img = base64.b64encode(img_data).decode()
                        
                        return {
                            "type": "image",
                            "path": result,
                            "base64": base64_img,
                            "message": f"图表已生成：{os.path.basename(result)}"
                        }
                    else:
                        return {
                            "type": "text",
                            "content": result,
                            "message": "PandasAI返回了文件路径，但文件不存在"
                        }
                else:
                    # 文本结果
                    return {
                        "type": "text",
                        "content": str(result),
                        "message": "PandasAI返回了文本结果而不是图表"
                    }
            
            # 其他类型的结果
            else:
                return {
                    "type": "text",
                    "content": str(result) if result else "未生成任何内容",
                    "message": "PandasAI没有生成图表，返回了其他类型的结果"
                }
                
        except Exception as e:
            print(f"可视化创建失败: {e}")
            return {
                "type": "error",
                "content": f"可视化创建失败: {e}",
                "message": "图表生成过程中发生错误"
            }
    
    def get_data_insights(self, df: pd.DataFrame) -> str:
        """
        获取数据洞察
        
        Args:
            df: pandas DataFrame
            
        Returns:
            数据洞察文本
        """
        try:
            pai_df = pai.DataFrame(df)
            
            # 获取基本统计信息
            insights = pai_df.chat("请为这个数据集提供详细的数据洞察，包括主要统计信息、趋势和异常值")
            
            return str(insights)
        except Exception as e:
            return f"洞察生成失败: {e}"
    
    def suggest_next_questions(self, df: pd.DataFrame, current_query: str) -> list:
        """
        基于当前数据和查询建议下一步问题
        
        Args:
            df: pandas DataFrame
            current_query: 当前查询
            
        Returns:
            建议问题列表
        """
        try:
            pai_df = pai.DataFrame(df)
            
            suggestion_prompt = f"""
            基于这个数据集和当前查询：'{current_query}'，
            请建议5个相关的后续分析问题，每个问题一行，格式为：
            1. 问题1
            2. 问题2
            ...
            """
            
            suggestions = pai_df.chat(suggestion_prompt)
            
            # 解析建议
            if isinstance(suggestions, str):
                lines = suggestions.strip().split('\n')
                questions = []
                for line in lines:
                    line = line.strip()
                    if line and any(line.startswith(str(i)) for i in range(1, 10)):
                        # 移除序号
                        question = line.split('.', 1)[1].strip() if '.' in line else line
                        questions.append(question)
                return questions[:5]  # 最多返回5个建议
            
            return []
        except Exception as e:
            print(f"建议生成失败: {e}")
            return []
    
    def compare_data_trends(self, df: pd.DataFrame, comparison_request: str) -> str:
        """
        比较数据趋势
        
        Args:
            df: pandas DataFrame
            comparison_request: 比较请求
            
        Returns:
            比较结果
        """
        try:
            pai_df = pai.DataFrame(df)
            result = pai_df.chat(comparison_request)
            return str(result)
        except Exception as e:
            return f"趋势比较失败: {e}" 