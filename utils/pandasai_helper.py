"""
PandasAI集成模块 - 用于数据可视化和进一步分析
"""
import os
import pandas as pd
from typing import Optional, Union
import base64
from io import BytesIO
import sqlite3
import glob

# 尝试导入PandasAI相关模块
try:
    import pandasai as pai
    PANDASAI_AVAILABLE = True
    PANDASAI_ERROR = None
except ImportError as e:
    PANDASAI_AVAILABLE = False
    PANDASAI_ERROR = f"PandasAI核心模块导入失败: {e}"

try:
    from pandasai_openai.openai import OpenAI
    OPENAI_LLM_AVAILABLE = True
    OPENAI_LLM_ERROR = None
except ImportError as e:
    OPENAI_LLM_AVAILABLE = False
    OPENAI_LLM_ERROR = f"OpenAI LLM模块导入失败: {e}"

class PandasAIAnalyzer:
    """PandasAI分析器类"""
    
    def __init__(self, db_path: str):
        """
        初始化PandasAI分析器
        
        Args:
            db_path: SQLite数据库路径
        """
        self.db_path = db_path
        self.analyzer_ready = False
        self.error_message = None
        
        # 检查依赖
        if not PANDASAI_AVAILABLE:
            raise ImportError(f"PandasAI不可用: {PANDASAI_ERROR}")
        
        if not OPENAI_LLM_AVAILABLE:
            raise ImportError(f"OpenAI LLM不可用: {OPENAI_LLM_ERROR}")
        
        try:
            self._setup_pandasai()
            self.analyzer_ready = True
        except Exception as e:
            self.error_message = str(e)
            raise e
    
    def _setup_pandasai(self):
        """设置PandasAI配置"""
        # 使用环境变量中的配置
        api_key = os.environ.get("OPENAI_API_KEY", os.environ.get("DASHSCOPE_API_KEY"))
        api_base = os.environ.get("OPENAI_API_BASE", "https://dashscope.aliyuncs.com/compatible-mode/v1")
        
        if not api_key:
            raise ValueError("未找到API密钥，请设置DASHSCOPE_API_KEY或OPENAI_API_KEY环境变量")
        
        # 配置LLM - 重点修复model参数
        try:
            # 优先使用阿里云百炼API（避免地区限制问题）
            if "dashscope" in api_base.lower():
                llm = OpenAI(
                    api_token=api_key,
                    base_url=api_base,
                    model="qwen-plus"  # 确保使用正确的模型名称
                )
                # 明确设置模型名称
                llm.model = "qwen-plus"
            else:
                # 使用标准OpenAI API
                llm = OpenAI(
                    api_token=api_key,
                    model="gpt-3.5-turbo"  # 使用更稳定的模型
                )
                llm.model = "gpt-3.5-turbo"
                
        except Exception as e:
            print(f"LLM配置警告: {e}")
            # 简化配置作为后备
            llm = OpenAI(api_token=api_key)
            llm.model = "qwen-plus" if "dashscope" in api_base.lower() else "gpt-3.5-turbo"
        
        # 配置PandasAI - 添加更多配置选项避免错误
        try:
            pai.config.set({
                "llm": llm,
                "save_charts": True,
                "save_charts_path": "charts/",
                "verbose": False,  # 减少日志输出
                "enable_cache": True,  # 启用缓存
                "max_retries": 2,  # 限制重试次数
                "response_parser": "python"  # 明确指定解析器
            })
        except Exception as e:
            print(f"警告：PandasAI配置可能不完全成功: {e}")
            # 基本配置
            try:
                pai.config.set({
                    "llm": llm,
                    "verbose": False,
                    "max_retries": 1
                })
            except:
                # 最简配置
                pai.config.llm = llm
    
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
            
            # 添加明确的指导，要求返回文本而不是代码
            enhanced_question = f"""
            请分析数据并回答以下问题：{question}
            
            要求：
            1. 直接给出分析结果，不要生成代码
            2. 用中文回答
            3. 提供具体的数字和洞察
            """
            
            # 使用自然语言进行分析
            result = pai_df.chat(enhanced_question)
            
            return str(result) if result else "分析完成，但没有生成具体结果。"
            
        except Exception as e:
            # 提供降级分析
            return self._provide_basic_analysis(df, question, str(e))
    
    def _provide_basic_analysis(self, df: pd.DataFrame, question: str, error_msg: str) -> str:
        """
        提供基础数据分析（当PandasAI失败时）
        
        Args:
            df: pandas DataFrame
            question: 用户问题
            error_msg: 错误信息
            
        Returns:
            基础分析结果
        """
        try:
            analysis = []
            analysis.append(f"📊 数据基本信息：")
            analysis.append(f"- 数据行数：{len(df)}")
            analysis.append(f"- 数据列数：{len(df.columns)}")
            analysis.append(f"- 列名：{', '.join(df.columns.tolist())}")
            
            # 数值列分析
            numeric_cols = df.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0:
                analysis.append(f"\n📈 数值列统计：")
                for col in numeric_cols[:3]:  # 只显示前3个数值列
                    try:
                        stats = df[col].describe()
                        analysis.append(f"- {col}：平均值 {stats['mean']:.2f}，最大值 {stats['max']:.2f}，最小值 {stats['min']:.2f}")
                    except:
                        pass
            
            # 文本列分析
            text_cols = df.select_dtypes(include=['object']).columns
            if len(text_cols) > 0:
                analysis.append(f"\n📝 文本列信息：")
                for col in text_cols[:3]:  # 只显示前3个文本列
                    try:
                        unique_count = df[col].nunique()
                        analysis.append(f"- {col}：{unique_count} 个不同值")
                    except:
                        pass
            
            # 根据问题类型提供相关分析
            question_lower = question.lower()
            if "最高" in question_lower or "最大" in question_lower:
                if len(numeric_cols) > 0:
                    col = numeric_cols[0]
                    max_row = df.loc[df[col].idxmax()]
                    analysis.append(f"\n🏆 {col}的最大值：{max_row[col]}")
            elif "最低" in question_lower or "最小" in question_lower:
                if len(numeric_cols) > 0:
                    col = numeric_cols[0]
                    min_row = df.loc[df[col].idxmin()]
                    analysis.append(f"\n📉 {col}的最小值：{min_row[col]}")
            elif "趋势" in question_lower:
                analysis.append(f"\n📊 趋势分析需要时间序列数据，当前数据包含 {len(df)} 个数据点")
            
            analysis.append(f"\n⚠️ 注意：由于PandasAI服务暂时不可用（{error_msg[:100]}...），以上是基础统计分析。")
            
            return "\n".join(analysis)
            
        except Exception as e:
            return f"数据分析失败：{e}。数据包含 {len(df)} 行 {len(df.columns)} 列。"
    
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
            insights_prompt = """
            请为这个数据集提供详细的数据洞察分析。要求：
            1. 用中文回答
            2. 不要生成代码，直接给出分析结果
            3. 包括主要统计信息、数据分布、异常值、趋势等
            4. 提供具体的数字和百分比
            """
            
            insights = pai_df.chat(insights_prompt)
            
            return str(insights) if insights else self._generate_basic_insights(df)
            
        except Exception as e:
            print(f"PandasAI洞察生成失败: {e}")
            return self._generate_basic_insights(df)
    
    def _generate_basic_insights(self, df: pd.DataFrame) -> str:
        """
        生成基础数据洞察（当PandasAI失败时）
        
        Args:
            df: pandas DataFrame
            
        Returns:
            基础洞察文本
        """
        try:
            insights = []
            insights.append("📊 **数据洞察报告**\n")
            
            # 基本信息
            insights.append(f"**数据概览：**")
            insights.append(f"- 总记录数：{len(df):,} 行")
            insights.append(f"- 字段数量：{len(df.columns)} 个")
            insights.append(f"- 内存使用：{df.memory_usage(deep=True).sum() / 1024:.1f} KB\n")
            
            # 数据类型分析
            numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
            text_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
            date_cols = df.select_dtypes(include=['datetime']).columns.tolist()
            
            insights.append(f"**数据类型分布：**")
            insights.append(f"- 数值字段：{len(numeric_cols)} 个 ({', '.join(numeric_cols[:3])}{'...' if len(numeric_cols) > 3 else ''})")
            insights.append(f"- 文本字段：{len(text_cols)} 个 ({', '.join(text_cols[:3])}{'...' if len(text_cols) > 3 else ''})")
            if date_cols:
                insights.append(f"- 日期字段：{len(date_cols)} 个 ({', '.join(date_cols)})")
            insights.append("")
            
            # 数值字段统计
            if numeric_cols:
                insights.append(f"**数值字段统计：**")
                for col in numeric_cols[:3]:  # 只分析前3个数值字段
                    try:
                        stats = df[col].describe()
                        null_pct = (df[col].isnull().sum() / len(df)) * 100
                        insights.append(f"- **{col}**：")
                        insights.append(f"  - 平均值：{stats['mean']:.2f}")
                        insights.append(f"  - 中位数：{stats['50%']:.2f}")
                        insights.append(f"  - 标准差：{stats['std']:.2f}")
                        insights.append(f"  - 范围：{stats['min']:.2f} ~ {stats['max']:.2f}")
                        if null_pct > 0:
                            insights.append(f"  - 缺失值：{null_pct:.1f}%")
                    except:
                        insights.append(f"- **{col}**：统计分析失败")
                insights.append("")
            
            # 文本字段分析
            if text_cols:
                insights.append(f"**分类字段分析：**")
                for col in text_cols[:3]:  # 只分析前3个文本字段
                    try:
                        unique_count = df[col].nunique()
                        null_count = df[col].isnull().sum()
                        most_common = df[col].value_counts().head(1)
                        insights.append(f"- **{col}**：")
                        insights.append(f"  - 唯一值数量：{unique_count}")
                        insights.append(f"  - 缺失值：{null_count}")
                        if len(most_common) > 0:
                            insights.append(f"  - 最常见值：{most_common.index[0]} ({most_common.iloc[0]} 次)")
                    except:
                        insights.append(f"- **{col}**：分析失败")
                insights.append("")
            
            # 数据质量评估
            insights.append(f"**数据质量评估：**")
            total_cells = len(df) * len(df.columns)
            null_cells = df.isnull().sum().sum()
            completeness = ((total_cells - null_cells) / total_cells) * 100
            insights.append(f"- 数据完整性：{completeness:.1f}%")
            
            if null_cells > 0:
                insights.append(f"- 缺失值总数：{null_cells:,} 个")
                null_cols = df.columns[df.isnull().any()].tolist()
                insights.append(f"- 有缺失值的字段：{', '.join(null_cols[:5])}{'...' if len(null_cols) > 5 else ''}")
            
            # 异常值检测（简单版本）
            if numeric_cols:
                insights.append("")
                insights.append(f"**异常值检测：**")
                for col in numeric_cols[:2]:  # 只检测前2个数值字段
                    try:
                        Q1 = df[col].quantile(0.25)
                        Q3 = df[col].quantile(0.75)
                        IQR = Q3 - Q1
                        outliers = df[(df[col] < Q1 - 1.5 * IQR) | (df[col] > Q3 + 1.5 * IQR)]
                        outlier_pct = (len(outliers) / len(df)) * 100
                        insights.append(f"- **{col}**：{len(outliers)} 个异常值 ({outlier_pct:.1f}%)")
                    except:
                        pass
            
            insights.append("\n💡 **建议：** 这是基础统计分析。要获得更深入的洞察，建议使用专业的数据分析工具。")
            
            return "\n".join(insights)
            
        except Exception as e:
            return f"无法生成数据洞察：{e}"
    
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
            # 首先尝试使用PandasAI生成建议
            pai_df = pai.DataFrame(df)
            
            suggestion_prompt = f"""
            基于这个数据集和当前查询：'{current_query}'，
            请建议5个相关的后续分析问题。请直接返回问题列表，不要生成代码。
            格式要求：
            1. 问题1
            2. 问题2
            3. 问题3
            4. 问题4
            5. 问题5
            """
            
            # 设置超时和重试限制
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
                        if question:  # 确保问题不为空
                            questions.append(question)
                
                if questions:
                    return questions[:5]  # 最多返回5个建议
            
            # 如果PandasAI失败，使用预定义的智能建议
            return self._generate_fallback_suggestions(df, current_query)
            
        except Exception as e:
            print(f"PandasAI建议生成失败: {e}")
            # 使用降级方案
            return self._generate_fallback_suggestions(df, current_query)
    
    def _generate_fallback_suggestions(self, df: pd.DataFrame, current_query: str) -> list:
        """
        生成降级建议问题（当PandasAI不可用时）
        
        Args:
            df: pandas DataFrame
            current_query: 当前查询
            
        Returns:
            建议问题列表
        """
        # 分析数据特征
        numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
        categorical_columns = df.select_dtypes(include=['object', 'category']).columns.tolist()
        
        suggestions = []
        
        # 基于数据特征生成建议
        if numeric_columns:
            if len(numeric_columns) >= 2:
                suggestions.append(f"分析{numeric_columns[0]}和{numeric_columns[1]}之间的相关性")
            suggestions.append(f"计算{numeric_columns[0]}的统计分布情况")
            suggestions.append(f"识别{numeric_columns[0]}中的异常值或极值")
        
        if categorical_columns:
            suggestions.append(f"按{categorical_columns[0]}进行分组分析")
            if len(categorical_columns) >= 2:
                suggestions.append(f"对比不同{categorical_columns[0]}和{categorical_columns[1]}的表现")
        
        # 基于当前查询内容生成相关建议
        query_lower = current_query.lower()
        if "销售" in query_lower or "收入" in query_lower or "revenue" in query_lower:
            suggestions.extend([
                "分析销售趋势的季节性变化",
                "计算各产品的市场份额占比",
                "识别销售增长最快的产品类别",
                "分析价格与销量的关系",
                "对比不同时间段的销售表现"
            ])
        elif "产品" in query_lower or "product" in query_lower:
            suggestions.extend([
                "分析产品的生命周期阶段",
                "计算产品的平均售价变化",
                "识别最受欢迎的产品特征",
                "分析产品的库存周转率",
                "对比不同产品类别的盈利能力"
            ])
        elif "客户" in query_lower or "customer" in query_lower:
            suggestions.extend([
                "分析客户的购买行为模式",
                "计算客户的生命周期价值",
                "识别高价值客户群体",
                "分析客户流失的主要原因",
                "对比不同客户群体的消费习惯"
            ])
        else:
            # 通用建议
            suggestions.extend([
                "进行数据的趋势分析",
                "计算关键指标的变化率",
                "识别数据中的异常模式",
                "进行分组对比分析",
                "分析数据的分布特征"
            ])
        
        # 去重并限制数量
        unique_suggestions = list(dict.fromkeys(suggestions))  # 保持顺序的去重
        return unique_suggestions[:5]
    
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