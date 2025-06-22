import streamlit as st
import os
import pandas as pd
from dotenv import load_dotenv
from crew_setup import (
    sql_generator_crew, sql_reviewer_crew, sql_compliance_crew,
    query_generator_agent, query_reviewer_agent, compliance_checker_agent
)
from utils.db_simulator import get_structured_schema, run_query, extract_relevant_metadata
from utils.pandasai_helper import PandasAIAnalyzer
import sqlparse
from utils.helper import extract_token_counts, calculate_gpt4o_mini_cost
import base64
from datetime import datetime
import uuid
from typing import List
import json
import traceback
from crewai import Task
import re # <--- 统一导入re模块

# 加载环境变量
load_dotenv()

# 设置阿里云百炼API为OpenAI兼容模式（CrewAI需要）
if os.environ.get("DASHSCOPE_API_KEY"):
    os.environ["OPENAI_API_KEY"] = os.environ.get("DASHSCOPE_API_KEY")
    os.environ["OPENAI_API_BASE"] = "https://dashscope.aliyuncs.com/compatible-mode/v1"

DB_PATH = "data/sample_db.sqlite"

# Cache the schema, but allow clearing it
@st.cache_data(show_spinner=False)
def load_schema():
    return get_structured_schema(DB_PATH)

# 初始化PandasAI分析器
@st.cache_resource
def get_pandasai_analyzer():
    try:
        analyzer = PandasAIAnalyzer(DB_PATH)
        return analyzer
    except ImportError as e:
        st.warning(f"⚠️ PandasAI依赖缺失: {e}")
        st.info("💡 请安装PandasAI相关依赖：pip install pandasai pandasai-openai")
        return None
    except Exception as e:
        st.warning(f"⚠️ PandasAI初始化警告: {e}")
        st.info("💡 PandasAI功能可能受限，但基础分析功能仍可正常使用")
        return None

# 执行SQL查询的函数
def run_query_to_dataframe(query):
    """执行SQL查询并返回DataFrame和文本结果"""
    try:
        import sqlite3
        conn = sqlite3.connect(DB_PATH)
        
        # 处理SQLite不支持的SQL语法
        processed_query = query
        
        # 将PostgreSQL/MySQL的INTERVAL语法转换为SQLite兼容语法
        if "CURRENT_DATE - INTERVAL" in query.upper():
            # 匹配 CURRENT_DATE - INTERVAL 'N days' 模式
            pattern = r"CURRENT_DATE\s*-\s*INTERVAL\s*['\"](\d+)\s*days?['\"]"
            processed_query = re.sub(
                pattern, 
                r"date('now', '-\1 days')", 
                query, 
                flags=re.IGNORECASE
            )
            
        # 将 CURRENT_DATE - INTERVAL '30 days' 转换为 date('now', '-30 days')
        if "- INTERVAL '30 days'" in query:
            processed_query = query.replace(
                "CURRENT_DATE - INTERVAL '30 days'",
                "date('now', '-30 days')"
            )
        
        # 将 CURRENT_DATE - INTERVAL '31 days' 转换为 date('now', '-31 days')
        if "- INTERVAL '31 days'" in query:
            processed_query = query.replace(
                "CURRENT_DATE - INTERVAL '31 days'",
                "date('now', '-31 days')"
            )
        
        st.info(f"🔧 执行查询: {processed_query}")
        
        df = pd.read_sql_query(processed_query, conn)
        conn.close()
        
        # 检查结果
        if df.empty:
            text_result = "查询执行成功，但没有返回数据。"
            st.warning("📭 查询结果为空，可能是筛选条件过于严格或数据不存在。")
        else:
            text_result = df.head().to_string(index=False)
            st.success(f"✅ 查询成功返回 {len(df)} 行数据")
        
        return df, text_result
        
    except Exception as e:
        error_msg = f"查询失败: {e}"
        st.error(f"❌ {error_msg}")
        
        # 提供调试信息
        if "syntax error" in str(e).lower():
            st.info("💡 可能是SQL语法问题，请检查查询语句是否符合SQLite语法规范。")
        elif "no such table" in str(e).lower():
            st.info("💡 表不存在，请检查表名是否正确。")
        elif "no such column" in str(e).lower():
            st.info("💡 字段不存在，请检查字段名是否正确。")
        
        return None, error_msg

# 初始化历史记录结构
def init_session_state():
    """初始化session state"""
    if "analysis_history" not in st.session_state:
        st.session_state["analysis_history"] = []
    if "llm_cost" not in st.session_state:
        st.session_state["llm_cost"] = 0.0
    if "current_cell" not in st.session_state:
        st.session_state["current_cell"] = None
    if "manual_intervention_mode" not in st.session_state:
        st.session_state["manual_intervention_mode"] = False
    if "pending_manual_sql" not in st.session_state:
        st.session_state["pending_manual_sql"] = ""
    if "pending_user_prompt" not in st.session_state:
        st.session_state["pending_user_prompt"] = ""
    if "pending_analysis" not in st.session_state:
        st.session_state["pending_analysis"] = None
    if "generated_sql_info" not in st.session_state:
        st.session_state["generated_sql_info"] = None

def create_analysis_record(user_prompt, generated_sql=None, reviewed_sql=None, 
                         compliance_report=None, query_result=None, 
                         query_dataframe=None, cost=0.0, manual_intervention=False, 
                         manual_sql=None):
    """创建新的分析记录"""
    return {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.now(),  # 确保是datetime对象
        "user_prompt": user_prompt,
        "generated_sql": generated_sql,
        "reviewed_sql": reviewed_sql,
        "compliance_report": compliance_report,
        "query_result": query_result,
        "query_dataframe": query_dataframe,
        "cost": cost,
        "visualizations": [],  # 存储可视化结果
        "analyses": [],  # 存储分析结果
        "manual_intervention": manual_intervention,  # 是否经过人工干预
        "manual_sql": manual_sql  # 人工修正的SQL
    }

def add_to_history(record):
    """添加记录到历史"""
    st.session_state["analysis_history"].append(record)
    # 安全地获取cost字段，如果不存在则默认为0
    cost = record.get("cost", 0.0)
    st.session_state["llm_cost"] += cost

def render_analysis_cell(record, is_current=False):
    """渲染单个分析单元"""
    cell_id = record["id"]
    
    # 创建可折叠的容器
    with st.container():
        # 单元格头部
        col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
        with col1:
            # 添加人工干预标记
            intervention_mark = "🛠️" if record.get("manual_intervention") else "🤖"
            # 确保timestamp是datetime对象
            timestamp = record['timestamp']
            if isinstance(timestamp, str):
                try:
                    timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                except:
                    timestamp = datetime.now()
            elif not isinstance(timestamp, datetime):
                timestamp = datetime.now()
            
            st.markdown(f"**{intervention_mark} [{timestamp.strftime('%H:%M:%S')}]** {record['user_prompt'][:50]}...")
        with col2:
            if st.button("🔄 重新执行", key=f"rerun_{cell_id}"):
                rerun_analysis(record["user_prompt"])
        with col3:
            if st.button("📋 复制", key=f"copy_{cell_id}"):
                st.session_state["current_prompt"] = record["user_prompt"]
                st.success("已复制到输入框")
        with col4:
            if st.button("🗑️ 删除", key=f"delete_{cell_id}"):
                st.session_state["analysis_history"] = [
                    r for r in st.session_state["analysis_history"] if r["id"] != cell_id
                ]
                st.rerun()
        
        # 始终显示查询结果（如果有）
        if record.get("query_result"):
            st.markdown("### 📊 查询结果")
            
            # 显示DataFrame基本信息
            if record.get("query_dataframe") is not None:
                df = record["query_dataframe"]
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("数据行数", len(df))
                with col2:
                    st.metric("数据列数", len(df.columns))
                with col3:
                    st.metric("内存使用", f"{df.memory_usage(deep=True).sum() / 1024:.1f} KB")
                
                # 显示数据表格
                st.dataframe(df, use_container_width=True)
                
                # 提供下载选项
                col1, col2 = st.columns(2)
                with col1:
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="📥 下载CSV",
                        data=csv,
                        file_name=f"query_result_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        key=f"download_csv_{cell_id}"
                    )
                with col2:
                    if st.button("📋 复制数据", key=f"copy_data_{cell_id}"):
                        st.code(df.to_string(index=False))
            else:
                # 如果没有DataFrame，显示文本结果
                st.code(record["query_result"])
        
        # PandasAI交互区域 - 始终显示（如果有数据）
        if record.get("query_dataframe") is not None:
            st.markdown("---")  # 分隔线
            render_pandasai_interface(record)
        
        # SQL详情和其他信息的可折叠区域
        expanded = is_current or st.checkbox("📋 查看SQL详情", key=f"expand_{cell_id}")
        
        if expanded:
            # 如果经过人工干预，优先显示人工修正的信息
            if record.get("manual_intervention"):
                st.info("🛠️ 此查询经过人工干预修正")
                
                # 显示人工修正的SQL
                if record.get("manual_sql"):
                    with st.expander("✏️ 人工修正的SQL", expanded=True):
                        formatted_sql = sqlparse.format(record["manual_sql"], reindent=True, keyword_case='upper')
                        st.code(formatted_sql, language="sql")
            else:
                # 显示生成的SQL
                if record.get("generated_sql"):
                    with st.expander("📝 生成的SQL", expanded=False):
                        formatted_sql = sqlparse.format(record["generated_sql"], reindent=True, keyword_case='upper')
                        st.code(formatted_sql, language="sql")
                
                # 显示审查后的SQL
                if record.get("reviewed_sql") and not record.get("manual_intervention"):
                    with st.expander("✅ 审查后的SQL", expanded=True):
                        formatted_sql = sqlparse.format(record["reviewed_sql"], reindent=True, keyword_case='upper')
                        st.code(formatted_sql, language="sql")
            
            # 显示合规报告
            if record.get("compliance_report"):
                with st.expander("🔒 合规报告", expanded=False):
                    st.markdown(record["compliance_report"])
            
            # 显示成本信息（包含人工干预标记）
            cost_info = f"💰 本次查询成本: ${record['cost']:.6f}"
            if record.get("manual_intervention"):
                cost_info += " (人工干预)"
            st.caption(cost_info)
        
        # 如果没有查询结果，显示状态信息
        if not record.get("query_result"):
            status = record.get("status", "unknown")
            if status == "compliance_failed":
                st.error("❌ 查询未通过合规审查")
                # 显示合规报告详情
                if record.get("compliance_report"):
                    with st.expander("查看合规报告详情"):
                        st.markdown(record["compliance_report"])
            elif status == "error":
                error_msg = record.get("error_message", "未知错误")
                st.error(f"❌ 查询执行出错: {error_msg}")
                # 显示更多错误详情
                st.write(f"🔍 **错误详情**：{error_msg}")
                if "error_details" in record:
                    st.code(record["error_details"])
            elif status == "query_failed":
                error_msg = record.get("error_message", "SQL查询执行失败")
                st.error(f"❌ SQL查询执行失败: {error_msg}")
                # 显示SQL和错误详情
                if record.get("reviewed_sql"):
                    with st.expander("查看失败的SQL"):
                        st.code(record["reviewed_sql"], language="sql")
            elif status == "generating":
                st.info("⏳ 正在生成SQL查询...")
            elif status == "pending_execution":
                st.info("⏳ 等待执行查询...")
            else:
                st.info(f"⏳ 查询正在处理中... (状态: {status})")
                # 显示记录的所有状态信息用于调试
                st.write("🔍 **调试信息 - 记录状态**:")
                debug_info = {
                    "status": record.get("status"),
                    "has_query_result": bool(record.get("query_result")),
                    "has_query_dataframe": record.get("query_dataframe") is not None,
                    "has_error_message": bool(record.get("error_message")),
                    "record_keys": list(record.keys())
                }
                st.json(debug_info)
        
        st.divider()

def render_pandasai_interface(record):
    """渲染PandasAI交互界面"""
    analyzer = get_pandasai_analyzer()
    
    if record.get("query_dataframe") is None:
        return
    
    df = record["query_dataframe"]
    cell_id = record["id"]
    
    # 突出显示的PandasAI标题
    st.markdown("""
    <div style="background: linear-gradient(90deg, #ff6b6b, #4ecdc4); padding: 15px; border-radius: 10px; margin: 20px 0;">
        <h2 style="color: white; margin: 0; text-align: center;">
            🤖 PandasAI 智能数据分析与可视化平台
        </h2>
        <p style="color: white; margin: 5px 0 0 0; text-align: center; opacity: 0.9;">
            使用自然语言与您的数据对话，生成图表和深度洞察
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # 检查PandasAI状态
    if not analyzer:
        st.error("🚫 PandasAI模块未能正常初始化")
        st.markdown("""
        **可能的解决方案：**
        1. 检查环境变量配置（DASHSCOPE_API_KEY或OPENAI_API_KEY）
        2. 安装缺失的依赖：`pip install pandasai pandasai-openai`
        3. 重启应用程序
        
        **替代方案：**
        - 您仍可以使用基础SQL查询功能
        - 可以导出数据到Excel进行手动分析
        - 查询结果可以复制到其他可视化工具
        """)
        
        # 提供基础数据导出功能
        st.markdown("### 📊 基础数据操作")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📥 下载CSV", key=f"download_csv_{cell_id}"):
                csv = df.to_csv(index=False)
                st.download_button(
                    label="点击下载CSV文件",
                    data=csv,
                    file_name=f"data_export_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key=f"csv_download_{cell_id}"
                )
        with col2:
            if st.button("📋 复制数据", key=f"copy_data_{cell_id}"):
                st.code(df.to_string(index=False))
        
        return
    
    # PandasAI正常工作时的界面
    # 快速功能入口
    st.markdown("### 🚀 快速开始")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("📊 生成图表", key=f"quick_chart_{cell_id}", help="快速生成数据可视化"):
            st.session_state[f"active_tab_{cell_id}"] = 0
    with col2:
        if st.button("🔍 数据洞察", key=f"quick_insight_{cell_id}", help="获取自动数据洞察"):
            st.session_state[f"active_tab_{cell_id}"] = 2
    with col3:
        if st.button("❓ 智能问答", key=f"quick_qa_{cell_id}", help="对数据进行问答"):
            st.session_state[f"active_tab_{cell_id}"] = 1
    with col4:
        if st.button("💡 建议问题", key=f"quick_suggest_{cell_id}", help="获取分析建议"):
            st.session_state[f"active_tab_{cell_id}"] = 3
    
    # 创建标签页
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 数据可视化", 
        "🔍 智能问答", 
        "💡 自动洞察", 
        "❓ 建议问题"
    ])
    
    with tab1:
        st.markdown("#### 🎨 可视化创作工厂")
        st.markdown("**用自然语言描述您想要的图表，AI将为您创建**")
        
        # 常用图表类型快捷按钮
        st.markdown("**快捷图表类型：**")
        chart_types = [
            ("条形图", "用条形图展示数据"),
            ("折线图", "用折线图显示趋势"),
            ("饼图", "用饼图显示占比"),
            ("散点图", "用散点图分析关系"),
            ("热力图", "用热力图显示分布")
        ]
        
        cols = st.columns(len(chart_types))
        for i, (chart_name, chart_desc) in enumerate(chart_types):
            with cols[i]:
                if st.button(chart_name, key=f"chart_type_{cell_id}_{i}"):
                    st.session_state[f"chart_request_{cell_id}"] = chart_desc
        
        chart_request = st.text_input(
            "📝 描述您想要的图表：", 
            placeholder="例如：将销售数据用条形图可视化，按产品分组",
            key=f"chart_request_{cell_id}",
            value=st.session_state.get(f"chart_request_{cell_id}", "")
        )
        
        if st.button("🎨 生成图表", key=f"gen_chart_{cell_id}", type="primary") and chart_request:
            with st.spinner("🎨 正在为您创建精美图表..."):
                chart_result = analyzer.create_visualization(df, chart_request)
                
                if chart_result:
                    # 保存到记录中
                    record["visualizations"].append({
                        "timestamp": datetime.now(),
                        "request": chart_request,
                        "result": chart_result
                    })
                    
                    if chart_result["type"] == "image":
                        st.success("🎉 " + chart_result["message"])
                        st.image(
                            base64.b64decode(chart_result["base64"]), 
                            caption="PandasAI生成的图表",
                            use_container_width=True
                        )
                        
                        st.download_button(
                            label="📥 下载图表",
                            data=base64.b64decode(chart_result["base64"]),
                            file_name=f"chart_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.png",
                            mime="image/png",
                            key=f"download_chart_{cell_id}_{len(record['visualizations'])}"
                        )
                        
                    elif chart_result["type"] == "text":
                        st.info("📄 " + chart_result["message"])
                        st.write(chart_result["content"])
                        
                    elif chart_result["type"] == "error":
                        st.error("❌ " + chart_result["message"])
                        st.write(chart_result["content"])
                else:
                    st.warning("⚠️ 图表生成失败，尝试进行数据分析...")
                    with st.spinner("🔍 转换为数据分析..."):
                        analysis_result = analyzer.analyze_with_natural_language(df, chart_request)
                        st.write("**分析结果：**")
                        st.write(analysis_result)
        
        # 显示历史可视化
        if record.get("visualizations"):
            st.markdown("#### 📈 历史图表库")
            for i, viz in enumerate(record["visualizations"]):
                with st.expander(f"🎨 {viz['request'][:40]}... ({viz['timestamp'].strftime('%H:%M:%S')})"):
                    if viz["result"]["type"] == "image":
                        st.image(
                            base64.b64decode(viz["result"]["base64"]), 
                            caption=viz["request"],
                            use_container_width=True
                        )
                    else:
                        st.write(viz["result"]["content"])
    
    with tab2:
        st.markdown("#### 🤖 智能数据问答")
        st.markdown("**向您的数据提问，获得准确答案**")
        
        # 常见问题快捷按钮
        common_questions = [
            "数据的基本统计信息是什么？",
            "哪个值最大？哪个值最小？",
            "数据中有什么趋势？",
            "有哪些异常值或特殊模式？",
            "不同类别之间有什么差异？"
        ]
        
        st.markdown("**常见问题：**")
        for i, question in enumerate(common_questions):
            if st.button(f"❓ {question}", key=f"common_q_{cell_id}_{i}"):
                st.session_state[f"analysis_question_{cell_id}"] = question
        
        analysis_question = st.text_input(
            "🤔 向数据提问：",
            placeholder="例如：哪个产品的销售额最高？销售趋势如何？",
            key=f"analysis_question_{cell_id}",
            value=st.session_state.get(f"analysis_question_{cell_id}", "")
        )
        
        if st.button("🔍 分析数据", key=f"analyze_{cell_id}", type="primary") and analysis_question:
            with st.spinner("🤖 AI正在分析您的数据..."):
                analysis_result = analyzer.analyze_with_natural_language(df, analysis_question)
                
                # 保存到记录中
                record["analyses"].append({
                    "timestamp": datetime.now(),
                    "question": analysis_question,
                    "result": analysis_result
                })
                
                st.success("✅ 分析完成！")
                st.markdown("**🎯 分析结果：**")
                st.write(analysis_result)
        
        # 显示历史分析
        if record.get("analyses"):
            st.markdown("#### 📚 历史问答记录")
            for i, analysis in enumerate(record["analyses"]):
                with st.expander(f"🔍 {analysis['question'][:40]}... ({analysis['timestamp'].strftime('%H:%M:%S')})"):
                    st.markdown("**问题：**")
                    st.write(analysis["question"])
                    st.markdown("**答案：**")
                    st.write(analysis["result"])
    
    with tab3:
        st.markdown("#### 💡 自动数据洞察")
        st.markdown("**AI自动发现数据中的关键洞察和模式**")
        
        if st.button("🔮 获取数据洞察", key=f"insights_{cell_id}", type="primary"):
            with st.spinner("🔍 AI正在深度分析数据模式..."):
                insights = analyzer.get_data_insights(df)
                st.success("🎯 洞察生成完成！")
                st.markdown("### 📊 数据洞察报告")
                st.markdown(insights)
    
    with tab4:
        st.markdown("#### 🎯 智能分析建议")
        st.markdown("**基于当前数据特征，AI推荐您可以探索的问题**")
        
        if st.button("💭 获取分析建议", key=f"suggestions_{cell_id}", type="primary"):
            with st.spinner("💡 AI正在生成个性化分析建议..."):
                suggestions = analyzer.suggest_next_questions(df, record["user_prompt"])
                if suggestions:
                    st.success("🎉 分析建议已生成！")
                    st.markdown("### 🔍 推荐分析方向")
                    for i, suggestion in enumerate(suggestions, 1):
                        col1, col2 = st.columns([4, 1])
                        with col1:
                            st.write(f"{i}. {suggestion}")
                        with col2:
                            if st.button("试试看", key=f"try_suggestion_{cell_id}_{i}"):
                                st.session_state[f"analysis_question_{cell_id}"] = suggestion
                                st.rerun()
                else:
                    st.info("💭 暂时没有特别的建议，您可以尝试在其他标签页中探索数据！")
    
    # 底部功能提示
    st.markdown("---")
    st.markdown("""
    <div style="background-color: #f0f2f6; padding: 10px; border-radius: 5px; margin-top: 20px;">
        <h4 style="margin: 0; color: #1f77b4;">💡 PandasAI 使用提示</h4>
        <p style="margin: 5px 0 0 0; color: #666;">
            • <strong>数据可视化</strong>：用自然语言描述想要的图表类型和样式<br>
            • <strong>智能问答</strong>：直接向数据提问，获得准确的分析结果<br>
            • <strong>自动洞察</strong>：让AI发现数据中的隐藏模式和趋势<br>
            • <strong>分析建议</strong>：获得个性化的数据探索方向建议
        </p>
    </div>
    """, unsafe_allow_html=True)

def rerun_analysis(user_prompt):
    """重新执行分析"""
    st.session_state["current_prompt"] = user_prompt
    # 触发新的分析
    execute_new_analysis(user_prompt)

def enter_manual_intervention_mode(user_prompt, generated_sql):
    """进入人工干预模式"""
    st.session_state["manual_intervention_mode"] = True
    st.session_state["pending_user_prompt"] = user_prompt
    st.session_state["pending_manual_sql"] = generated_sql

def process_manual_sql(manual_sql: str, user_request: str):
    """处理手动输入的SQL"""
    if manual_sql and manual_sql.strip():
        st.write("### 🔍 SQL代码审查")
        
        with st.spinner("🔍 正在进行SQL代码审查..."):
            # 使用智能任务创建函数
            review_task = create_sql_review_task(manual_sql)
            
            # 创建临时的Crew来执行这个任务
            from crewai import Crew
            temp_crew = Crew(
                agents=[query_reviewer_agent],
                tasks=[review_task],
                verbose=True
            )
            review_result = temp_crew.kickoff()
            
            # 提取审查后的SQL
            reviewed_sql = extract_sql_from_response(str(review_result))
        
        # 显示审查后的SQL
        st.code(reviewed_sql, language="sql")
        
        # 合规性审查
        st.write("### 🛡️ 数据合规性审查")
        
        with st.spinner("🛡️ 正在进行合规性审查..."):
            compliance_task = Task(
                description=f"""请对以下SQL查询进行数据安全与合规性审查：
**待审查的SQL查询：**
{reviewed_sql}
**审查维度：**
1. **个人敏感信息(PII)保护**：检查是否可能泄露个人身份信息
2. **数据访问权限**：验证查询是否符合数据访问控制策略  
3. **合规风险**：识别可能违反数据保护法规的操作
4. **数据脱敏**：检查敏感数据是否需要脱敏处理
5. **操作安全性**：确保查询不会造成数据损坏或系统风险
**审查标准：**
- 严格按照数据治理政策执行
- 识别所有潜在的合规风险点
- 提供具体的风险缓解建议
- 给出明确的合规评估结论""",
                expected_output="JSON格式的合规审查报告，包含report字段",
                agent=compliance_checker_agent
            )
            
            # 创建临时的Crew来执行这个任务
            temp_crew = Crew(
                agents=[compliance_checker_agent],
                tasks=[compliance_task],
                verbose=True
            )
            compliance_result = temp_crew.kickoff()
            
            try:
                compliance_data = json.loads(str(compliance_result))
                compliance_report = compliance_data.get("report", str(compliance_result))
            except:
                compliance_report = str(compliance_result)
        
        # 显示合规报告
        with st.expander("📋 查看详细合规报告", expanded=False):
            st.markdown(compliance_report)
        
        # 创建查询记录 - 使用create_analysis_record函数确保所有字段都存在
        record = create_analysis_record(
            user_prompt=user_request,
            generated_sql=manual_sql,
            reviewed_sql=reviewed_sql,
            compliance_report=compliance_report,
            cost=0.0,
            manual_intervention=True,
            manual_sql=manual_sql
        )
        st.session_state["current_cell"] = record["id"]
        record["status"] = "pending_execution"
        
        # 执行查询（如果合规）
        compliance_lower = str(compliance_report).lower()
        is_compliant = (
            "合规通过" in str(compliance_report) or 
            "compliant" in compliance_lower or
            ("合规" in str(compliance_report) and "不合规" not in str(compliance_report) and "违规" not in str(compliance_report))
        )
        
        if is_compliant:
            with st.spinner("📊 执行查询..."):
                df, text_result = run_query_to_dataframe(reviewed_sql)
                record["query_result"] = text_result
                record["query_dataframe"] = df
                
                if df is not None:
                    st.success("🎉 查询执行成功！数据已准备就绪，可以使用PandasAI进行进一步分析。")
                    record["status"] = "completed"
                else:
                    # 查询失败，text_result包含错误信息
                    st.warning("⚠️ 查询执行失败，请检查SQL语句。")
                    record["status"] = "query_failed"
                    # 从text_result中提取具体的错误信息
                    if text_result and "查询失败:" in text_result:
                        record["error_message"] = text_result
                    else:
                        record["error_message"] = text_result or "SQL查询执行失败，但未返回具体错误信息"
        else:
            st.error("❌ 查询未通过合规审查，无法执行。请查看合规报告了解详情。")
            record["status"] = "compliance_failed"
        
        # 添加到历史记录
        st.write(f"🔍 **调试信息**：准备添加记录到历史，记录状态 = {record.get('status', 'unknown')}")
        try:
            add_to_history(record)
            st.write("🔍 **调试信息**：记录已添加到历史")
        except Exception as history_error:
            st.error(f"❌ 添加记录到历史时发生错误: {history_error}")
            st.write(f"🔍 **调试信息**：历史记录错误类型 = {type(history_error)}")
            st.write(f"🔍 **调试信息**：历史记录错误详情 = {str(history_error)}")
            # 即使添加历史失败，也要保存错误信息到记录中
            record["status"] = "error"
            record["error_message"] = f"历史记录保存失败: {history_error}"
            record["error_details"] = traceback.format_exc()
            # 尝试再次添加到历史
            try:
                st.session_state["analysis_history"].append(record)
            except:
                st.error("❌ 无法保存到历史记录")
        
        # 显示查询结果
        if record.get("query_dataframe") is not None:
            st.write("🔍 **调试信息**：准备显示查询结果")
            try:
                display_query_results(record["query_dataframe"], record["query_result"])
            except Exception as display_error:
                st.error(f"❌ 显示查询结果时发生错误: {display_error}")
                st.write(f"🔍 **调试信息**：显示错误详情 = {str(display_error)}")
        else:
            st.write(f"🔍 **调试信息**：无查询结果显示，记录状态 = {record.get('status', 'unknown')}")
            # 显示记录的完整内容用于调试
            st.write("🔍 **调试信息 - 完整记录内容**:")
            debug_record = {k: str(v)[:200] + "..." if len(str(v)) > 200 else v 
                          for k, v in record.items() if k not in ['query_dataframe']}
            st.json(debug_record)

def execute_new_analysis(user_prompt):
    """执行新的数据分析"""
    if not user_prompt.strip():
        st.warning("请输入您的数据分析需求。")
        return
    
    # 创建新的分析记录 - 使用create_analysis_record函数确保所有字段都存在
    record = create_analysis_record(
        user_prompt=user_prompt,
        cost=0.0
    )
    st.session_state["current_cell"] = record["id"]
    record["status"] = "generating"
    
    try:
        # Step 1: SQL生成
        st.write("### 🤖 Step 1: 智能SQL生成")
        with st.spinner("🤖 正在生成SQL查询..."):
            # 使用智能任务创建函数
            generation_task = create_sql_generation_task(user_prompt)
            
            # 创建临时的Crew来执行这个任务
            from crewai import Crew
            temp_crew = Crew(
                agents=[query_generator_agent],
                tasks=[generation_task],
                verbose=True
            )
            generation_result = temp_crew.kickoff()
            
            # 提取SQL查询
            raw_sql = extract_sql_from_response(str(generation_result))
            record["generated_sql"] = raw_sql
        
        # 显示生成的SQL
        if raw_sql:
            st.write("**生成的SQL查询：**")
            formatted_sql = sqlparse.format(raw_sql, reindent=True, keyword_case='upper')
            st.code(formatted_sql, language="sql")
            # 继续处理，传递record对象
            continue_with_generated_sql(raw_sql, user_prompt, record)
        else:
            st.error("❌ SQL生成失败")
            record["status"] = "error"
            add_to_history(record)
        
    except Exception as e:
        st.error(f"❌ 分析过程中发生错误: {e}")
        record["status"] = "error"
        # 即使出错也要保存记录
        add_to_history(record)

def continue_with_generated_sql(generated_sql: str, user_request: str, record: dict):
    """继续处理生成的SQL"""
    try:
        st.write("🔍 **调试信息**：开始处理生成的SQL")
        st.write(f"🔍 **调试信息**：生成的SQL长度 = {len(generated_sql) if generated_sql else 0}")
        
        if generated_sql and generated_sql.strip():
            st.write("### 🔍 Step 2: SQL代码审查")
            
            with st.spinner("🔍 正在进行SQL代码审查..."):
                st.write("🔍 **调试信息**：开始SQL审查")
                # 使用智能任务创建函数
                review_task = create_sql_review_task(generated_sql)
                
                # 创建临时的Crew来执行这个任务
                from crewai import Crew
                temp_crew = Crew(
                    agents=[query_reviewer_agent],
                    tasks=[review_task],
                    verbose=True
                )
                review_result = temp_crew.kickoff()
                
                st.write(f"🔍 **调试信息**：SQL审查原始结果 = {str(review_result)[:200]}...")
                
                # 提取审查后的SQL
                reviewed_sql = extract_sql_from_response(str(review_result))
                st.write("🔍 **调试信息**：SQL审查完成")
                st.write(f"🔍 **调试信息**：提取的SQL = {reviewed_sql[:200] if reviewed_sql else 'None'}...")
            
            record["reviewed_sql"] = reviewed_sql
            st.write(f"🔍 **调试信息**：审查后的SQL长度 = {len(reviewed_sql) if reviewed_sql else 0}")
            
            # 检查SQL提取是否成功
            if not reviewed_sql or not reviewed_sql.strip():
                st.error("❌ SQL提取失败，无法继续执行")
                st.write("🔍 **调试信息**：SQL提取失败，原始审查结果：")
                st.code(str(review_result))
                record["status"] = "error"
                record["error_message"] = "SQL提取失败"
                add_to_history(record)
                return
            
            # 显示审查后的SQL
            st.code(reviewed_sql, language="sql")
            
            # Step 3: 合规性审查
            st.write("### 🛡️ Step 3: 数据合规性审查")
            
            with st.spinner("🛡️ 正在进行合规性审查..."):
                st.write("🔍 **调试信息**：开始合规审查")
                compliance_task = Task(
                    description=f"""请对以下SQL查询进行数据安全与合规性审查：
**待审查的SQL查询：**
{reviewed_sql}
**审查维度：**
1. **个人敏感信息(PII)保护**：检查是否可能泄露个人身份信息
2. **数据访问权限**：验证查询是否符合数据访问控制策略  
3. **合规风险**：识别可能违反数据保护法规的操作
4. **数据脱敏**：检查敏感数据是否需要脱敏处理
5. **操作安全性**：确保查询不会造成数据损坏或系统风险
**审查标准：**
- 严格按照数据治理政策执行
- 识别所有潜在的合规风险点
- 提供具体的风险缓解建议
- 给出明确的合规评估结论""",
                    expected_output="JSON格式的合规审查报告，包含report字段",
                    agent=compliance_checker_agent
                )
                
                # 创建临时的Crew来执行这个任务
                temp_crew = Crew(
                    agents=[compliance_checker_agent],
                    tasks=[compliance_task],
                    verbose=True
                )
                compliance_result = temp_crew.kickoff()
                
                try:
                    compliance_data = json.loads(str(compliance_result))
                    compliance_report = compliance_data.get("report", str(compliance_result))
                except:
                    compliance_report = str(compliance_result)
                
                st.write("🔍 **调试信息**：合规审查完成")
                
            record["compliance_report"] = compliance_report
        
            # 显示合规报告
            with st.expander("📋 查看详细合规报告", expanded=False):
                st.markdown(compliance_report)
            
            record["status"] = "pending_execution"
            st.write("🔍 **调试信息**：设置状态为 pending_execution")
            
            # Step 4: 执行查询（如果合规）
            compliance_lower = str(compliance_report).lower()
            # 更准确的合规判断逻辑
            is_compliant = (
                "合规通过" in str(compliance_report) or 
                "compliant" in compliance_lower or
                ("合规" in str(compliance_report) and "不合规" not in str(compliance_report) and "违规" not in str(compliance_report))
            )
            
            st.write(f"🔍 **调试信息**：合规判断结果 = {is_compliant}")
            st.write(f"🔍 **调试信息**：合规报告内容 = {compliance_report[:200]}...")
            
            if is_compliant:
                st.write("🔍 **调试信息**：开始执行查询")
                st.write("### 📊 Step 4: 执行查询")
                with st.spinner("📊 执行查询..."):
                    try:
                        st.write(f"🔍 **调试信息**：准备执行SQL = {reviewed_sql[:100]}...")
                        st.write("🔍 **调试信息**：调用 run_query_to_dataframe 函数")
                        df, text_result = run_query_to_dataframe(reviewed_sql)
                        st.write(f"🔍 **调试信息**：查询执行完成")
                        st.write(f"🔍 **调试信息**：df类型 = {type(df)}")
                        st.write(f"🔍 **调试信息**：df为空 = {df is None}")
                        st.write(f"🔍 **调试信息**：text_result = {text_result[:200] if text_result else 'None'}...")
                        
                        st.write("🔍 **调试信息**：保存查询结果到记录")
                        record["query_result"] = text_result
                        record["query_dataframe"] = df
                        
                        # 如果查询成功，显示成功信息
                        if df is not None:
                            st.write("🔍 **调试信息**：查询成功，设置状态为 completed")
                            st.success("🎉 查询执行成功！数据已准备就绪，可以使用PandasAI进行进一步分析。")
                            record["status"] = "completed"
                        else:
                            st.write("🔍 **调试信息**：查询失败，df为None，设置状态为 query_failed")
                            # 查询失败，text_result包含错误信息
                            st.error(f"❌ 查询执行失败: {text_result}")
                            record["status"] = "query_failed"
                            # 从text_result中提取具体的错误信息
                            if text_result and "查询失败:" in text_result:
                                record["error_message"] = text_result
                            else:
                                record["error_message"] = text_result or "SQL查询执行失败，但未返回具体错误信息"
                            st.write(f"🔍 **调试信息**：设置错误信息 = {record['error_message']}")
                            
                    except Exception as query_error:
                        st.write("🔍 **调试信息**：查询过程中发生异常")
                        error_msg = f"查询执行过程中发生错误: {query_error}"
                        st.error(f"❌ {error_msg}")
                        st.write(f"🔍 **调试信息**：查询错误类型 = {type(query_error)}")
                        st.write(f"🔍 **调试信息**：查询错误详情 = {str(query_error)}")
                        record["status"] = "query_failed"
                        record["error_message"] = error_msg
            else:
                st.write("🔍 **调试信息**：合规检查未通过")
                st.error("❌ 查询未通过合规审查，无法执行。请查看合规报告了解详情。")
                record["status"] = "compliance_failed"
        
        st.write("🔍 **调试信息**：准备添加记录到历史")
        st.write(f"🔍 **调试信息**：当前记录状态 = {record.get('status', 'unknown')}")
        st.write(f"🔍 **调试信息**：记录包含的字段 = {list(record.keys())}")
        
        # 添加到历史记录
        try:
            add_to_history(record)
            st.write("🔍 **调试信息**：记录已添加到历史")
        except Exception as history_error:
            st.error(f"❌ 添加记录到历史时发生错误: {history_error}")
            st.write(f"🔍 **调试信息**：历史记录错误类型 = {type(history_error)}")
            st.write(f"🔍 **调试信息**：历史记录错误详情 = {str(history_error)}")
            # 即使添加历史失败，也要保存错误信息到记录中
            record["status"] = "error"
            record["error_message"] = f"历史记录保存失败: {history_error}"
            record["error_details"] = traceback.format_exc()
            # 尝试再次添加到历史
            try:
                st.session_state["analysis_history"].append(record)
            except:
                st.error("❌ 无法保存到历史记录")
        
        # 显示查询结果
        if record.get("query_dataframe") is not None:
            st.write("🔍 **调试信息**：准备显示查询结果")
            try:
                display_query_results(record["query_dataframe"], record["query_result"])
            except Exception as display_error:
                st.error(f"❌ 显示查询结果时发生错误: {display_error}")
                st.write(f"🔍 **调试信息**：显示错误详情 = {str(display_error)}")
        else:
            st.write(f"🔍 **调试信息**：无查询结果显示，记录状态 = {record.get('status', 'unknown')}")
            # 如果查询失败，显示错误信息
            if record.get("status") == "query_failed":
                st.error(f"❌ 查询执行失败: {record.get('error_message', '未知错误')}")
                # 显示失败的SQL
                if record.get("reviewed_sql"):
                    with st.expander("查看失败的SQL"):
                        st.code(record["reviewed_sql"], language="sql")
            elif record.get("status") == "error":
                st.error(f"❌ 处理过程中发生错误: {record.get('error_message', '未知错误')}")
            
            # 显示记录的完整内容用于调试
            st.write("🔍 **调试信息 - 完整记录内容**:")
            debug_record = {k: str(v)[:200] + "..." if len(str(v)) > 200 else v 
                          for k, v in record.items() if k not in ['query_dataframe']}
            st.json(debug_record)
        
        st.write("🔍 **调试信息**：continue_with_generated_sql 函数执行完成")
        
    except Exception as e:
        st.write("🔍 **调试信息**：外层异常捕获")
        error_msg = f"查询处理过程中发生错误: {e}"
        st.error(f"❌ {error_msg}")
        st.write(f"🔍 **调试信息**：外层异常类型 = {type(e)}")
        st.write(f"🔍 **调试信息**：外层异常详情 = {str(e)}")
        
        # 打印完整的错误堆栈
        st.code(traceback.format_exc())
        
        record["status"] = "error"
        record["error_message"] = error_msg
        add_to_history(record)

def create_sql_generation_task(user_request: str) -> Task:
    """创建SQL生成任务"""
    # 使用智能元数据筛选，只提供相关的表信息
    relevant_metadata = extract_relevant_metadata(user_request, DB_PATH)
    
    return Task(
        description=f"""
**数据库架构信息：**
{relevant_metadata}

**用户需求：**
{user_request}

**重要规则：**
1. 首先分析用户需求，明确需要使用的表和字段
2. 严格按照提供的数据库架构生成SQL查询
3. 只能使用架构中存在的表名和字段名，不得自创
4. 如果用户需求无法通过现有架构满足，请用SQL注释（以--开头）说明原因
5. 确保生成的查询语句准确反映用户意图
6. 优先考虑查询性能和结果准确性
7. 使用清晰的字段别名和适当的排序规则
8. **数据库类型为SQLite，请使用SQLite语法**
9. **日期函数使用 date('now', '-N days') 格式，不要使用DATEADD或GETDATE**

**输出格式要求：**
- 先列出将要使用的表和字段
- 然后提供完整的SQL查询语句
- 添加必要的注释说明查询逻辑
        """,
        expected_output="JSON格式的SQL查询结果，包含sqlquery字段",
        agent=query_generator_agent
    )

def create_sql_review_task(sql_query: str) -> Task:
    """创建SQL审查任务"""
    # 从SQL查询中提取相关表信息
    tables_in_query = extract_tables_from_sql(sql_query)
    user_query_context = f"SQL查询涉及的表: {', '.join(tables_in_query)}"
    relevant_metadata = extract_relevant_metadata(user_query_context, DB_PATH)
    
    return Task(
        description=f"""
**数据库架构信息：**
{relevant_metadata}

**待审查的SQL查询：**
{sql_query}

**审查要点：**
1. **语法正确性**：检查SQL语法是否正确
2. **架构匹配性**：确认所有表名和字段名都存在于提供的架构中
3. **查询逻辑**：验证查询逻辑是否符合预期
4. **性能优化**：识别可能的性能问题和优化机会
5. **代码规范**：检查代码风格和可读性
6. **安全性**：确保查询不存在SQL注入等安全风险
7. **SQLite兼容性**：确保使用SQLite支持的语法

**审查规则：**
- 不得添加架构中不存在的表或字段
- 如果查询已经正确，请保持原样
- 如果发现问题但无法修复，请用SQL注释说明原因
- 优化建议应该具体可行
- **重点检查日期函数语法，确保使用SQLite格式**
        """,
        expected_output="JSON格式的审查结果，包含reviewed_sqlquery字段",
        agent=query_reviewer_agent
    )

def extract_tables_from_sql(sql_query: str) -> List[str]:
    """
    从SQL查询中提取表名
    
    Args:
        sql_query: SQL查询语句
        
    Returns:
        表名列表
    """
    # 移除注释
    sql_clean = re.sub(r'--.*$', '', sql_query, flags=re.MULTILINE)
    sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)
    
    # 提取FROM和JOIN后的表名
    table_pattern = r'(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)'
    matches = re.findall(table_pattern, sql_clean, re.IGNORECASE)
    
    # 去重并返回
    return list(set(matches))

def display_query_results(df, text_result):
    """显示查询结果"""
    if df is not None and not df.empty:
        st.subheader("📊 查询结果")
        
        # 显示数据概览
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("数据行数", len(df))
        with col2:
            st.metric("数据列数", len(df.columns))
        with col3:
            st.metric("内存使用", f"{df.memory_usage(deep=True).sum() / 1024:.1f} KB")
        
        # 显示数据表格
        st.dataframe(df, use_container_width=True)
        
        # 提供下载选项
        col1, col2 = st.columns(2)
        with col1:
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 下载CSV",
                data=csv,
                file_name=f"query_result_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        with col2:
            if st.button("📋 复制数据"):
                st.code(df.to_string(index=False))
    else:
        st.warning("📭 查询结果为空")

def extract_sql_from_response(response_text: str) -> str:
    """
    从AI代理的响应中提取纯净的SQL查询语句
    
    Args:
        response_text: AI代理的完整响应文本
        
    Returns:
        纯净的SQL查询语句
    """
    try:
        st.write(f"🔍 **SQL提取调试**：开始提取SQL，原始响应长度 = {len(response_text)}")
        st.write(f"🔍 **SQL提取调试**：响应前200字符 = {response_text[:200]}...")
        
        # 首先尝试解析JSON格式的响应
        if response_text.strip().startswith('{'):
            st.write("🔍 **SQL提取调试**：检测到JSON格式响应")
            try:
                response_data = json.loads(response_text)
                st.write(f"🔍 **SQL提取调试**：JSON解析成功，包含字段 = {list(response_data.keys())}")
                # 尝试不同的可能字段名
                for field in ['sqlquery', 'reviewed_sqlquery', 'sql_query', 'query']:
                    if field in response_data:
                        sql_content = response_data[field]
                        st.write(f"🔍 **SQL提取调试**：从字段 '{field}' 提取到SQL，长度 = {len(sql_content) if sql_content else 0}")
                        cleaned_sql = clean_sql_content(sql_content)
                        st.write(f"🔍 **SQL提取调试**：清理后SQL = {cleaned_sql[:200] if cleaned_sql else 'None'}...")
                        return cleaned_sql
                st.write("🔍 **SQL提取调试**：JSON中未找到SQL字段")
            except json.JSONDecodeError as e:
                st.write(f"🔍 **SQL提取调试**：JSON解析失败 = {e}")
        
        # 如果包含```json标记，提取JSON内容
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', response_text, re.DOTALL)
        if json_match:
            st.write("🔍 **SQL提取调试**：检测到JSON代码块")
            try:
                json_content = json_match.group(1)
                response_data = json.loads(json_content)
                st.write(f"🔍 **SQL提取调试**：JSON代码块解析成功，包含字段 = {list(response_data.keys())}")
                for field in ['sqlquery', 'reviewed_sqlquery', 'sql_query', 'query']:
                    if field in response_data:
                        sql_content = response_data[field]
                        st.write(f"🔍 **SQL提取调试**：从JSON代码块字段 '{field}' 提取到SQL")
                        cleaned_sql = clean_sql_content(sql_content)
                        st.write(f"🔍 **SQL提取调试**：清理后SQL = {cleaned_sql[:200] if cleaned_sql else 'None'}...")
                        return cleaned_sql
                st.write("🔍 **SQL提取调试**：JSON代码块中未找到SQL字段")
            except json.JSONDecodeError as e:
                st.write(f"🔍 **SQL提取调试**：JSON代码块解析失败 = {e}")
        
        # 如果包含SQL代码块，直接提取
        sql_match = re.search(r'```sql\s*(.*?)\s*```', response_text, re.DOTALL)
        if sql_match:
            st.write("🔍 **SQL提取调试**：检测到SQL代码块")
            sql_content = sql_match.group(1)
            cleaned_sql = clean_sql_content(sql_content)
            st.write(f"🔍 **SQL提取调试**：从SQL代码块提取到SQL = {cleaned_sql[:200] if cleaned_sql else 'None'}...")
            return cleaned_sql
        
        # 查找SELECT语句（忽略大小写）
        select_match = re.search(r'(SELECT\s+.*?(?:;|$))', response_text, re.DOTALL | re.IGNORECASE)
        if select_match:
            st.write("🔍 **SQL提取调试**：检测到SELECT语句")
            sql_content = select_match.group(1)
            cleaned_sql = clean_sql_content(sql_content)
            st.write(f"🔍 **SQL提取调试**：从SELECT语句提取到SQL = {cleaned_sql[:200] if cleaned_sql else 'None'}...")
            return cleaned_sql
        
        # 如果都没找到，尝试直接清理原文本
        st.write("🔍 **SQL提取调试**：未找到明确的SQL格式，尝试直接清理原文本")
        cleaned = clean_sql_content(response_text)
        # 如果清理后的文本包含SELECT，则返回
        if 'SELECT' in cleaned.upper():
            st.write(f"🔍 **SQL提取调试**：清理后的文本包含SELECT = {cleaned[:200]}...")
            return cleaned
        
        # 最后尝试从整个响应中提取SQL相关内容
        st.write("🔍 **SQL提取调试**：尝试使用正则表达式模式匹配")
        # 查找可能的SQL语句模式
        sql_patterns = [
            r'SELECT\s+[^;]+;',
            r'WITH\s+[^;]+;',
            r'INSERT\s+[^;]+;',
            r'UPDATE\s+[^;]+;',
            r'DELETE\s+[^;]+;'
        ]
        
        for i, pattern in enumerate(sql_patterns):
            match = re.search(pattern, response_text, re.DOTALL | re.IGNORECASE)
            if match:
                st.write(f"🔍 **SQL提取调试**：模式 {i+1} 匹配成功")
                sql_content = match.group(0)
                cleaned_sql = clean_sql_content(sql_content)
                st.write(f"🔍 **SQL提取调试**：模式匹配提取到SQL = {cleaned_sql[:200] if cleaned_sql else 'None'}...")
                return cleaned_sql
        
        # 如果仍然没有找到，返回清理后的原文本
        st.write("🔍 **SQL提取调试**：所有提取方法都失败，返回清理后的原文本")
        st.write(f"🔍 **SQL提取调试**：最终返回 = {cleaned[:200] if cleaned else 'None'}...")
        return cleaned
        
    except Exception as e:
        st.warning(f"SQL提取过程中出现警告: {e}")
        st.write(f"🔍 **SQL提取调试**：提取过程异常 = {str(e)}")
        cleaned = clean_sql_content(response_text)
        st.write(f"🔍 **SQL提取调试**：异常处理返回 = {cleaned[:200] if cleaned else 'None'}...")
        return cleaned

def clean_sql_content(sql_content: str) -> str:
    """
    清理SQL内容，移除注释和多余的空白
    
    Args:
        sql_content: 原始SQL内容
        
    Returns:
        清理后的SQL语句
    """
    if not sql_content:
        return ""
    
    # 移除JSON引号和转义字符
    sql_content = sql_content.strip().strip('"\'')
    
    # 按行分割并处理
    lines = sql_content.split('\n')
    cleaned_lines = []
    
    for line in lines:
        line = line.strip()
        # 跳过空行和注释行
        if not line or line.startswith('--') or line.startswith('#'):
            continue
        # 移除行内注释
        if '--' in line:
            line = line.split('--')[0].strip()
        if line:
            cleaned_lines.append(line)
    
    # 重新组合SQL
    sql_query = ' '.join(cleaned_lines)
    
    # 移除多余的空白
    sql_query = re.sub(r'\s+', ' ', sql_query).strip()
    
    # 确保以分号结尾
    if sql_query and not sql_query.endswith(';'):
        sql_query += ';'
    
    return sql_query

# === 主应用界面 ===
def main():
    # 初始化
    init_session_state()
    
    # 页面标题和功能介绍
    st.title("🚀 DataCrew AutoPilot - 智能数据分析自动驾驶平台")
    
    # 功能概览
    st.markdown("""
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 20px; border-radius: 15px; margin-bottom: 20px;">
        <h3 style="color: white; margin: 0 0 10px 0; text-align: center;">
            🎯 全自动智能数据分析平台
        </h3>
        <p style="color: white; margin: 0; text-align: center; opacity: 0.9; font-size: 16px;">
            基于 CrewAI + PandasAI 构建的企业级数据分析解决方案<br>
            <strong>自然语言输入 → AI生成SQL → 智能审查 → 安全执行 → 可视化分析</strong>
        </p>
    </div>
    """, unsafe_allow_html=True)
    
    # 核心功能展示
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown("""
        <div style="text-align: center; padding: 15px; background: #f8f9fa; border-radius: 10px; margin-bottom: 10px;">
            <h4 style="margin: 0; color: #2c3e50;">🤖 AI SQL生成</h4>
            <p style="margin: 5px 0 0 0; color: #7f8c8d; font-size: 14px;">自然语言转SQL查询</p>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown("""
        <div style="text-align: center; padding: 15px; background: #f8f9fa; border-radius: 10px; margin-bottom: 10px;">
            <h4 style="margin: 0; color: #2c3e50;">🔍 智能审查</h4>
            <p style="margin: 5px 0 0 0; color: #7f8c8d; font-size: 14px;">多重安全与合规检查</p>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown("""
        <div style="text-align: center; padding: 15px; background: #f8f9fa; border-radius: 10px; margin-bottom: 10px;">
            <h4 style="margin: 0; color: #2c3e50;">🛠️ 人工干预</h4>
            <p style="margin: 5px 0 0 0; color: #7f8c8d; font-size: 14px;">精确控制与优化调整</p>
        </div>
        """, unsafe_allow_html=True)
    with col4:
        st.markdown("""
        <div style="text-align: center; padding: 15px; background: #f8f9fa; border-radius: 10px; margin-bottom: 10px;">
            <h4 style="margin: 0; color: #2c3e50;">📊 可视化分析</h4>
            <p style="margin: 5px 0 0 0; color: #7f8c8d; font-size: 14px;">PandasAI智能图表生成</p>
        </div>
        """, unsafe_allow_html=True)
    
    # 侧边栏
    with st.sidebar:
        st.header("📋 分析历史")
        
        # 计算统计信息
        total_queries = len(st.session_state["analysis_history"])
        manual_interventions = len([r for r in st.session_state["analysis_history"] if r.get("manual_intervention")])
        intervention_rate = (manual_interventions / total_queries * 100) if total_queries > 0 else 0
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("总查询数", total_queries)
        with col2:
            st.metric("人工干预", manual_interventions)
        
        st.metric("总成本", f"${st.session_state['llm_cost']:.6f}")
        if total_queries > 0:
            st.metric("干预率", f"{intervention_rate:.1f}%")
        
        if st.button("🗑️ 清空历史"):
            st.session_state["analysis_history"] = []
            st.session_state["llm_cost"] = 0.0
            st.session_state["current_cell"] = None
            st.session_state["manual_intervention_mode"] = False
            st.session_state["pending_manual_sql"] = ""
            st.session_state["pending_user_prompt"] = ""
            st.session_state["pending_analysis"] = None
            st.session_state["generated_sql_info"] = None
            st.rerun()
        
        st.markdown("---")
        
        # 数据库信息展示
        st.header("📊 数据库概览")
        try:
            import sqlite3
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            # 获取表的统计信息
            tables_info = [
                ("customers", "客户", "👥"),
                ("orders", "订单", "🛒"),
                ("products", "产品", "🛍️"),
                ("employees", "员工", "👨‍💼"),
                ("product_reviews", "评价", "⭐"),
                ("website_sessions", "会话", "📱"),
                ("customer_support_tickets", "工单", "🎯")
            ]
            
            for table_name, display_name, icon in tables_info:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    st.metric(f"{icon} {display_name}", f"{count:,}")
                except:
                    pass
            
            conn.close()
        except Exception as e:
            st.error(f"数据库连接错误: {e}")
        
        st.markdown("---")
        
        # 快速分析入口
        st.header("⚡ 快速分析")
        quick_analyses = [
            ("📊 今日概览", "显示今天的销售概览数据"),
            ("🏆 热销排行", "列出销售额最高的前10个产品"),
            ("👥 客户统计", "统计客户数量按国家分布"),
            ("💰 收入趋势", "分析最近3个月的收入趋势")
        ]
        
        for title, query in quick_analyses:
            if st.button(title, key=f"quick_{title}"):
                st.session_state["current_prompt"] = query
                execute_new_analysis(query)
                st.rerun()
        
        st.markdown("---")
        
        # 显示数据库模式
        with st.expander("🗃️ 完整数据库模式"):
            st.code(load_schema(), language="sql")
        
        if st.button("🔄 刷新模式"):
            load_schema.clear()
            st.success("模式已刷新")
        
        st.markdown("---")
        
        # PandasAI功能状态
        st.header("🤖 PandasAI 功能")
        analyzer = get_pandasai_analyzer()
        if analyzer:
            st.success("✅ PandasAI已就绪")
            st.markdown("""
            **可用功能：**
            - 📊 智能图表生成
            - 🔍 自然语言问答
            - 💡 自动数据洞察
            - 🎯 分析建议推荐
            """)
            
            # 显示PandasAI配置信息
            with st.expander("⚙️ PandasAI配置"):
                st.markdown("""
                **当前配置：**
                - 🔑 API密钥：已配置
                - 🌐 服务端点：阿里云百炼
                - 🎨 图表保存：已启用
                - 📊 可视化引擎：Matplotlib + Plotly
                """)
        else:
            st.error("❌ PandasAI未初始化")
            st.markdown("""
            **故障排除：**
            1. 检查API密钥配置
            2. 确认依赖包已安装
            3. 重启应用程序
            """)
            
            # 显示安装命令
            with st.expander("🔧 安装指南"):
                st.code("""
# 安装PandasAI相关依赖
pip install pandasai pandasai-openai

# 或者重新安装所有依赖
pip install -r requirements.txt
                """, language="bash")
        
        # 使用提示
        with st.expander("💡 PandasAI使用指南"):
            st.markdown("""
            **快速上手：**
            1. 🔍 先执行一个数据查询
            2. 📊 在结果下方找到PandasAI区域
            3. 🎯 选择功能标签页开始探索
            4. 🎨 用自然语言描述分析需求
            
            **最佳实践：**
            - 💬 用简单明确的语言描述需求
            - 📈 尝试不同的图表类型和样式
            - 🔍 利用建议问题获得分析灵感
            - 💾 及时保存重要的可视化结果
            
            **示例问题：**
            - "用柱状图显示销售数据"
            - "分析数据的趋势和模式"
            - "找出异常值和特殊情况"
            - "对比不同类别的表现"
            """)
        
        # 功能演示
        with st.expander("🎬 功能演示"):
            st.markdown("""
            **数据可视化示例：**
            - 📊 "创建一个显示月度销售趋势的折线图"
            - 🥧 "用饼图展示产品类别的占比"
            - 📈 "制作散点图分析价格与销量的关系"
            
            **智能问答示例：**
            - ❓ "哪个产品的销售额最高？"
            - 📊 "数据中有什么明显的趋势？"
            - 🔍 "识别数据中的异常值"
            """)
    
    # 添加功能说明
    with st.expander("💡 平台功能详细说明"):
        st.markdown("""
        ### 🚀 DataCrew AutoPilot 核心功能
        
        #### 🤖 智能SQL生成
        - **自然语言理解**：支持中文和英文查询描述
        - **智能推理**：根据数据库架构自动生成最优SQL
        - **多表关联**：自动识别表间关系，生成复杂查询
        - **性能优化**：生成高效的查询语句
        
        #### 🔍 多重智能审查
        - **语法检查**：确保SQL语法正确性
        - **逻辑验证**：验证查询逻辑的合理性
        - **性能分析**：识别潜在的性能问题
        - **安全合规**：检查数据访问权限和隐私保护
        
        #### 🛠️ 人工干预模式
        **适用场景**：复杂业务逻辑、特殊查询需求、学习SQL技能
        
        **工作流程**：
        1. ✅ 启用干预模式开关
        2. 📝 输入自然语言查询需求
        3. 🤖 AI生成初始SQL查询
        4. 🔍 查看生成的SQL并选择：
           - **直接执行**：SQL符合预期
           - **人工修正**：进入编辑模式
        5. ✏️ 在编辑器中优化SQL代码
        6. 🔒 自动进行安全合规检查
        7. 📊 执行查询并获得结果
        
        #### 📊 PandasAI可视化分析
        - **智能图表生成**：自然语言描述转换为精美图表
        - **深度数据分析**：AI驱动的数据洞察和模式识别
        - **交互式问答**：直接向数据提问获得答案
        - **个性化建议**：基于数据特征推荐分析方向
        
        #### 🎯 企业级特性
        - **成本追踪**：实时监控API调用成本
        - **历史记录**：完整的分析历史和结果保存
        - **数据导出**：支持多种格式的数据导出
        - **权限控制**：细粒度的数据访问控制
        """)
    
    # 使用技巧
    with st.expander("🎓 使用技巧与最佳实践"):
        st.markdown("""
        ### 💡 查询优化技巧
        
        **描述查询需求时：**
        - 🎯 **明确具体**：说明需要哪些字段、时间范围、筛选条件
        - 📊 **指定格式**：说明是否需要排序、分组、聚合等
        - 🔢 **限制结果**：指定返回的记录数量（如"前10个"）
        
        **示例对比：**
        - ❌ 模糊："显示销售数据"
        - ✅ 具体："显示2024年1-3月销售额最高的前10个产品，包括产品名称、销售额和销量"
        
        ### 🛠️ 人工干预使用场景
        - **复杂业务逻辑**：需要多表关联、复杂计算
        - **特殊查询需求**：窗口函数、递归查询等高级SQL
        - **学习和验证**：检查AI生成的SQL，学习最佳实践
        - **性能优化**：针对大数据量进行查询优化
        
        ### 📊 PandasAI使用技巧
        - **图表描述**：详细描述图表类型、颜色、标题等
        - **分析问题**：提出具体的业务问题而非技术问题
        - **迭代优化**：基于结果不断优化问题描述
        - **保存结果**：及时下载重要的图表和分析结果
        """)
    
    # 新查询输入区域
    st.subheader("🔧 新建分析")
    
    # 添加人工干预模式开关
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("**分析模式配置**")
    with col2:
        enable_manual_intervention = st.toggle(
            "🛠️ 启用人工干预", 
            value=st.session_state.get("enable_manual_intervention", False),
            help="启用后，AI生成SQL后会暂停，让您选择直接执行或手动修正"
        )
        st.session_state["enable_manual_intervention"] = enable_manual_intervention
    
    # 根据模式显示不同的提示信息
    if enable_manual_intervention:
        st.info("🛠️ **人工干预模式已启用**：AI生成SQL后将暂停，您可以选择直接执行或手动修正")
    else:
        st.info("🤖 **快速模式**：AI将自动生成、审查并执行SQL查询")
    
    # 添加分析示例引导
    with st.expander("💡 分析示例引导", expanded=False):
        st.markdown("### 🎯 快速开始分析")
        
        # 创建示例分类
        example_categories = {
            "📈 基础销售分析": [
                "显示最近30天的销售总额",
                "列出销售额最高的前10个产品",
                "统计每个月的订单数量",
                "计算平均订单金额",
                "显示不同支付方式的使用情况"
            ],
            "👥 客户行为分析": [
                "统计客户数量按国家分布",
                "显示客户细分的占比情况",
                "分析VIP客户的购买特征",
                "计算客户复购率",
                "识别流失风险客户"
            ],
            "🛍️ 产品与库存": [
                "显示每个产品分类的产品数量",
                "列出价格最高的20个产品",
                "分析产品评价与销售的关系",
                "计算产品的毛利率排名",
                "统计各个品牌的产品数量"
            ],
            "📊 销售业绩分析": [
                "分析2023年每个月的销售趋势",
                "对比不同地区的销售表现",
                "分析销售团队的业绩表现",
                "计算销售的季节性波动",
                "分析不同销售渠道的业绩"
            ],
            "🎯 客户服务分析": [
                "分析客服工单的处理效率",
                "统计不同问题类型的分布",
                "分析客户满意度趋势",
                "计算平均响应时间",
                "识别高效客服人员"
            ],
            "📈 营销效果分析": [
                "分析网站流量与转化的关系",
                "计算不同流量来源的转化率",
                "分析用户跳出率按设备类型",
                "识别高价值会话特征",
                "分析用户浏览深度与购买意愿"
            ]
        }
        
        # 显示示例分类
        for category, examples in example_categories.items():
            with st.expander(category):
                for i, example in enumerate(examples):
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        st.write(f"• {example}")
                    with col2:
                        if st.button("试试看", key=f"example_{category}_{i}"):
                            # 设置查询文本并执行
                            st.session_state["current_prompt"] = example
                            execute_new_analysis(example)
                            # st.rerun() # 移除强制刷新，让Streamlit自动处理UI更新
        
        st.markdown("---")
        st.markdown("### 🔧 自定义分析")
        st.markdown("**分析模板：**")
        templates = [
            "分析[时间范围]内[业务指标]的变化趋势，包括[具体维度]的对比",
            "对比[分析对象A]和[分析对象B]在[业务指标]方面的差异",
            "按[细分维度]分析[业务指标]，识别[关键洞察]"
        ]
        for template in templates:
            st.code(template, language="text")
        
        st.info("💡 提示：复制示例文本，根据需要修改具体参数，然后在上方输入框中执行分析")

    # 检查是否有生成的SQL等待用户选择
    if st.session_state.get("generated_sql_info") and st.session_state["generated_sql_info"].get("show_choice"):
        sql_info = st.session_state["generated_sql_info"]
        st.subheader("🤖 SQL已生成")
        
        # 显示生成的SQL
        formatted_sql = sqlparse.format(sql_info["raw_sql"], reindent=True, keyword_case='upper')
        st.code(formatted_sql, language="sql")
        
        # 显示选择按钮
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            if st.button("✅ 直接执行", type="primary", key="execute_sql_direct"):
                # 创建一个完整的记录对象
                record = create_analysis_record(
                    user_prompt=sql_info["user_prompt"],
                    generated_sql=sql_info["raw_sql"],
                    cost=0.0
                )
                record["status"] = "generated"
                # 执行完整流程
                continue_with_generated_sql(sql_info["raw_sql"], sql_info["user_prompt"], record)
                # 清除生成的SQL信息
                st.session_state["generated_sql_info"] = None
                st.rerun()
        with col2:
            if st.button("✏️ 人工修正", key="manual_edit_sql"):
                # 进入人工干预模式
                enter_manual_intervention_mode(sql_info["user_prompt"], sql_info["raw_sql"])
                # 清除生成的SQL信息
                st.session_state["generated_sql_info"] = None
                st.rerun()
        with col3:
            st.info("💡 请选择执行方式")
        
        st.divider()
    
    # 检查是否处于人工干预模式
    if st.session_state.get("manual_intervention_mode", False):
        st.info("🛠️ 人工干预模式：请修正生成的SQL后提交")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            st.write(f"**原始查询：** {st.session_state.get('pending_user_prompt', '')}")
        with col2:
            if st.button("❌ 取消干预", key="cancel_intervention"):
                st.session_state["manual_intervention_mode"] = False
                st.session_state["pending_manual_sql"] = ""
                st.session_state["pending_user_prompt"] = ""
                st.rerun()
        
        # SQL编辑器
        manual_sql = st.text_area(
            "编辑SQL：",
            value=st.session_state.get("pending_manual_sql", ""),
            height=200,
            help="请修正生成的SQL，确保查询符合您的预期"
        )
        
        col1, col2 = st.columns([1, 3])
        with col1:
            if st.button("✅ 提交修正SQL", type="primary", key="submit_manual_sql"):
                process_manual_sql(manual_sql, st.session_state.get("pending_user_prompt", ""))
                st.rerun()
        with col2:
            st.caption("💡 修正后的SQL将直接进行合规检查和执行")
    
    else:
        # 正常的新查询输入
        col1, col2 = st.columns([4, 1])
        with col1:
            user_prompt = st.text_input(
                "输入您的数据查询请求：",
                placeholder="例如：显示2024年4月收入最高的前5个产品",
                key="current_prompt",
                value=st.session_state.get("current_prompt", "")
            )
        with col2:
            st.write("")  # 空行对齐
            if st.button("🚀 执行分析", type="primary"):
                execute_new_analysis(user_prompt)
                # st.rerun() # 移除强制刷新，让Streamlit自动处理UI更新
    
    # 显示历史分析
    if st.session_state["analysis_history"]:
        st.subheader("📚 分析历史")
        
        # 按时间倒序显示
        for record in reversed(st.session_state["analysis_history"]):
            is_current = record["id"] == st.session_state.get("current_cell")
            render_analysis_cell(record, is_current)
    else:
        st.info("👆 开始您的第一次数据分析吧！")

if __name__ == "__main__":
    main()

