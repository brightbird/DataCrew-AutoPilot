import streamlit as st
import os
import pandas as pd
from dotenv import load_dotenv
from crew_setup import sql_generator_crew, sql_reviewer_crew, sql_compliance_crew
from utils.db_simulator import get_structured_schema, run_query
from utils.pandasai_helper import PandasAIAnalyzer
import sqlparse
from utils.helper import extract_token_counts, calculate_gpt4o_mini_cost
import base64
from datetime import datetime
import uuid

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
        return PandasAIAnalyzer(DB_PATH)
    except Exception as e:
        st.error(f"PandasAI初始化失败: {e}")
        return None

# 执行SQL查询的函数
def run_query_to_dataframe(query):
    """执行SQL查询并返回DataFrame和文本结果"""
    try:
        import sqlite3
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(query, conn)
        conn.close()
        text_result = df.head().to_string(index=False)
        return df, text_result
    except Exception as e:
        return None, f"查询失败: {e}"

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
        "timestamp": datetime.now(),
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
    st.session_state["llm_cost"] += record["cost"]

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
            st.markdown(f"**{intervention_mark} [{record['timestamp'].strftime('%H:%M:%S')}]** {record['user_prompt'][:50]}...")
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
        
        # 如果是当前单元格或被展开，显示详细内容
        expanded = is_current or st.checkbox("展开详情", key=f"expand_{cell_id}")
        
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
            
            # 显示查询结果
            if record.get("query_result"):
                with st.expander("📊 查询结果", expanded=True):
                    st.code(record["query_result"])
                    
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
                        
                        # PandasAI交互区域
                        render_pandasai_interface(record)
            
            # 显示成本信息（包含人工干预标记）
            cost_info = f"💰 本次查询成本: ${record['cost']:.6f}"
            if record.get("manual_intervention"):
                cost_info += " (人工干预)"
            st.caption(cost_info)
        
        st.divider()

def render_pandasai_interface(record):
    """渲染PandasAI交互界面"""
    analyzer = get_pandasai_analyzer()
    if not analyzer or record.get("query_dataframe") is None:
        return
    
    df = record["query_dataframe"]
    cell_id = record["id"]
    
    st.markdown("### 🤖 PandasAI 数据分析与可视化")
    
    # 创建标签页
    tab1, tab2, tab3, tab4 = st.tabs(["📊 数据可视化", "🔍 深度分析", "💡 数据洞察", "❓ 建议问题"])
    
    with tab1:
        st.write("**使用自然语言创建图表**")
        chart_request = st.text_input(
            "描述您想要的图表：", 
            placeholder="例如：将以上查询结果用条形图可视化",
            key=f"chart_request_{cell_id}"
        )
        
        if st.button("生成图表", key=f"gen_chart_{cell_id}") and chart_request:
            with st.spinner("正在生成图表..."):
                chart_result = analyzer.create_visualization(df, chart_request)
                
                if chart_result:
                    # 保存到记录中
                    record["visualizations"].append({
                        "timestamp": datetime.now(),
                        "request": chart_request,
                        "result": chart_result
                    })
                    
                    if chart_result["type"] == "image":
                        st.success(chart_result["message"])
                        st.image(
                            base64.b64decode(chart_result["base64"]), 
                            caption="PandasAI生成的图表",
                            use_column_width=True
                        )
                        
                        st.download_button(
                            label="📥 下载图表",
                            data=base64.b64decode(chart_result["base64"]),
                            file_name=f"chart_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.png",
                            mime="image/png",
                            key=f"download_chart_{cell_id}_{len(record['visualizations'])}"
                        )
                        
                    elif chart_result["type"] == "text":
                        st.info(chart_result["message"])
                        st.write(chart_result["content"])
                        
                    elif chart_result["type"] == "error":
                        st.error(chart_result["message"])
                        st.write(chart_result["content"])
                else:
                    st.warning("未能生成图表，尝试进行数据分析...")
                    with st.spinner("尝试进行数据分析..."):
                        analysis_result = analyzer.analyze_with_natural_language(df, chart_request)
                        st.write("**分析结果：**")
                        st.write(analysis_result)
        
        # 显示历史可视化
        if record.get("visualizations"):
            st.write("**历史可视化结果**")
            for i, viz in enumerate(record["visualizations"]):
                with st.expander(f"📈 {viz['request'][:30]}... ({viz['timestamp'].strftime('%H:%M:%S')})"):
                    if viz["result"]["type"] == "image":
                        st.image(
                            base64.b64decode(viz["result"]["base64"]), 
                            caption=viz["request"],
                            use_column_width=True
                        )
                    else:
                        st.write(viz["result"]["content"])
    
    with tab2:
        st.write("**对数据进行自然语言分析**")
        analysis_question = st.text_input(
            "询问关于数据的问题：",
            placeholder="例如：哪个产品的销售额最高？销售趋势如何？",
            key=f"analysis_question_{cell_id}"
        )
        
        if st.button("分析数据", key=f"analyze_{cell_id}") and analysis_question:
            with st.spinner("正在分析数据..."):
                analysis_result = analyzer.analyze_with_natural_language(df, analysis_question)
                
                # 保存到记录中
                record["analyses"].append({
                    "timestamp": datetime.now(),
                    "question": analysis_question,
                    "result": analysis_result
                })
                
                st.write(analysis_result)
        
        # 显示历史分析
        if record.get("analyses"):
            st.write("**历史分析结果**")
            for i, analysis in enumerate(record["analyses"]):
                with st.expander(f"🔍 {analysis['question'][:30]}... ({analysis['timestamp'].strftime('%H:%M:%S')})"):
                    st.write(analysis["result"])
    
    with tab3:
        st.write("**自动数据洞察**")
        if st.button("获取数据洞察", key=f"insights_{cell_id}"):
            with st.spinner("正在生成数据洞察..."):
                insights = analyzer.get_data_insights(df)
                st.markdown(insights)
    
    with tab4:
        st.write("**基于当前数据的建议问题**")
        if st.button("获取建议问题", key=f"suggestions_{cell_id}"):
            with st.spinner("正在生成建议..."):
                suggestions = analyzer.suggest_next_questions(df, record["user_prompt"])
                if suggestions:
                    for i, suggestion in enumerate(suggestions, 1):
                        st.write(f"{i}. {suggestion}")
                else:
                    st.write("暂无建议问题")

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

def process_manual_sql(user_prompt, manual_sql):
    """处理人工修正的SQL"""
    if not manual_sql.strip():
        st.warning("请输入修正后的SQL。")
        return
    
    # 创建新的分析记录
    record = create_analysis_record(
        user_prompt, 
        manual_intervention=True, 
        manual_sql=manual_sql
    )
    total_cost = 0.0
    
    try:
        # 跳过SQL生成步骤，直接从合规检查开始
        # Step 1: 合规检查
        with st.spinner("🔒 检查修正后SQL的合规性..."):
            compliance_output = sql_compliance_crew.kickoff(inputs={"reviewed_sqlquery": manual_sql})
            compliance_report = compliance_output.pydantic.report
            
            # 计算成本
            token_usage_str = str(compliance_output.token_usage)
            prompt_tokens, completion_tokens = extract_token_counts(token_usage_str)
            cost = calculate_gpt4o_mini_cost(prompt_tokens, completion_tokens)
            total_cost += cost
            
            # 清理报告格式
            lines = compliance_report.splitlines()
            if lines and lines[0].strip().lower().startswith("# compliance report"):
                compliance_report = "\n".join(lines[1:]).lstrip()
            record["compliance_report"] = compliance_report
            record["reviewed_sql"] = manual_sql  # 将人工修正的SQL作为最终SQL
        
        # Step 2: 执行查询（如果合规）
        if "compliant" in compliance_report.lower():
            with st.spinner("📊 执行修正后的查询..."):
                df, text_result = run_query_to_dataframe(manual_sql)
                record["query_result"] = text_result
                record["query_dataframe"] = df
        else:
            st.error("⚠️ 修正后的查询未通过合规检查，无法执行。")
        
        # 设置总成本
        record["cost"] = total_cost
        
        # 添加到历史记录
        add_to_history(record)
        st.session_state["current_cell"] = record["id"]
        
        # 退出人工干预模式
        st.session_state["manual_intervention_mode"] = False
        st.session_state["pending_manual_sql"] = ""
        st.session_state["pending_user_prompt"] = ""
        
        # 显示成功消息
        st.success(f"✅ 人工修正后的分析完成！成本: ${total_cost:.6f}")
        
    except Exception as e:
        st.error(f"❌ 处理修正SQL过程中发生错误: {e}")
        # 即使出错也要保存记录
        record["cost"] = total_cost
        add_to_history(record)

def execute_new_analysis(user_prompt):
    """执行新的分析"""
    if not user_prompt.strip():
        st.warning("请输入查询请求。")
        return
    
    # 创建新的分析记录
    record = create_analysis_record(user_prompt)
    total_cost = 0.0
    
    try:
        # Step 1: 生成SQL
        with st.spinner("🔧 生成SQL查询..."):
            gen_output = sql_generator_crew.kickoff(inputs={"user_input": user_prompt, "db_schema": load_schema()})
            raw_sql = gen_output.pydantic.sqlquery
            record["generated_sql"] = raw_sql
            
            # 计算成本
            token_usage_str = str(gen_output.token_usage)
            prompt_tokens, completion_tokens = extract_token_counts(token_usage_str)
            cost = calculate_gpt4o_mini_cost(prompt_tokens, completion_tokens)
            total_cost += cost
        
        # 根据全局设置决定是否需要人工干预选择
        if st.session_state.get("enable_manual_intervention", False):
            # 保存生成的SQL和相关信息到session state
            st.session_state["generated_sql_info"] = {
                "user_prompt": user_prompt,
                "record": record,
                "raw_sql": raw_sql,
                "total_cost": total_cost,
                "show_choice": True
            }
            st.success("✅ SQL已生成！请在下方选择执行方式。")
            
        else:
            # 直接执行完整流程
            st.success("✅ SQL已生成！正在执行完整流程...")
            formatted_sql = sqlparse.format(raw_sql, reindent=True, keyword_case='upper')
            st.code(formatted_sql, language="sql")
            continue_with_generated_sql(record, raw_sql, total_cost)
        
    except Exception as e:
        st.error(f"❌ SQL生成过程中发生错误: {e}")
        # 即使出错也要保存记录
        record["cost"] = total_cost
        add_to_history(record)

def continue_with_generated_sql(record, raw_sql, initial_cost):
    """继续执行生成的SQL（原有流程）"""
    total_cost = initial_cost
    
    try:
        # Step 2: 审查SQL
        with st.spinner("🔍 审查SQL查询..."):
            review_output = sql_reviewer_crew.kickoff(inputs={"sql_query": raw_sql, "db_schema": load_schema()})
            reviewed_sql = review_output.pydantic.reviewed_sqlquery
            record["reviewed_sql"] = reviewed_sql
            
            # 计算成本
            token_usage_str = str(review_output.token_usage)
            prompt_tokens, completion_tokens = extract_token_counts(token_usage_str)
            cost = calculate_gpt4o_mini_cost(prompt_tokens, completion_tokens)
            total_cost += cost
        
        # Step 3: 合规检查
        with st.spinner("🔒 检查合规性..."):
            compliance_output = sql_compliance_crew.kickoff(inputs={"reviewed_sqlquery": reviewed_sql})
            compliance_report = compliance_output.pydantic.report
            
            # 计算成本
            token_usage_str = str(compliance_output.token_usage)
            prompt_tokens, completion_tokens = extract_token_counts(token_usage_str)
            cost = calculate_gpt4o_mini_cost(prompt_tokens, completion_tokens)
            total_cost += cost
            
            # 清理报告格式
            lines = compliance_report.splitlines()
            if lines and lines[0].strip().lower().startswith("# compliance report"):
                compliance_report = "\n".join(lines[1:]).lstrip()
            record["compliance_report"] = compliance_report
        
        # Step 4: 执行查询（如果合规）
        if "compliant" in compliance_report.lower():
            with st.spinner("📊 执行查询..."):
                df, text_result = run_query_to_dataframe(reviewed_sql)
                record["query_result"] = text_result
                record["query_dataframe"] = df
        else:
            st.error("⚠️ 查询未通过合规检查，无法执行。")
        
        # 设置总成本
        record["cost"] = total_cost
        
        # 添加到历史记录
        add_to_history(record)
        st.session_state["current_cell"] = record["id"]
        
        # 显示成功消息
        st.success(f"✅ 分析完成！成本: ${total_cost:.6f}")
        
    except Exception as e:
        st.error(f"❌ 分析过程中发生错误: {e}")
        # 即使出错也要保存记录
        record["cost"] = total_cost
        add_to_history(record)

# === 主应用界面 ===
def main():
    # 初始化
    init_session_state()
    
    # 页面标题
    st.title("🤖 SQL Assistant Crew - 交互式分析")
    
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
    
    # 添加功能说明
    with st.expander("💡 人工干预功能详细说明"):
        st.markdown("""
        ### 🛠️ 人工干预模式工作流程
        
        **适用场景**：当您需要对AI生成的SQL进行精确调整时
        
        #### 📋 操作步骤
        1. **✅ 启用干预模式** → 勾选右侧"启用人工干预"开关
        2. **📝 输入查询请求** → 用自然语言描述分析需求
        3. **🤖 AI生成SQL** → 系统自动生成初始SQL查询
        4. **🔍 查看和选择** → 检查生成的SQL并选择执行方式：
           - **直接执行**：SQL符合预期，继续完整流程
           - **人工修正**：进入编辑模式进行调整
        5. **✏️ 编辑优化** → 在专业编辑器中修正SQL代码
        6. **🔒 安全检查** → 修正后自动进行合规验证
        7. **📊 查看结果** → 执行查询并进行可视化分析
        
        #### 🎯 核心优势
        - **🎯 精确控制**：确保查询完全符合业务需求
        - **📚 技能提升**：在实践中学习SQL优化技巧
        - **🔒 安全保障**：修正后仍有完整的安全检查
        - **📊 完整记录**：保留干预历史，便于学习总结
        
        #### 💡 使用技巧
        - 对于复杂业务逻辑，建议启用人工干预
        - 简单查询可使用快速模式提高效率
        - 通过干预记录学习SQL最佳实践
        """)
    
    # 新查询输入区域
    st.subheader("🔧 新建分析")
    
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
                            st.rerun()
        
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

    # 调试信息（临时）
    if st.session_state.get("enable_manual_intervention", False):
        st.info("🛠️ 人工干预模式已启用")
        if st.session_state.get("pending_analysis"):
            st.info("📋 检测到待处理的分析请求")
    
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
                # 执行完整流程
                continue_with_generated_sql(sql_info["record"], sql_info["raw_sql"], sql_info["total_cost"])
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
                process_manual_sql(st.session_state.get("pending_user_prompt", ""), manual_sql)
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
                st.rerun()
    
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

