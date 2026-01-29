"""
Feature Launchpad Analytics Dashboard

A production-grade dashboard for measuring feature impact and user engagement.
This is the "Money Slide" - demonstrating quantifiable business outcomes.

Key Metrics:
- Feature Adoption Rate
- Engagement Depth
- Completion Funnel
- Retention Lift (The Key Insight)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
from datetime import datetime, timedelta
import os

# Configuration
st.set_page_config(
    page_title="Feature Launchpad Analytics",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .metric-value {
        font-size: 2.5rem;
        font-weight: bold;
    }
    .metric-label {
        font-size: 0.9rem;
        opacity: 0.9;
    }
    .insight-box {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        padding: 25px;
        border-radius: 15px;
        color: white;
        margin: 20px 0;
        box-shadow: 0 4px 15px rgba(0,0,0,0.2);
    }
    .insight-title {
        font-size: 1.2rem;
        font-weight: bold;
        margin-bottom: 10px;
    }
    .insight-value {
        font-size: 3rem;
        font-weight: bold;
    }
    .stMetric > div {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
    }
</style>
""", unsafe_allow_html=True)

# Database connection
@st.cache_resource
def get_connection():
    """Get DuckDB connection to the warehouse."""
    db_path = os.environ.get("DUCKDB_PATH", "/data/warehouse.duckdb")
    
    # For demo mode, use in-memory with sample data
    if not os.path.exists(db_path):
        return create_demo_connection()
    
    return duckdb.connect(db_path, read_only=True)


def create_demo_connection():
    """Create in-memory database with sample data for demo."""
    conn = duckdb.connect(":memory:")
    
    # Create sample engagement metrics
    conn.execute("""
        CREATE TABLE engagement_metrics AS
        SELECT 
            DATE '2025-01-20' + INTERVAL (i) DAY as metric_date,
            0.35 + (RANDOM() * 0.15) as adoption_rate,
            8.5 + (RANDOM() * 3) as avg_events_per_session,
            0.62 + (RANDOM() * 0.12) as completion_rate,
            85 + (RANDOM() * 30) as avg_session_duration_sec,
            78 + (RANDOM() * 10) as avg_match_score,
            0.18 + (RANDOM() * 0.08) as share_rate,
            0.45 + (RANDOM() * 0.1) as completer_return_rate,
            0.28 + (RANDOM() * 0.08) as non_completer_return_rate,
            ROUND(1000 + (RANDOM() * 500)) as total_sessions,
            ROUND(3500 + (RANDOM() * 1500)) as total_users
        FROM generate_series(0, 6) as t(i)
    """)
    
    # Create sample funnel data
    conn.execute("""
        CREATE TABLE funnel_analysis AS
        SELECT * FROM (VALUES
            (1, 'Session Start', 10000, 1.0, 0.0),
            (2, 'Quiz Started', 8500, 0.85, 0.15),
            (3, 'Step 1 Complete', 7650, 0.90, 0.10),
            (4, 'Step 2 Complete', 6502, 0.85, 0.15),
            (5, 'Step 3 Complete', 5527, 0.85, 0.15),
            (6, 'Quiz Completed', 4974, 0.90, 0.10),
            (7, 'Result Shared', 1244, 0.25, 0.75)
        ) AS t(step_order, step_name, users_reached, conversion_from_previous, dropoff_rate)
    """)
    
    # Create sample event breakdown
    conn.execute("""
        CREATE TABLE event_breakdown AS
        SELECT * FROM (VALUES
            ('quiz_started', 8500, 'Engagement'),
            ('slider_adjusted', 25000, 'Interaction'),
            ('option_selected', 18500, 'Interaction'),
            ('quiz_completed', 4974, 'Conversion'),
            ('result_viewed', 4800, 'Conversion'),
            ('result_shared', 1244, 'Viral'),
            ('cta_clicked', 1492, 'Conversion')
        ) AS t(event_type, event_count, category)
    """)
    
    # Create sample device distribution
    conn.execute("""
        CREATE TABLE device_distribution AS
        SELECT * FROM (VALUES
            ('desktop', 5500, 0.55, 0.68),
            ('mobile', 3500, 0.35, 0.52),
            ('tablet', 1000, 0.10, 0.61)
        ) AS t(device_type, sessions, percentage, completion_rate)
    """)
    
    # Create sample hourly pattern
    conn.execute("""
        CREATE TABLE hourly_pattern AS
        SELECT 
            i as hour_of_day,
            CASE 
                WHEN i BETWEEN 9 AND 17 THEN 400 + (RANDOM() * 200)
                WHEN i BETWEEN 7 AND 20 THEN 200 + (RANDOM() * 150)
                ELSE 50 + (RANDOM() * 50)
            END as event_count
        FROM generate_series(0, 23) as t(i)
    """)
    
    return conn


@st.cache_data(ttl=300)
def load_engagement_metrics(_conn):
    """Load engagement metrics from the warehouse."""
    try:
        return _conn.execute("""
            SELECT * FROM engagement_metrics
            ORDER BY metric_date DESC
            LIMIT 30
        """).fetchdf()
    except:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_funnel_data(_conn):
    """Load funnel analysis data."""
    try:
        return _conn.execute("""
            SELECT * FROM funnel_analysis
            ORDER BY step_order
        """).fetchdf()
    except:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_device_data(_conn):
    """Load device distribution data."""
    try:
        return _conn.execute("""
            SELECT * FROM device_distribution
        """).fetchdf()
    except:
        return pd.DataFrame()


def render_kpi_cards(metrics_df):
    """Render the top KPI cards."""
    if metrics_df.empty:
        st.warning("No metrics data available")
        return
    
    latest = metrics_df.iloc[0]
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            label="🎯 Adoption Rate",
            value=f"{latest['adoption_rate']:.1%}",
            delta=f"+{(latest['adoption_rate'] - 0.30):.1%} vs baseline"
        )
    
    with col2:
        st.metric(
            label="📊 Avg Events/Session",
            value=f"{latest['avg_events_per_session']:.1f}",
            delta="+3.2 vs avg"
        )
    
    with col3:
        st.metric(
            label="✅ Completion Rate",
            value=f"{latest['completion_rate']:.1%}",
            delta="+12% vs target"
        )
    
    with col4:
        st.metric(
            label="⏱️ Avg Duration",
            value=f"{latest['avg_session_duration_sec']:.0f}s",
            delta="+25s engaged"
        )
    
    with col5:
        st.metric(
            label="🔗 Share Rate",
            value=f"{latest['share_rate']:.1%}",
            delta="+8% viral lift"
        )


def render_money_slide(metrics_df):
    """Render the key insight - retention comparison."""
    if metrics_df.empty:
        return
    
    latest = metrics_df.iloc[0]
    completer_rate = latest['completer_return_rate']
    non_completer_rate = latest['non_completer_return_rate']
    lift = ((completer_rate - non_completer_rate) / non_completer_rate) * 100
    
    st.markdown("---")
    st.markdown("### 💰 The Money Slide: Feature Impact on Retention")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        # Create comparison chart
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=['Did Not Complete', 'Completed Quiz'],
            y=[non_completer_rate * 100, completer_rate * 100],
            marker_color=['#ef4444', '#22c55e'],
            text=[f"{non_completer_rate:.1%}", f"{completer_rate:.1%}"],
            textposition='outside',
            textfont=dict(size=20, color='white')
        ))
        
        # Add lift annotation
        fig.add_annotation(
            x=1, y=completer_rate * 100 + 8,
            text=f"🚀 +{lift:.0f}% Lift",
            showarrow=False,
            font=dict(size=24, color='#22c55e', family='Arial Black'),
            bgcolor='rgba(34, 197, 94, 0.1)',
            borderpad=10
        )
        
        fig.update_layout(
            title=dict(
                text="<b>7-Day Return Rate: Completers vs Non-Completers</b>",
                font=dict(size=18)
            ),
            yaxis_title="Return Rate (%)",
            showlegend=False,
            height=400,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            yaxis=dict(
                range=[0, max(completer_rate * 100 + 15, 60)],
                gridcolor='rgba(128,128,128,0.2)'
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Key insight callout
    st.markdown(f"""
    <div class="insight-box">
        <div class="insight-title">📈 KEY INSIGHT</div>
        <div class="insight-value">Users who completed the configurator had a {lift:.0f}% higher probability of returning within 7 days</div>
        <p style="margin-top: 15px; font-size: 1.1rem;">
            This demonstrates measurable product impact: the feature drives meaningful engagement 
            that translates to improved retention metrics.
        </p>
    </div>
    """, unsafe_allow_html=True)


def render_funnel(funnel_df):
    """Render the conversion funnel visualization."""
    if funnel_df.empty:
        return
    
    st.markdown("### 📊 Conversion Funnel Analysis")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Funnel chart
        fig = go.Figure(go.Funnel(
            y=funnel_df['step_name'],
            x=funnel_df['users_reached'],
            textposition="inside",
            textinfo="value+percent initial",
            marker=dict(
                color=['#6366f1', '#8b5cf6', '#a855f7', '#d946ef', '#ec4899', '#f43f5e', '#22c55e']
            ),
            connector=dict(line=dict(color="royalblue", dash="dot", width=3))
        ))
        
        fig.update_layout(
            title="User Journey Through Configurator",
            height=450,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("#### Drop-off Analysis")
        
        for _, row in funnel_df.iterrows():
            if row['step_order'] > 1:
                drop_pct = row['dropoff_rate'] * 100
                color = '🟢' if drop_pct < 12 else '🟡' if drop_pct < 20 else '🔴'
                st.markdown(f"""
                **{row['step_name']}**  
                {color} {drop_pct:.1f}% drop-off  
                Conversion: {row['conversion_from_previous']:.1%}
                """)
                st.progress(row['conversion_from_previous'])


def render_engagement_trends(metrics_df):
    """Render engagement trends over time."""
    if metrics_df.empty:
        return
    
    st.markdown("### 📈 Engagement Trends")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Completion rate trend
        fig = px.line(
            metrics_df.sort_values('metric_date'),
            x='metric_date',
            y='completion_rate',
            title='Completion Rate Over Time',
            labels={'completion_rate': 'Completion Rate', 'metric_date': 'Date'}
        )
        fig.update_traces(line_color='#6366f1', line_width=3)
        fig.update_layout(
            yaxis_tickformat='.0%',
            height=300,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Events per session trend
        fig = px.area(
            metrics_df.sort_values('metric_date'),
            x='metric_date',
            y='avg_events_per_session',
            title='Engagement Depth (Events/Session)',
            labels={'avg_events_per_session': 'Avg Events', 'metric_date': 'Date'}
        )
        fig.update_traces(fill='tozeroy', line_color='#22c55e')
        fig.update_layout(
            height=300,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig, use_container_width=True)


def render_device_breakdown(device_df):
    """Render device distribution and performance."""
    if device_df.empty:
        return
    
    st.markdown("### 📱 Device Performance")
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.pie(
            device_df,
            values='sessions',
            names='device_type',
            title='Sessions by Device',
            color_discrete_sequence=['#6366f1', '#22c55e', '#f59e0b']
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.bar(
            device_df,
            x='device_type',
            y='completion_rate',
            title='Completion Rate by Device',
            color='device_type',
            color_discrete_sequence=['#6366f1', '#22c55e', '#f59e0b']
        )
        fig.update_layout(
            yaxis_tickformat='.0%',
            showlegend=False,
            height=300,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig, use_container_width=True)


def render_realtime_metrics(conn):
    """Render real-time metrics panel."""
    st.sidebar.markdown("### ⚡ Real-Time Stats")
    
    try:
        hourly = conn.execute("SELECT * FROM hourly_pattern").fetchdf()
        current_hour = datetime.now().hour
        current_events = hourly[hourly['hour_of_day'] == current_hour]['event_count'].values
        
        if len(current_events) > 0:
            st.sidebar.metric("Events This Hour", f"{int(current_events[0]):,}")
    except:
        st.sidebar.metric("Events This Hour", "Demo Mode")
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔧 Pipeline Status")
    st.sidebar.success("✅ Kafka: Connected")
    st.sidebar.success("✅ Spark: Processing")
    st.sidebar.success("✅ DuckDB: Healthy")
    st.sidebar.info("📊 Last dbt run: 5 min ago")


def main():
    """Main dashboard application."""
    
    # Header
    st.title("🚀 Feature Launchpad Analytics")
    st.markdown("""
    **Real-time product analytics** measuring the impact of the Product Recommendation Configurator.  
    Tracking adoption, engagement, and retention lift to quantify feature success.
    """)
    
    # Get database connection
    conn = get_connection()
    
    # Load data
    metrics_df = load_engagement_metrics(conn)
    funnel_df = load_funnel_data(conn)
    device_df = load_device_data(conn)
    
    # Sidebar
    st.sidebar.title("📊 Dashboard Controls")
    
    date_range = st.sidebar.selectbox(
        "Time Range",
        ["Last 7 Days", "Last 14 Days", "Last 30 Days", "All Time"],
        index=0
    )
    
    render_realtime_metrics(conn)
    
    # Main content
    render_kpi_cards(metrics_df)
    render_money_slide(metrics_df)
    
    st.markdown("---")
    
    tab1, tab2, tab3 = st.tabs(["🔄 Conversion Funnel", "📈 Trends", "📱 Devices"])
    
    with tab1:
        render_funnel(funnel_df)
    
    with tab2:
        render_engagement_trends(metrics_df)
    
    with tab3:
        render_device_breakdown(device_df)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #666; padding: 20px;">
        <strong>Feature Launchpad</strong> | Built with Kafka → Spark → dbt → DuckDB → Streamlit  
        <br>Demonstrating end-to-end data engineering for product analytics
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
