"""
Streamlit Dashboard - Data Quality Monitoring

This dashboard provides real-time visibility into:
1. Data quality pass/fail rates
2. Cross-source consistency
3. Ingestion status
4. Data freshness

TODO: Implement in Session 2

To run: streamlit run src/dashboard/app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="Financial Data Pipeline - Quality Dashboard",
    page_icon="📊",
    layout="wide",
)

# =============================================================================
# SIDEBAR
# =============================================================================

st.sidebar.title("📊 Data Quality Dashboard")
st.sidebar.markdown("---")

# Date range filter
days_back = st.sidebar.slider("Days to display", min_value=1, max_value=30, value=7)

# Source filter
source_filter = st.sidebar.selectbox(
    "Data Source",
    options=["All", "yahoo_finance", "alpha_vantage", "polygon"],
)

# Refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("Last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# =============================================================================
# MAIN CONTENT
# =============================================================================

st.title("Financial Data Pipeline")
st.markdown("### Data Quality Monitoring Dashboard")

# Placeholder for when not implemented
st.info("""
    🚧 **Dashboard Implementation Placeholder**
    
    This dashboard will show:
    - **Quality Overview**: Pass/fail rates over time
    - **Cross-Source Comparison**: Price discrepancies between sources
    - **Data Freshness**: Latest data timestamps per source
    - **Failure Details**: Recent validation failures to investigate
    
    **TODO**: Implement in Session 2 using:
    - `src.database.QualityMetricsRepository` for quality data
    - `src.database.MarketDataRepository` for market data
    - Plotly for visualizations
""")

# =============================================================================
# SECTION 1: QUALITY OVERVIEW (Implement in Session 2)
# =============================================================================

st.markdown("---")
st.subheader("📈 Quality Overview")

# Placeholder metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Total Checks (7d)",
        value="--",
        delta="Pending implementation",
    )

with col2:
    st.metric(
        label="Pass Rate",
        value="--%",
        delta="Pending implementation",
    )

with col3:
    st.metric(
        label="Failed Checks",
        value="--",
        delta="Pending implementation",
    )

with col4:
    st.metric(
        label="Data Freshness",
        value="--",
        delta="Pending implementation",
    )

# Placeholder chart
st.markdown("#### Daily Pass Rate Trend")
st.text("Chart will show pass rate over time by source")

# TODO: Implement this section
# 1. Query quality metrics from database
# 2. Calculate daily pass rates
# 3. Create time series chart with Plotly

# =============================================================================
# SECTION 2: CROSS-SOURCE CONSISTENCY (Implement in Session 2)
# =============================================================================

st.markdown("---")
st.subheader("🔀 Cross-Source Consistency")

st.text("Table will show price discrepancies between sources")

# TODO: Implement this section
# 1. Query cross-source comparison view
# 2. Highlight discrepancies above threshold
# 3. Allow drilling down into specific symbols

# =============================================================================
# SECTION 3: DATA FRESHNESS (Implement in Session 2)
# =============================================================================

st.markdown("---")
st.subheader("⏰ Data Freshness")

st.text("Table will show latest timestamp per source and symbol")

# TODO: Implement this section
# 1. Query latest prices view
# 2. Calculate staleness (time since last update)
# 3. Alert on stale data

# =============================================================================
# SECTION 4: RECENT FAILURES (Implement in Session 2)
# =============================================================================

st.markdown("---")
st.subheader("❌ Recent Validation Failures")

st.text("Table will show recent failures with details")

# TODO: Implement this section
# 1. Query recent failures from quality metrics
# 2. Show expectation type, column, observed vs expected
# 3. Allow filtering and sorting

# =============================================================================
# FOOTER
# =============================================================================

st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: gray;'>
        Financial Data Pipeline v1.0.0 | 
        <a href='/docs'>API Docs</a> | 
        Built with Streamlit
    </div>
    """,
    unsafe_allow_html=True,
)
