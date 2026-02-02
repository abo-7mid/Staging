import streamlit as st
import pandas as pd
import html
from ..data_access import get_standings

def show_standings():
    st.markdown('<h2 class="main-header">LEAGUE STANDINGS</h2>', unsafe_allow_html=True)
    
    df = get_standings()
    if df.empty:
        st.info("No standings available yet.")
        return

    # Split by Group
    groups = df['group_name'].unique()
    groups = sorted([g for g in groups if g]) # Filter None/Empty and sort
    
    if not groups:
        # If no groups defined, show all
        display_standings_table(df)
    else:
        tabs = st.tabs([f"Group {g}" for g in groups])
        for i, group in enumerate(groups):
            with tabs[i]:
                group_df = df[df['group_name'] == group].reset_index(drop=True)
                display_standings_table(group_df)

def display_standings_table(df):
    # Legend
    st.markdown("""
    <div style="display: flex; gap: 15px; margin-bottom: 10px; font-size: 0.8em; color: #ccc;">
        <div style="display: flex; align-items: center;"><div style="width: 10px; height: 10px; background: #2ECC71; margin-right: 5px; border-radius: 2px;"></div> Round 1 BYE</div>
        <div style="display: flex; align-items: center;"><div style="width: 10px; height: 10px; background: #3498DB; margin-right: 5px; border-radius: 2px;"></div> Round of 24</div>
        <div style="display: flex; align-items: center;"><div style="width: 10px; height: 10px; background: #555; margin-right: 5px; border-radius: 2px;"></div> Eliminated</div>
    </div>
    """, unsafe_allow_html=True)

    # Select columns for display
    cols = ['name', 'Wins', 'Losses', 'PD', 'Points']
    display_df = df[cols].copy()
    display_df.columns = ['Team', 'W', 'L', 'RD', 'Pts']
    
    # Generate HTML Table
    html_str = '<table class="valorant-table">'
    html_str += '<thead><tr>'
    for col in display_df.columns:
        html_str += f'<th>{col}</th>'
    html_str += '</tr></thead><tbody>'
    
    for i, (_, row) in enumerate(display_df.iterrows()):
        # Determine Row Style
        if i < 2: # Top 2
            row_style = 'style="border-left: 4px solid #2ECC71; background: linear-gradient(90deg, rgba(46, 204, 113, 0.1) 0%, rgba(0,0,0,0) 30%);"'
        elif i < 6: # 3-6
            row_style = 'style="border-left: 4px solid #3498DB; background: linear-gradient(90deg, rgba(52, 152, 219, 0.1) 0%, rgba(0,0,0,0) 30%);"'
        else:
            row_style = 'style="border-left: 4px solid #555;"'
            
        html_str += f'<tr {row_style}>'
        for col in display_df.columns:
            val = row[col]
            if col == 'RD' and val > 0:
                val = f"+{val}"
            # Escape strings to prevent XSS/layout breakage
            val_str = html.escape(str(val))
            html_str += f'<td>{val_str}</td>'
        html_str += '</tr>'
    
    html_str += '</tbody></table>'
    
    st.markdown(html_str, unsafe_allow_html=True)
