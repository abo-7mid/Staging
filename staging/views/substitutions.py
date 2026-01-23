import streamlit as st
import pandas as pd
import plotly.express as px
import html
from staging.config import apply_plotly_theme
from staging.data_access import get_substitutions_log

def show_substitutions():
    st.markdown('<h1 class="main-header">SUBSTITUTIONS LOG</h1>', unsafe_allow_html=True)
    
    df = get_substitutions_log()
    if df.empty:
        st.info("No substitutions recorded.")
    else:
        # Summary Metrics
        m1, m2 = st.columns(2)
        with m1:
            st.markdown(f"""<div class="custom-card" style="text-align: center;">
<div style="color: var(--text-dim); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 1px;">Total Subs</div>
<div style="font-size: 2.5rem; font-family: 'Orbitron'; color: var(--primary-blue); margin: 10px 0;">{len(df)}</div>
</div>""", unsafe_allow_html=True)
        with m2:
            top_team = df.groupby('team').size().idxmax() if not df.empty else "N/A"
            st.markdown(f"""<div class="custom-card" style="text-align: center;">
<div style="color: var(--text-dim); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 1px;">Most Active Team</div>
<div style="font-size: 1.5rem; font-family: 'Orbitron'; color: var(--primary-red); margin: 10px 0;">{html.escape(str(top_team))}</div>
</div>""", unsafe_allow_html=True)
            
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Charts Section
        c1, c2 = st.columns(2)
        with c1:
            tcount = df.groupby('team').size().reset_index(name='subs').sort_values('subs', ascending=False)
            fig_sub_team = px.bar(tcount, x='team', y='subs', title="Subs by Team",
                                  color_discrete_sequence=['#3FD1FF'], labels={'team': 'Team', 'subs': 'Substitutions'})
            st.plotly_chart(apply_plotly_theme(fig_sub_team), use_container_width=True)
        
        with c2:
            if 'week' in df.columns:
                wcount = df.groupby('week').size().reset_index(name='subs')
                fig_sub_week = px.line(wcount, x='week', y='subs', title="Subs per Week", markers=True,
                                       color_discrete_sequence=['#FF4655'], labels={'week': 'Week', 'subs': 'Substitutions'})
                st.plotly_chart(apply_plotly_theme(fig_sub_week), use_container_width=True)
        
        # Detailed Log
        st.markdown('<h3 style="color: var(--primary-blue); font-family: \'Orbitron\';">DETAILED LOG</h3>', unsafe_allow_html=True)
        st.dataframe(df, use_container_width=True, hide_index=True)
