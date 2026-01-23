import streamlit as st
from ..config import GLOBAL_STYLES
from ..data_access import get_standings, get_week_matches, get_completed_matches
from ..auth import get_active_user_count, get_active_admin_session
import pandas as pd

def show_home():
    # Header
    st.markdown('<h1 class="portal-header">VALORANT S23 PORTAL</h1>', unsafe_allow_html=True)
    st.markdown('<p class="portal-subtitle">OFFICIAL TOURNAMENT DASHBOARD</p>', unsafe_allow_html=True)
    
    # Status Indicators
    active_users = get_active_user_count()
    admin_sess = get_active_admin_session()
    
    st.markdown(f"""
        <div class="status-grid">
            <div class="status-indicator status-online">● SYSTEM ONLINE</div>
            <div class="status-indicator status-online">● ACTIVE USERS: {active_users}</div>
            <div class="status-indicator {'status-online' if admin_sess else 'status-offline'}">
                ● ADMIN: {'ONLINE' if admin_sess else 'OFFLINE'}
            </div>
        </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    # Quick Stats Overview
    c1, c2, c3 = st.columns(3)
    
    with c1:
        st.markdown('<div class="custom-card">', unsafe_allow_html=True)
        st.subheader("🏆 Current Leader")
        standings = get_standings()
        if not standings.empty:
            top_team = standings.iloc[0]
            st.markdown(f"**{top_team['name']}**")
            st.metric("Points", int(top_team['Points']))
        else:
            st.info("No data available")
        st.markdown('</div>', unsafe_allow_html=True)

    with c2:
        st.markdown('<div class="custom-card">', unsafe_allow_html=True)
        st.subheader("📅 Recent Activity")
        completed = get_completed_matches()
        if not completed.empty:
            last_match = completed.iloc[-1]
            st.markdown(f"Week {last_match['week']}")
            st.caption("Last match recorded")
        else:
            st.info("No matches completed")
        st.markdown('</div>', unsafe_allow_html=True)

    with c3:
        st.markdown('<div class="custom-card">', unsafe_allow_html=True)
        st.subheader("📢 Announcements")
        st.info("Season 23 is live! Check the Schedule tab for upcoming matches.")
        st.markdown('</div>', unsafe_allow_html=True)
