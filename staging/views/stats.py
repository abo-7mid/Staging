import streamlit as st
import pandas as pd
import plotly.express as px
from ..data_access import get_player_leaderboard, get_all_players_directory, get_player_profile
from ..utils import apply_plotly_theme

def show_stats():
    st.markdown('<h2 class="main-header">PLAYER STATISTICS</h2>', unsafe_allow_html=True)
    
    tab1, tab2 = st.tabs(["LEADERBOARD", "PLAYER SEARCH"])
    
    with tab1:
        df = get_player_leaderboard()
        if df.empty:
            st.info("No stats available.")
        else:
            # Filters
            min_games = st.slider("Minimum Games Played", 1, int(df['games'].max()) if not df.empty else 5, 1)
            filtered_df = df[df['games'] >= min_games]
            
            st.dataframe(
                filtered_df,
                column_config={
                    "name": "Player",
                    "team": "Team",
                    "games": "Games",
                    "avg_acs": st.column_config.NumberColumn("ACS", format="%.1f"),
                    "kd_ratio": st.column_config.NumberColumn("K/D", format="%.2f"),
                    "total_kills": "K",
                    "total_deaths": "D",
                    "total_assists": "A"
                },
                use_container_width=True,
                hide_index=True
            )
            
    with tab2:
        all_players = get_all_players_directory()
        if all_players.empty:
            st.info("No players found.")
        else:
            p_list = all_players['name'].tolist()
            search = st.selectbox("Search Player", p_list, index=None, placeholder="Select a player...")
            
            if search:
                pid = all_players[all_players['name'] == search].iloc[0]['id']
                profile = get_player_profile(pid)
                
                if profile:
                    st.markdown(f"### {profile['display_name']}")
                    st.caption(f"Team: {profile['info'].get('team') or 'Free Agent'} | Rank: {profile['info'].get('rank') or 'Unranked'}")
                    
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("ACS", profile['avg_acs'], delta=round(profile['avg_acs'] - profile['lg_avg_acs'], 1))
                    c2.metric("K/D", profile['kd_ratio'])
                    c3.metric("Kills", profile['total_kills'])
                    c4.metric("Games", profile['games'])
                    
                    if not profile['trend'].empty:
                        st.subheader("Performance Trend")
                        fig = px.line(profile['trend'], x='label', y='avg_acs', title="ACS over Matches")
                        fig = apply_plotly_theme(fig)
                        st.plotly_chart(fig, use_container_width=True)
