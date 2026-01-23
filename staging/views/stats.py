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
            filtered_df = df[df['games'] >= min_games].sort_values('avg_acs', ascending=False)
            
            # Podium Logic
            if len(filtered_df) >= 3:
                top3 = filtered_df.head(3).to_dict('records')
                
                # Custom CSS for Podium
                st.markdown("""
                <style>
                .podium-container {
                    display: flex;
                    justify-content: center;
                    align-items: flex-end;
                    gap: 1rem;
                    margin-bottom: 2rem;
                    height: 250px;
                }
                .podium-item {
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    width: 120px;
                }
                .podium-rank {
                    width: 100%;
                    border-radius: 8px 8px 0 0;
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    justify-content: center;
                    color: #0f1923;
                    font-weight: bold;
                    box-shadow: 0 4px 15px rgba(0,0,0,0.5);
                }
                .rank-1 { height: 160px; background: linear-gradient(135deg, #FFD700, #FDB931); border: 2px solid #FFF8D6; }
                .rank-2 { height: 120px; background: linear-gradient(135deg, #E0E0E0, #BDBDBD); border: 2px solid #F5F5F5; }
                .rank-3 { height: 90px; background: linear-gradient(135deg, #CD7F32, #A0522D); border: 2px solid #E6C2AA; }
                
                .player-name { font-weight: bold; margin-bottom: 5px; text-align: center; color: #ece8e1; }
                .player-stats { font-size: 0.8rem; color: #8b97a5; }
                .medal { font-size: 2rem; margin-bottom: 10px; }
                </style>
                """, unsafe_allow_html=True)
                
                c1, c2, c3 = st.columns([1,1,1]) # Use columns for layout control, but actually better to use HTML directly if possible or columns
                
                # Construct HTML for podium
                # Order: 2nd, 1st, 3rd for visual podium effect
                p1 = top3[0]
                p2 = top3[1]
                p3 = top3[2]
                
                html = f"""
                <div class="podium-container">
                    <div class="podium-item">
                        <div class="player-name">{p2['name']}</div>
                        <div class="player-stats">{p2['avg_acs']:.1f} ACS</div>
                        <div class="podium-rank rank-2">
                            <div class="medal">🥈</div>
                            <div>#2</div>
                        </div>
                    </div>
                    <div class="podium-item">
                        <div class="player-name">👑 {p1['name']}</div>
                        <div class="player-stats">{p1['avg_acs']:.1f} ACS</div>
                        <div class="podium-rank rank-1">
                            <div class="medal">🥇</div>
                            <div>#1</div>
                        </div>
                    </div>
                    <div class="podium-item">
                        <div class="player-name">{p3['name']}</div>
                        <div class="player-stats">{p3['avg_acs']:.1f} ACS</div>
                        <div class="podium-rank rank-3">
                            <div class="medal">🥉</div>
                            <div>#3</div>
                        </div>
                    </div>
                </div>
                """
                st.markdown(html, unsafe_allow_html=True)
            
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
