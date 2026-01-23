import streamlit as st
import pandas as pd
import html
from staging.data_access import get_all_players_directory

def show_directory():
    st.markdown('<h1 class="main-header">PLAYERS DIRECTORY</h1>', unsafe_allow_html=True)
    
    players_df = get_all_players_directory()
    
    ranks_base = ["Unranked", "Iron", "Bronze", "Silver", "Gold", "Platinum", "Diamond", "Ascendant", "Immortal", "Radiant"]
    dynamic_ranks = sorted(list(set(players_df['rank'].dropna().unique().tolist() + ranks_base)))
    
    with st.container():
        st.markdown('<div class="custom-card">', unsafe_allow_html=True)
        c1, c2 = st.columns([2, 1])
        with c1:
            rf = st.multiselect("Filter by Rank", dynamic_ranks, default=dynamic_ranks)
        with c2:
            q = st.text_input("Search Name or Riot ID", placeholder="Search...")
        st.markdown('</div>', unsafe_allow_html=True)
    
    out = players_df.copy()
    out['rank'] = out['rank'].fillna("Unranked")
    out = out[out['rank'].isin(rf)]
    if q:
        s = q.lower()
        out = out[
            out['name'].str.lower().fillna("").str.contains(s) | 
            out['riot_id'].str.lower().fillna("").str.contains(s)
        ]
    
    # Display as a clean table with the brand theme
    st.markdown("<br>", unsafe_allow_html=True)
    if out.empty:
        st.info("No players found matching your criteria.")
    else:
        st.dataframe(
            out[['name', 'rank', 'team']], 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "name": st.column_config.TextColumn("Name (Riot ID)", width="large"),
                "rank": st.column_config.TextColumn("Rank", width="small"),
                "team": st.column_config.TextColumn("Team", width="medium"),
            }
        )
