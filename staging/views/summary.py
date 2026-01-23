import streamlit as st
import pandas as pd
import html
from staging.data_access import get_match_weeks, get_week_matches, get_match_maps, get_map_stats, get_latest_played_week

def show_summary():
    st.markdown('<h1 class="main-header">MATCH SUMMARY</h1>', unsafe_allow_html=True)
    
    wk_list = get_match_weeks()
    latest_week = get_latest_played_week()
    
    # Determine default index for latest played week
    default_idx = 0
    if latest_week in wk_list:
        default_idx = wk_list.index(latest_week)
        
    # Week selection moved from sidebar to main page
    col_wk1, col_wk2 = st.columns([1, 3])
    with col_wk1:
        week = st.selectbox("Select Week", wk_list if wk_list else [1], index=default_idx, key="wk_sum")
    
    df = get_week_matches(week) if wk_list else pd.DataFrame()
    
    # Filter only completed matches
    if not df.empty:
        df = df[df['status'] == 'completed']
    
    if df.empty:
        st.info("No completed matches for this week.")
    else:
        # Vectorized option generation
        opts = (df['t1_name'].fillna('') + " vs " + df['t2_name'].fillna('') + " (" + df['group_name'].fillna('') + ")").tolist()
        sel = st.selectbox("Select Match", list(range(len(opts))), format_func=lambda i: opts[i])
        m = df.iloc[sel]
        
        # Match Score Card
        forfeit_badge = '<div style="text-align: center; margin-bottom: 10px;"><span style="background: rgba(255, 70, 85, 0.2); color: var(--primary-red); padding: 4px 12px; border-radius: 4px; font-size: 0.8rem; font-weight: bold; border: 1px solid var(--primary-red); letter-spacing: 2px;">FORFEIT MATCH</span></div>' if m.get('is_forfeit', 0) else ''
        
        st.markdown(f"""<div class="custom-card" style="margin-bottom: 2rem; border-bottom: 4px solid {'var(--primary-blue)' if m['score_t1'] > m['score_t2'] else 'var(--primary-red)'};">
{forfeit_badge}
<div style="display: flex; justify-content: space-between; align-items: center; padding: 10px 0;">
<div style="flex: 1; text-align: right;">
<h2 style="margin: 0; color: {'var(--primary-blue)' if m['score_t1'] > m['score_t2'] else 'var(--text-main)'}; font-family: 'Orbitron';">{html.escape(str(m['t1_name']))}</h2>
</div>
<div style="margin: 0 30px; display: flex; align-items: center; gap: 15px;">
<span style="font-size: 3rem; font-family: 'Orbitron'; color: var(--text-main);">{m['score_t1']}</span>
<span style="font-size: 1.5rem; color: var(--text-dim); font-family: 'Orbitron';">:</span>
<span style="font-size: 3rem; font-family: 'Orbitron'; color: var(--text-main);">{m['score_t2']}</span>
</div>
<div style="flex: 1; text-align: left;">
<h2 style="margin: 0; color: {'var(--primary-red)' if m['score_t2'] > m['score_t1'] else 'var(--text-main)'}; font-family: 'Orbitron';">{html.escape(str(m['t2_name']))}</h2>
</div>
</div>
<div style="text-align: center; color: var(--text-dim); font-size: 0.9rem; margin-top: 10px; letter-spacing: 2px;">{html.escape(str(m['format'].upper()))} • {html.escape(str(m['group_name'].upper()))}</div>
</div>""", unsafe_allow_html=True)
        
        maps_df = get_match_maps(int(m['id']))
        if maps_df.empty:
            st.info("No detailed map data recorded for this match.")
        else:
            # Map Selection
            map_indices = sorted(maps_df['map_index'].unique().tolist())
            map_labels = [f"Map {i+1}: {maps_df[maps_df['map_index'] == i].iloc[0]['map_name']}" for i in map_indices]
            
            selected_map_idx = st.radio("Select Map", map_indices, format_func=lambda i: map_labels[i], horizontal=True)
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # Map Score Card
            curr_map = maps_df[maps_df['map_index'] == selected_map_idx].iloc[0]
            t1_id_val = int(m.get('t1_id', m.get('team1_id')))
            t2_id_val = int(m.get('t2_id', m.get('team2_id')))
            st.markdown(f"""<div class="custom-card" style="background: rgba(255,255,255,0.02); margin-bottom: 20px;">
<div style="display: flex; justify-content: center; align-items: center; gap: 40px;">
<div style="text-align: center;">
<div style="color: var(--text-dim); font-size: 0.8rem; margin-bottom: 5px;">{html.escape(str(m['t1_name']))}</div>
<div style="font-size: 2rem; font-family: 'Orbitron'; color: {'var(--primary-blue)' if curr_map['team1_rounds'] > curr_map['team2_rounds'] else 'var(--text-main)'};">{curr_map['team1_rounds']}</div>
</div>
<div style="text-align: center;">
<div style="font-family: 'Orbitron'; color: var(--primary-blue); font-size: 1.2rem;">{html.escape(str(curr_map['map_name'].upper()))}</div>
<div style="color: var(--text-dim); font-size: 0.7rem;">WINNER: {html.escape(str(m['t1_name'] if curr_map['winner_id'] == t1_id_val else m['t2_name']))}</div>
</div>
<div style="text-align: center;">
<div style="color: var(--text-dim); font-size: 0.8rem; margin-bottom: 5px;">{html.escape(str(m['t2_name']))}</div>
<div style="font-size: 2rem; font-family: 'Orbitron'; color: {'var(--primary-red)' if curr_map['team2_rounds'] > curr_map['team1_rounds'] else 'var(--text-main)'};">{curr_map['team2_rounds']}</div>
</div>
</div>
</div>""", unsafe_allow_html=True)
            
            # Scoreboards
            t1_id_val = int(m.get('t1_id', m.get('team1_id')))
            t2_id_val = int(m.get('t2_id', m.get('team2_id')))
            s1 = get_map_stats(m['id'], selected_map_idx, t1_id_val)
            s2 = get_map_stats(m['id'], selected_map_idx, t2_id_val)
            
            c1, c2 = st.columns(2)
            with c1:
                st.markdown(f'<h4 style="color: var(--primary-blue); font-family: \'Orbitron\';">{html.escape(str(m["t1_name"]))} Scoreboard</h4>', unsafe_allow_html=True)
                if s1.empty:
                    st.info("No scoreboard data")
                else:
                    st.dataframe(s1.rename(columns={'name':'Player','agent':'Agent','acs':'ACS','kills':'K','deaths':'D','assists':'A','is_sub':'Sub'}), hide_index=True, use_container_width=True)
            
            with c2:
                st.markdown(f'<h4 style="color: var(--primary-red); font-family: \'Orbitron\';">{html.escape(str(m["t2_name"]))} Scoreboard</h4>', unsafe_allow_html=True)
                if s2.empty:
                    st.info("No scoreboard data")
                else:
                    st.dataframe(s2.rename(columns={'name':'Player','agent':'Agent','acs':'ACS','kills':'K','deaths':'D','assists':'A','is_sub':'Sub'}), hide_index=True, use_container_width=True)
