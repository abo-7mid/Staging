import streamlit as st
import pandas as pd
import html
from ..data_access import get_week_matches, get_match_weeks, get_playoff_matches, get_standings, get_match_maps

def show_matches():
    st.markdown('<h1 class="main-header">MATCH SCHEDULE</h1>', unsafe_allow_html=True)
    week_options = [1, 2, 3, 4, 5, 6, "Playoffs"]
    week = st.selectbox("Select Week", week_options, index=0)
    
    if week == "Playoffs":
        df = get_playoff_matches()
    else:
        df = get_week_matches(week)
        
    if df.empty:
        st.info("No matches for this week.")
    else:
        if week == "Playoffs":
            st.markdown("### Playoff Brackets")
            # Group by playoff_round (1=Quarters, 2=Semis, 3=Finals etc.)
            rounds = sorted(df['playoff_round'].unique())
            cols = st.columns(len(rounds))
            for i, r_num in enumerate(rounds):
                with cols[i]:
                    r_name = {
                        1: "Round of 24",
                        2: "Round of 16",
                        3: "Quarter-Finals",
                        4: "Semi-Finals",
                        5: "Grand Finals"
                    }.get(r_num, f"Round {r_num}")
                    st.markdown(f"<h4 style='text-align: center; color: var(--primary-red);'>{r_name}</h4>", unsafe_allow_html=True)
                    # If Round 1, show BYEs
                    if r_num == 1:
                        standings = get_standings()
                        if not standings.empty:
                            # Group by group_name and get top 2
                            for g_name, g_df in standings.groupby('group_name'):
                                g_df = g_df.sort_values(['Points', 'PD'], ascending=False).head(2)
                                for team in g_df.itertuples():
                                    st.markdown(f"""<div class="custom-card" style="margin-bottom: 10px; padding: 10px; border-left: 3px solid var(--primary-blue); opacity: 0.8;">
<div style="display: flex; justify-content: space-between; font-size: 0.9rem;">
<span style="color: var(--primary-blue); font-weight: bold;">{html.escape(str(team.name))}</span>
<span style="font-family: 'Orbitron'; color: var(--text-dim); font-size: 0.7rem;">BYE</span>
</div>
<div style="text-align: center; font-size: 0.6rem; color: var(--text-dim); margin-top: 5px;">ADVANCES TO R16</div>
</div>""", unsafe_allow_html=True)

                    r_matches = df[df['playoff_round'] == r_num].sort_values('bracket_pos')
                    for m in r_matches.itertuples():
                        winner_color_1 = "var(--primary-blue)" if m.status == 'completed' and m.winner_id == m.t1_id else "var(--text-main)"
                        winner_color_2 = "var(--primary-red)" if m.status == 'completed' and m.winner_id == m.t2_id else "var(--text-main)"
                        
                        # Use bracket label if names are TBD
                        t1_display = m.t1_name if m.t1_name else (m.bracket_label.split(' vs ')[0] if m.bracket_label and ' vs ' in m.bracket_label else "TBD")
                        t2_display = m.t2_name if m.t2_name else (m.bracket_label.split(' vs ')[1] if m.bracket_label and ' vs ' in m.bracket_label else "TBD")

                        st.markdown(f"""<div class="custom-card" style="margin-bottom: 10px; padding: 10px; border-left: 3px solid {winner_color_1 if m.winner_id == m.t1_id else winner_color_2};">
<div style="display: flex; justify-content: space-between; font-size: 0.9rem;">
<span style="color: {winner_color_1}; font-weight: {'bold' if m.winner_id == m.t1_id else 'normal'};">{html.escape(str(t1_display))}</span>
<span style="font-family: 'Orbitron';">{int(m.score_t1) if m.status == 'completed' else '-'}</span>
</div>
<div style="display: flex; justify-content: space-between; font-size: 0.9rem; margin-top: 5px;">
<span style="color: {winner_color_2}; font-weight: {'bold' if m.winner_id == m.t2_id else 'normal'};">{html.escape(str(t2_display))}</span>
<span style="font-family: 'Orbitron';">{int(m.score_t2) if m.status == 'completed' else '-'}</span>
</div>
<div style="text-align: center; font-size: 0.6rem; color: var(--text-dim); margin-top: 5px;">{html.escape(str(m.format))}</div>
</div>""", unsafe_allow_html=True)
        else:
            st.markdown("### Scheduled")
            sched = df[df['status'] != 'completed']
            if sched.empty:
                st.caption("None")
            else:
                for m in sched.itertuples():
                    st.markdown(f"""<div class="custom-card">
<div style="display: flex; justify-content: space-between; align-items: center;">
<div style="flex: 1; text-align: right; font-weight: bold; color: var(--primary-blue);">{html.escape(str(m.t1_name))}</div>
<div style="margin: 0 20px; color: var(--text-dim); font-family: 'Orbitron';">VS</div>
<div style="flex: 1; text-align: left; font-weight: bold; color: var(--primary-red);">{html.escape(str(m.t2_name))}</div>
</div>
<div style="text-align: center; color: var(--text-dim); font-size: 0.8rem; margin-top: 10px;">{html.escape(str(m.format))} • {html.escape(str(m.group_name))}</div>
</div>""", unsafe_allow_html=True)
        
            st.markdown("### Completed")
            comp = df[df['status'] == 'completed']
            for m in comp.itertuples():
                with st.container():
                    winner_color_1 = "var(--primary-blue)" if m.score_t1 > m.score_t2 else "var(--text-main)"
                    winner_color_2 = "var(--primary-red)" if m.score_t2 > m.score_t1 else "var(--text-main)"
                    
                    forfeit_badge = '<div style="text-align: center; margin-bottom: 5px;"><span style="background: rgba(255, 70, 85, 0.2); color: var(--primary-red); padding: 2px 8px; border-radius: 4px; font-size: 0.7rem; font-weight: bold; border: 1px solid var(--primary-red);">FORFEIT</span></div>' if getattr(m, 'is_forfeit', 0) else ''
                    
                    st.markdown(f"""<div class="custom-card" style="border-left: 4px solid {'var(--primary-blue)' if m.score_t1 > m.score_t2 else 'var(--primary-red)'};">
{forfeit_badge}
<div style="display: flex; justify-content: space-between; align-items: center;">
<div style="flex: 1; text-align: right;">
<span style="font-weight: bold; color: {winner_color_1};">{html.escape(str(m.t1_name))}</span>
<span style="font-size: 1.5rem; margin-left: 10px; font-family: 'Orbitron';">{m.score_t1}</span>
</div>
<div style="margin: 0 20px; color: var(--text-dim); font-family: 'Orbitron';">-</div>
<div style="flex: 1; text-align: left;">
<span style="font-size: 1.5rem; margin-right: 10px; font-family: 'Orbitron';">{m.score_t2}</span>
<span style="font-weight: bold; color: {winner_color_2};">{html.escape(str(m.t2_name))}</span>
</div>
</div>
<div style="text-align: center; color: var(--text-dim); font-size: 0.8rem; margin-top: 10px;">{html.escape(str(m.format))} • {html.escape(str(m.group_name))}</div>
</div>""", unsafe_allow_html=True)
                    
                    with st.expander("Match Details"):
                        maps_df = get_match_maps(int(m.id))
                        if maps_df.empty:
                            st.caption("No map details")
                        else:
                            md = maps_df.copy()
                            t1_id_val = int(getattr(m, 't1_id', getattr(m, 'team1_id', 0)))
                            t2_id_val = int(getattr(m, 't2_id', getattr(m, 'team2_id', 0)))
                            # Vectorized Winner calculation
                            md['Winner'] = ''
                            md.loc[md['winner_id'] == t1_id_val, 'Winner'] = m.t1_name
                            md.loc[md['winner_id'] == t2_id_val, 'Winner'] = m.t2_name
                            
                            md = md.rename(columns={
                                'map_index': 'Map',
                                'map_name': 'Name',
                                'team1_rounds': m.t1_name,
                                'team2_rounds': m.t2_name,
                            })
                            md['Map'] = md['Map'] + 1
                            st.dataframe(md[['Map', 'Name', m.t1_name, m.t2_name, 'Winner']], hide_index=True, use_container_width=True)
