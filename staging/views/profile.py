import streamlit as st
import pandas as pd
import html
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from ..config import apply_plotly_theme
from ..data_access import get_all_players, get_player_profile

def show_profile():
    players_df = get_all_players()
    
    st.markdown('<h1 class="main-header">PLAYER PROFILE</h1>', unsafe_allow_html=True)
    
    if not players_df.empty:
        players_df = players_df.copy()
        players_df['display_label'] = players_df.apply(lambda r: f"{r['name']} ({r['riot_id']})" if r['riot_id'] and str(r['riot_id']).strip() else r['name'], axis=1)
        
        opts = players_df['display_label'].tolist()
        sel = st.selectbox("Select a Player", opts)
        
        if sel:
            pid = int(players_df[players_df['display_label'] == sel].iloc[0]['id'])
            prof = get_player_profile(pid)
            
            if prof:
                # Header Card
                st.markdown(f"""<div class="custom-card" style="margin-bottom: 2rem;">
<div style="display: flex; align-items: center; gap: 20px;">
<div style="background: var(--primary-blue); width: 60px; height: 60px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 2rem; color: var(--bg-dark);">
{html.escape(str(prof['info'].get('name')[0].upper() if prof['info'].get('name') else 'P'))}
</div>
<div>
<h2 style="margin: 0; color: var(--primary-blue); font-family: 'Orbitron';">{html.escape(str(prof['display_name']))}</h2>
<div style="color: var(--text-dim); font-size: 1.1rem;">{html.escape(str(prof['info'].get('team') or 'Free Agent'))}</div>
</div>
</div>
</div>""", unsafe_allow_html=True)
            
            # Metrics Grid
            m1, m2, m3, m4 = st.columns(4)
            with m1:
                st.markdown(f"""<div class="custom-card" style="text-align: center;">
<div style="color: var(--text-dim); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 1px;">Games</div>
<div style="font-size: 2rem; font-family: 'Orbitron'; color: var(--text-main); margin: 10px 0;">{prof['games']}</div>
</div>""", unsafe_allow_html=True)
            with m2:
                st.markdown(f"""<div class="custom-card" style="text-align: center;">
<div style="color: var(--text-dim); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 1px;">Avg ACS</div>
<div style="font-size: 2rem; font-family: 'Orbitron'; color: var(--primary-blue); margin: 10px 0;">{prof['avg_acs']}</div>
</div>""", unsafe_allow_html=True)
            with m3:
                st.markdown(f"""<div class="custom-card" style="text-align: center;">
<div style="color: var(--text-dim); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 1px;">KD Ratio</div>
<div style="font-size: 2rem; font-family: 'Orbitron'; color: var(--primary-red); margin: 10px 0;">{prof['kd_ratio']}</div>
</div>""", unsafe_allow_html=True)
            with m4:
                st.markdown(f"""<div class="custom-card" style="text-align: center;">
<div style="color: var(--text-dim); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 1px;">Assists</div>
<div style="font-size: 2rem; font-family: 'Orbitron'; color: var(--text-main); margin: 10px 0;">{prof['total_assists']}</div>
</div>""", unsafe_allow_html=True)
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # Comparison Radar or Bar Chart
            st.markdown('<h3 style="color: var(--primary-blue); font-family: \'Orbitron\';">PERFORMANCE BENCHMARKS</h3>', unsafe_allow_html=True)
            
            cmp_df = pd.DataFrame({
                'Metric': ['ACS','Kills/Match','Deaths/Match','Assists/Match'],
                'Player': [prof['avg_acs'], prof['total_kills']/max(prof['games'],1), prof['total_deaths']/max(prof['games'],1), prof['total_assists']/max(prof['games'],1)],
                'Rank Avg': [prof['sr_avg_acs'], prof['sr_k'], prof['sr_d'], prof['sr_a']],
                'League Avg': [prof['lg_avg_acs'], prof['lg_k'], prof['lg_d'], prof['lg_a']],
            })
            
            # Plotly Bar Chart for comparison with dual axis
            fig_cmp = make_subplots(specs=[[{"secondary_y": True}]])
            
            # ACS (Primary Y-Axis)
            fig_cmp.add_trace(go.Bar(name='Player ACS', x=['ACS'], y=[prof['avg_acs']], marker_color='#3FD1FF'), secondary_y=False)
            fig_cmp.add_trace(go.Bar(name='Rank Avg ACS', x=['ACS'], y=[prof['sr_avg_acs']], marker_color='#FF4655', opacity=0.7), secondary_y=False)
            fig_cmp.add_trace(go.Bar(name='League Avg ACS', x=['ACS'], y=[prof['lg_avg_acs']], marker_color='#ECE8E1', opacity=0.5), secondary_y=False)
            
            # Per-Match Stats (Secondary Y-Axis)
            other_metrics = ['Kills/Match', 'Deaths/Match', 'Assists/Match']
            player_others = [prof['total_kills']/max(prof['games'],1), prof['total_deaths']/max(prof['games'],1), prof['total_assists']/max(prof['games'],1)]
            rank_others = [prof['sr_k'], prof['sr_d'], prof['sr_a']]
            league_others = [prof['lg_k'], prof['lg_d'], prof['lg_a']]
            
            fig_cmp.add_trace(go.Bar(name='Player Stats', x=other_metrics, y=player_others, marker_color='#3FD1FF', showlegend=False), secondary_y=True)
            fig_cmp.add_trace(go.Bar(name='Rank Avg Stats', x=other_metrics, y=rank_others, marker_color='#FF4655', opacity=0.7, showlegend=False), secondary_y=True)
            fig_cmp.add_trace(go.Bar(name='League Avg Stats', x=other_metrics, y=league_others, marker_color='#ECE8E1', opacity=0.5, showlegend=False), secondary_y=True)
            
            fig_cmp.update_layout(
                barmode='group', 
                height=400,
                title_text="Performance vs Benchmarks (ACS on Left, Others on Right)",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            fig_cmp.update_yaxes(title_text="Average Combat Score (ACS)", secondary_y=False)
            fig_cmp.update_yaxes(title_text="K/D/A Per Match", secondary_y=True)
            
            st.plotly_chart(apply_plotly_theme(fig_cmp), use_container_width=True)
            
            # Added Charts (ACS Trend, KDA Trend, Sub Impact, Maps)
            if 'trend' in prof and not prof['trend'].empty:
                st.caption("ACS trend")
                fig_acs = px.line(prof['trend'], x='label', y='avg_acs', 
                                  title="ACS Trend", markers=True,
                                  color_discrete_sequence=['#3FD1FF'])
                st.plotly_chart(apply_plotly_theme(fig_acs), use_container_width=True)
                
                st.caption("KDA trend")
                fig_kda = px.line(prof['trend'], x='label', y='kda', 
                                  title="KDA Trend", markers=True,
                                  color_discrete_sequence=['#FF4655'])
                st.plotly_chart(apply_plotly_theme(fig_kda), use_container_width=True)

            if 'sub_impact' in prof:
                sid = prof['sub_impact']
                st.caption("Substitution impact")
                c_sub1, c_sub2 = st.columns(2)
                with c_sub1:
                    fig_sub_acs = px.bar(x=['Starter', 'Sub'], y=[sid['starter_acs'], sid['sub_acs']], 
                                       title="ACS: Starter vs Sub",
                                       labels={'x': 'Role', 'y': 'ACS'},
                                       color_discrete_sequence=['#3FD1FF'])
                    st.plotly_chart(apply_plotly_theme(fig_sub_acs), use_container_width=True)
                with c_sub2:
                    fig_sub_kda = px.bar(x=['Starter', 'Sub'], y=[sid['starter_kda'], sid['sub_kda']], 
                                       title="KDA: Starter vs Sub",
                                       labels={'x': 'Role', 'y': 'KDA'},
                                       color_discrete_sequence=['#FF4655'])
                    st.plotly_chart(apply_plotly_theme(fig_sub_kda), use_container_width=True)

            if not prof['maps'].empty:
                st.markdown('<h3 style="color: var(--primary-blue); font-family: \'Orbitron\';">RECENT MATCHES</h3>', unsafe_allow_html=True)
                maps_display = prof['maps'][['match_id','map_index','agent','acs','kills','deaths','assists','is_sub']].copy()
                maps_display.columns = ['Match ID', 'Map', 'Agent', 'ACS', 'K', 'D', 'A', 'Sub']
                st.dataframe(maps_display, hide_index=True, use_container_width=True)
