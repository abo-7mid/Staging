import streamlit as st
import pandas as pd
import html
from staging.data_access import get_teams_list, get_completed_matches, get_all_players, get_conn

def show_predictor():
    st.markdown('<h1 class="main-header">MATCH PREDICTOR</h1>', unsafe_allow_html=True)
    st.write("Predict the outcome of a match based on team history and stats.")
    
    # 1. Automatic Weekly Predictions
    conn = get_conn()
    try:
        # Find the next week with scheduled matches
        # Get max completed week first
        res = conn.execute("SELECT MAX(week) FROM matches WHERE status='completed'").fetchone()
        last_completed_week = int(res[0] or 0)
        current_week = last_completed_week + 1
        
        # Get scheduled matches for this week (or all future)
        # The user asked for "unplayed matches of the current week always till the playoffs"
        # We'll just fetch all 'scheduled' matches sorted by week
        scheduled = pd.read_sql_query("""
            SELECT m.id, m.week, m.team1_id, m.team2_id, t1.name as t1, t2.name as t2, m.group_name
            FROM matches m
            JOIN teams t1 ON m.team1_id = t1.id
            JOIN teams t2 ON m.team2_id = t2.id
            WHERE m.status = 'scheduled'
            ORDER BY m.week ASC, m.id ASC
        """, conn)
    finally:
        conn.close()
        
    if not scheduled.empty:
        st.markdown(f"### 📅 Upcoming Matches Predictions")
        
        # Group by week
        weeks = scheduled['week'].unique()
        for wk in weeks:
            if wk > 6: continue # Skip playoffs for now unless handled
            
            st.markdown(f"#### Week {wk}")
            week_matches = scheduled[scheduled['week'] == wk]
            
            # Display grid of predictions
            cols = st.columns(3)
            for i, (_, m) in enumerate(week_matches.iterrows()):
                with cols[i % 3]:
                    # Run Prediction
                    try:
                        import predictor.predictor_model as pm
                        prob = pm.predict_match(m['team1_id'], m['team2_id'], wk)
                    except:
                        prob = 0.5
                    
                    if prob is None: prob = 0.5
                    
                    t1_win_prob = prob * 100
                    winner = m['t1'] if t1_win_prob > 50 else m['t2']
                    conf = max(t1_win_prob, 100 - t1_win_prob)
                    
                    color = "#2ECC71" if conf > 60 else "#F1C40F"
                    
                    st.markdown(f"""
                    <div style="background: rgba(255,255,255,0.05); border-radius: 8px; padding: 10px; margin-bottom: 10px; border-left: 4px solid {color};">
                        <div style="font-size: 0.8em; color: #aaa;">{m['t1']} vs {m['t2']}</div>
                        <div style="font-weight: bold; font-size: 1.1em; color: {color};">{winner}</div>
                        <div style="font-size: 0.9em;">{conf:.1f}% Confidence</div>
                    </div>
                    """, unsafe_allow_html=True)
        st.markdown("---")

    # 2. Custom Predictor
    st.markdown("### 🛠️ Custom Scenario Predictor")
    
    teams_df = get_teams_list()
    matches_df = get_completed_matches()
    all_players = get_all_players()
    
    tnames = teams_df['name'].tolist() if not teams_df.empty else []
    c1, c2 = st.columns(2)
    
    # Check if user is admin or dev
    is_privileged = st.session_state.get('is_admin', False) or st.session_state.get('role') in ['admin', 'dev']
    
    t1_name = c1.selectbox("Team 1", tnames, index=0)
    t2_name = c2.selectbox("Team 2", tnames, index=(1 if len(tnames)>1 else 0))
    
    # Advanced Options (Roster Selection)
    with st.expander("Advanced Options (Roster & Map)"):
        # Map Selection (Placeholder for now, but passed to model if we implement map-specifics later)
        map_opts = ["Ascent", "Bind", "Breeze", "Fracture", "Haven", "Icebox", "Lotus", "Pearl", "Split", "Sunset"]
        sel_maps = st.multiselect("Map(s) (Optional)", map_opts)
        
        # Roster Selection
        # Filter players by selected teams for convenience, but allow all
        t1_id = teams_df[teams_df['name'] == t1_name].iloc[0]['id']
        t2_id = teams_df[teams_df['name'] == t2_name].iloc[0]['id']
        
        t1_default = all_players[all_players['default_team_id'] == t1_id]['id'].tolist()
        t2_default = all_players[all_players['default_team_id'] == t2_id]['id'].tolist()
        
        # Create map for multiselect
        player_map = {f"{r['name']} ({r['riot_id'] or ''})": r['id'] for _, r in all_players.iterrows()}
        player_map_inv = {v: k for k, v in player_map.items()}
        
        # Pre-select defaults
        t1_def_labels = [player_map_inv.get(pid) for pid in t1_default if pid in player_map_inv]
        t2_def_labels = [player_map_inv.get(pid) for pid in t2_default if pid in player_map_inv]
        
        ac1, ac2 = st.columns(2)
        with ac1:
            t1_sel = st.multiselect(f"{t1_name} Roster", list(player_map.keys()), default=t1_def_labels)
        with ac2:
            t2_sel = st.multiselect(f"{t2_name} Roster", list(player_map.keys()), default=t2_def_labels)

    if st.button("Predict Result", type="primary"):
        if t1_name == t2_name:
            st.error("Select two different teams.")
        else:
            t1_pids = [player_map[l] for l in t1_sel]
            t2_pids = [player_map[l] for l in t2_sel]
            
            overrides = {
                't1_players': t1_pids,
                't2_players': t2_pids,
                'map': sel_maps if sel_maps else None
            }
            
            # Feature extraction helper (Local for heuristic fallback)
            def get_team_stats_local(tid, pids=None):
                # Basic stats from team history
                played = matches_df[(matches_df['team1_id']==tid) | (matches_df['team2_id']==tid)]
                if played.empty:
                    base = {'win_rate': 0.0, 'avg_score': 0.0, 'games': 0}
                else:
                    wins = played[played['winner_id'] == tid].shape[0]
                    total = played.shape[0]
                    scores_t1 = played.loc[played['team1_id'] == tid, 'score_t1']
                    scores_t2 = played.loc[played['team2_id'] == tid, 'score_t2']
                    all_scores = pd.concat([scores_t1, scores_t2])
                    avg_score = all_scores.mean() if not all_scores.empty else 0
                    base = {'win_rate': wins/total, 'avg_score': avg_score, 'games': total}
                
                # If custom roster, try to adjust (this is just for display/heuristic)
                # For ML, we use the model's logic
                return base

            s1 = get_team_stats_local(t1_id, t1_pids)
            s2 = get_team_stats_local(t2_id, t2_pids)
            
            # Head to head
            h2h = matches_df[((matches_df['team1_id']==t1_id) & (matches_df['team2_id']==t2_id)) | 
                             ((matches_df['team1_id']==t2_id) & (matches_df['team2_id']==t1_id))]
            h2h_wins_t1 = h2h[h2h['winner_id'] == t1_id].shape[0]
            h2h_wins_t2 = h2h[h2h['winner_id'] == t2_id].shape[0]
            
            # Heuristic Score
            score1 = (s1['win_rate'] * 40) + (s1['avg_score'] * 2) + (h2h_wins_t1 * 5)
            score2 = (s2['win_rate'] * 40) + (s2['avg_score'] * 2) + (h2h_wins_t2 * 5)
            
            ml_prob = None
            try:
                import predictor.predictor_model as pm
                # Pass overrides to model
                ml_prob = pm.predict_match(t1_id, t2_id, overrides=overrides)
            except Exception as e:
                print(e)
                pass
                
            if ml_prob is not None:
                prob1 = ml_prob * 100
                prob2 = (1 - ml_prob) * 100
                prediction_type = "ML MODEL (Custom Roster)"
            else:
                total = score1 + score2
                if total == 0:
                    prob1 = 50.0
                    prob2 = 50.0
                else:
                    prob1 = (score1 / total) * 100
                    prob2 = (score2 / total) * 100
                prediction_type = "HEURISTIC"
                
            winner = t1_name if prob1 > prob2 else t2_name
            conf = max(prob1, prob2)
            
            st.markdown(f"""<div class="custom-card" style="text-align: center; border-top: 4px solid { 'var(--primary-blue)' if winner == t1_name else 'var(--primary-red)' };">
<div style="color: var(--text-dim); font-size: 0.7rem; margin-bottom: 5px;">{prediction_type} PREDICTION</div>
<h2 style="margin: 0; color: { 'var(--primary-blue)' if winner == t1_name else 'var(--primary-red)' };">{html.escape(str(winner))}</h2>
<div style="font-size: 3rem; font-family: 'Orbitron'; margin: 10px 0;">{conf:.1f}%</div>
<div style="color: var(--text-dim);">CONFIDENCE LEVEL</div>
</div>""", unsafe_allow_html=True)

            # Probability Bar
            st.markdown(f"""<div style="width: 100%; background: rgba(255,255,255,0.05); height: 20px; border-radius: 10px; overflow: hidden; display: flex; margin: 20px 0;">
<div style="width: {prob1}%; background: var(--primary-blue); height: 100%; transition: width 1s ease-in-out;"></div>
<div style="width: {prob2}%; background: var(--primary-red); height: 100%; transition: width 1s ease-in-out;"></div>
</div>
<div style="display: flex; justify-content: space-between; font-family: 'Orbitron'; font-size: 0.8rem;">
<div style="color: var(--primary-blue);">{html.escape(str(t1_name))} ({prob1:.1f}%)</div>
<div style="color: var(--primary-red);">{html.escape(str(t2_name))} ({prob2:.1f}%)</div>
</div>""", unsafe_allow_html=True)
            
            c1, c2 = st.columns(2)
            with c1:
                st.markdown(f"""<div class="custom-card">
<h3 style="color: var(--primary-blue); margin-top: 0;">{html.escape(str(t1_name))} Analysis</h3>
<div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px;">
<div>
<div style="color: var(--text-dim); font-size: 0.7rem; text-transform: uppercase;">Win Rate</div>
<div style="font-size: 1.2rem; font-family: 'Orbitron';">{s1['win_rate']:.0%}</div>
</div>
<div>
<div style="color: var(--text-dim); font-size: 0.7rem; text-transform: uppercase;">Avg Score</div>
<div style="font-size: 1.2rem; font-family: 'Orbitron';">{s1['avg_score']:.1f}</div>
</div>
<div>
<div style="color: var(--text-dim); font-size: 0.7rem; text-transform: uppercase;">H2H Wins</div>
<div style="font-size: 1.2rem; font-family: 'Orbitron';">{h2h_wins_t1}</div>
</div>
</div>
</div>""", unsafe_allow_html=True)
            with c2:
                st.markdown(f"""<div class="custom-card">
<h3 style="color: var(--primary-red); margin-top: 0;">{html.escape(str(t2_name))} Analysis</h3>
<div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px;">
<div>
<div style="color: var(--text-dim); font-size: 0.7rem; text-transform: uppercase;">Win Rate</div>
<div style="font-size: 1.2rem; font-family: 'Orbitron';">{s2['win_rate']:.0%}</div>
</div>
<div>
<div style="color: var(--text-dim); font-size: 0.7rem; text-transform: uppercase;">Avg Score</div>
<div style="font-size: 1.2rem; font-family: 'Orbitron';">{s2['avg_score']:.1f}</div>
</div>
<div>
<div style="color: var(--text-dim); font-size: 0.7rem; text-transform: uppercase;">H2H Wins</div>
<div style="font-size: 1.2rem; font-family: 'Orbitron';">{h2h_wins_t2}</div>
</div>
</div>
</div>""", unsafe_allow_html=True)
