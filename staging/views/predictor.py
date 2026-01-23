import streamlit as st
import pandas as pd
import html
from staging.data_access import get_teams_list, get_completed_matches

def show_predictor():
    st.markdown('<h1 class="main-header">MATCH PREDICTOR</h1>', unsafe_allow_html=True)
    st.write("Predict the outcome of a match based on team history and stats.")
    
    teams_df = get_teams_list()
    matches_df = get_completed_matches()
    
    tnames = teams_df['name'].tolist() if not teams_df.empty else []
    c1, c2 = st.columns(2)
    
    # Check if user is admin or dev
    is_privileged = st.session_state.get('is_admin', False) or st.session_state.get('role') in ['admin', 'dev']
    
    if not is_privileged:
        st.info("Prediction tools are currently locked for visitors until sufficient match data is collected.")
    
    t1_name = c1.selectbox("Team 1", tnames, index=0, disabled=not is_privileged)
    t2_name = c2.selectbox("Team 2", tnames, index=(1 if len(tnames)>1 else 0), disabled=not is_privileged)
    
    if st.button("Predict Result", disabled=not is_privileged):
        if t1_name == t2_name:
            st.error("Select two different teams.")
        else:
            t1_id = teams_df[teams_df['name'] == t1_name].iloc[0]['id']
            t2_id = teams_df[teams_df['name'] == t2_name].iloc[0]['id']
            
            # Feature extraction helper
            def get_team_stats(tid):
                import pandas as pd
                played = matches_df[(matches_df['team1_id']==tid) | (matches_df['team2_id']==tid)]
                if played.empty:
                    return {'win_rate': 0.0, 'avg_score': 0.0, 'games': 0}
                wins = played[played['winner_id'] == tid].shape[0]
                total = played.shape[0]
                
                # Calculate avg score (rounds won) using vectorized operations
                scores_t1 = played.loc[played['team1_id'] == tid, 'score_t1']
                scores_t2 = played.loc[played['team2_id'] == tid, 'score_t2']
                all_scores = pd.concat([scores_t1, scores_t2])
                avg_score = all_scores.mean() if not all_scores.empty else 0
                
                return {'win_rate': wins/total, 'avg_score': avg_score, 'games': total}

            s1 = get_team_stats(t1_id)
            s2 = get_team_stats(t2_id)
            
            # Head to head
            h2h = matches_df[((matches_df['team1_id']==t1_id) & (matches_df['team2_id']==t2_id)) | 
                             ((matches_df['team1_id']==t2_id) & (matches_df['team2_id']==t1_id))]
            h2h_wins_t1 = h2h[h2h['winner_id'] == t1_id].shape[0]
            h2h_wins_t2 = h2h[h2h['winner_id'] == t2_id].shape[0]
            
            # Heuristic Score
            # Win Rate (40%), Avg Score (30%), H2H (30%)
            # Normalize scores? No, just compare raw weighted sums or probabilities
            
            # Heuristic Score (Fallback if ML fails or data too small)
            score1 = (s1['win_rate'] * 40) + (s1['avg_score'] * 2) + (h2h_wins_t1 * 5)
            score2 = (s2['win_rate'] * 40) + (s2['avg_score'] * 2) + (h2h_wins_t2 * 5)
            
            ml_prob = None
            try:
                import predictor_model
                ml_prob = predictor_model.predict_match(t1_id, t2_id)
            except Exception as e:
                pass
                
            if ml_prob is not None:
                prob1 = ml_prob * 100
                prob2 = (1 - ml_prob) * 100
                prediction_type = "ML MODEL"
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
