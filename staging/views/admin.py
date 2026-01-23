import streamlit as st
import pandas as pd
import json
import os
from ..data_access import (
    get_teams_list, get_all_players, get_match_weeks, get_completed_matches,
    upsert_match_maps, get_conn, import_sqlite_db, export_db_bytes, reset_db,
    get_match_maps, get_team_history_counts, get_agents_list
)
from ..utils import parse_tracker_json
from ..auth import create_admin_with_role

def show_admin_panel():
    st.markdown('<h2 class="main-header">ADMINISTRATION</h2>', unsafe_allow_html=True)
    
    # Check if user is admin
    if not st.session_state.get('is_admin'):
        st.error("Access Denied")
        return

    tabs = st.tabs(["MATCHES", "PLAYERS", "TEAMS", "SYSTEM"])
    
    # --- MATCHES TAB ---
    with tabs[0]:
        show_admin_matches()

    # --- PLAYERS TAB ---
    with tabs[1]:
        show_admin_players()
        
    # --- TEAMS TAB ---
    with tabs[2]:
        show_admin_teams()
        
    # --- SYSTEM TAB ---
    with tabs[3]:
        show_admin_system()

def show_admin_matches():
    st.subheader("Match Management")
    
    action = st.radio("Action", ["Enter Match Results", "Edit Schedule", "Manage Playoffs"], horizontal=True)
    
    if action == "Enter Match Results":
        st.markdown("### Enter Match Result")
        
        # Select Match
        weeks = get_match_weeks()
        sel_week = st.selectbox("Select Week", weeks, index=len(weeks)-1 if weeks else 0)
        
        conn = get_conn()
        matches_df = pd.read_sql_query(
            "SELECT m.id, t1.name as t1, t2.name as t2, m.status FROM matches m JOIN teams t1 ON m.team1_id=t1.id JOIN teams t2 ON m.team2_id=t2.id WHERE m.week=? ORDER BY m.id",
            conn,
            params=(sel_week,)
        )
        conn.close()
        
        if matches_df.empty:
            st.warning("No matches found for this week.")
        else:
            match_opts = {f"{r['id']}: {r['t1']} vs {r['t2']} ({r['status']})": r['id'] for _, r in matches_df.iterrows()}
            sel_match_label = st.selectbox("Select Match", list(match_opts.keys()))
            
            if sel_match_label:
                mid = match_opts[sel_match_label]
                with st.expander("Result Entry", expanded=True):
                    # 1. Match Link (Preferred)
                    tracker_url = st.text_input("Tracker.gg Match URL", placeholder="https://tracker.gg/valorant/match/...")
                    
                    # 2. File Uploader (Backup)
                    uploaded_file = st.file_uploader("Or Upload JSON", type=['json'], key=f"u_{mid}")
                    
                    # 3. Manual Override
                    manual_entry = st.checkbox("Manual Data Entry", value=False)
                    
                    js_data = None
                    
                    # Logic to get JSON
                    if tracker_url:
                        # Extract ID
                        try:
                            # Extract ID from URL (last part)
                            match_uuid = tracker_url.strip().split('/')[-1]
                            json_path = os.path.join(os.getcwd(), "assets", "matches", f"match_{match_uuid}.json")
                            
                            if os.path.exists(json_path):
                                with open(json_path, 'r', encoding='utf-8') as f:
                                    js_data = json.load(f)
                                st.info(f"Loaded match data from assets: match_{match_uuid}.json")
                            else:
                                st.warning(f"No local file found for ID: {match_uuid} (Checked: {json_path})")
                        except Exception as e:
                            st.error(f"Error parsing URL: {e}")
                            
                    if not js_data and uploaded_file:
                        try:
                            js_data = json.load(uploaded_file)
                        except Exception as e:
                            st.error(f"Invalid JSON file: {e}")

                    if js_data and not manual_entry:
                        try:
                            # Get match details for team IDs
                            conn = get_conn()
                            m_info = pd.read_sql("SELECT * FROM matches WHERE id=?", conn, params=(mid,)).iloc[0]
                            conn.close()
                            
                            # Get all players for mapping
                            all_players = get_all_players()
                            
                            # Parse JSON
                            suggestions, map_name, t1_r, t2_r = parse_tracker_json(js_data, m_info['team1_id'], m_info['team2_id'], all_players)
                            
                            st.success(f"Parsed: {map_name} | Score: {t1_r}-{t2_r}")
                            
                            # Confirm & Save
                            if st.button("Save Match Data", type="primary"):
                                save_match_result(mid, map_name, t1_r, t2_r, suggestions, m_info)
                                st.toast("Match saved successfully!")
                                st.rerun()
                                
                        except Exception as e:
                            st.error(f"Error processing match data: {str(e)}")
                            
                    elif manual_entry:

                        st.info("Manual entry feature coming soon.")

    elif action == "Edit Schedule":
        st.info("Schedule editing feature coming soon.")
        
    elif action == "Manage Playoffs":
        st.info("Playoff management feature coming soon.")

def save_match_result(match_id, map_name, t1_rounds, t2_rounds, player_stats, match_info):
    conn = get_conn()
    try:
        # Update Match
        winner = match_info['team1_id'] if t1_rounds > t2_rounds else match_info['team2_id']
        if t1_rounds == t2_rounds: winner = None # Draw
        
        conn.execute(
            "UPDATE matches SET status='completed', score_t1=?, score_t2=?, winner_id=?, maps_played=1 WHERE id=?",
            (t1_rounds, t2_rounds, winner, match_id)
        )
        
        # Update Map
        conn.execute("DELETE FROM match_maps WHERE match_id=?", (match_id,))
        conn.execute(
            "INSERT INTO match_maps (match_id, map_index, map_name, team1_rounds, team2_rounds, winner_id) VALUES (?, 0, ?, ?, ?, ?)",
            (match_id, map_name, t1_rounds, t2_rounds, winner)
        )
        
        # Update Stats
        conn.execute("DELETE FROM match_stats_map WHERE match_id=?", (match_id,))
        
        # Get team rosters to map stats to teams
        t1_roster = pd.read_sql("SELECT id, name, riot_id FROM players WHERE default_team_id=?", conn, params=(match_info['team1_id'],))
        t2_roster = pd.read_sql("SELECT id, name, riot_id FROM players WHERE default_team_id=?", conn, params=(match_info['team2_id'],))
        
        # Create lookups
        t1_lookup = {str(r).lower(): i for r, i in zip(t1_roster['riot_id'], t1_roster['id']) if r}
        t1_lookup.update({str(n).lower(): i for n, i in zip(t1_roster['name'], t1_roster['id'])})
        
        t2_lookup = {str(r).lower(): i for r, i in zip(t2_roster['riot_id'], t2_roster['id']) if r}
        t2_lookup.update({str(n).lower(): i for n, i in zip(t2_roster['name'], t2_roster['id'])})
        
        for riot_id, stats in player_stats.items():
            # Determine Team ID
            team_id = match_info['team1_id'] if stats['team_num'] == 1 else match_info['team2_id']
            
            # Find Player ID
            pid = None
            if stats['name']:
                # If name was found in utils.parse_tracker_json
                # We need to find the ID. 
                # This is a bit circular, ideally parse_tracker_json returns IDs if we passed a DF with IDs
                # But we can look it up again
                pass
            
            # Try to resolve PID from DB based on riot_id or name
            # (Simplified for now, assuming stats['name'] matches DB name)
            
            # Insert Stats
            conn.execute(
                """
                INSERT INTO match_stats_map (match_id, map_index, team_id, player_id, agent, acs, kills, deaths, assists)
                VALUES (?, 0, ?, ?, ?, ?, ?, ?, ?)
                """,
                (match_id, team_id, pid, stats['agent'], stats['acs'], stats['k'], stats['d'], stats['a'])
            )
            
        conn.commit()
    except Exception as e:
        st.error(f"Database Error: {e}")
    finally:
        conn.close()

def show_admin_players():
    st.subheader("Manage Players")
    
    # Add Player
    with st.expander("Add New Player"):
        with st.form("add_player"):
            name = st.text_input("Name")
            riot_id = st.text_input("Riot ID (Name#Tag)")
            rank = st.selectbox("Rank", ["Iron", "Bronze", "Silver", "Gold", "Platinum", "Diamond", "Ascendant", "Immortal", "Radiant"])
            
            teams = get_teams_list()
            team_opts = {r['name']: r['id'] for _, r in teams.iterrows()}
            team_sel = st.selectbox("Team", list(team_opts.keys()))
            
            if st.form_submit_button("Create Player"):
                if name and team_sel:
                    conn = get_conn()
                    try:
                        conn.execute(
                            "INSERT INTO players (name, riot_id, rank, default_team_id) VALUES (?, ?, ?, ?)",
                            (name, riot_id, rank, team_opts[team_sel])
                        )
                        conn.commit()
                        st.success(f"Player {name} added!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")
                    finally:
                        conn.close()

    # List Players
    st.markdown("### Player List")
    df = get_all_players()
    if not df.empty:
        st.dataframe(df, use_container_width=True)

def show_admin_teams():
    st.subheader("Manage Teams")
    
    with st.expander("Add New Team"):
        with st.form("add_team"):
            name = st.text_input("Team Name")
            tag = st.text_input("Tag (2-4 chars)")
            group = st.selectbox("Group", ["A", "B"])
            
            if st.form_submit_button("Create Team"):
                if name and tag:
                    conn = get_conn()
                    try:
                        conn.execute(
                            "INSERT INTO teams (name, tag, group_name) VALUES (?, ?, ?)",
                            (name, tag, group)
                        )
                        conn.commit()
                        st.success(f"Team {name} added!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")
                    finally:
                        conn.close()

def show_admin_system():
    st.subheader("System Operations")
    
    c1, c2 = st.columns(2)
    
    with c1:
        st.markdown("### Database Backup")
        data = export_db_bytes()
        if data:
            st.download_button("Download Database (.sqlite)", data, "valorant_s23.db", "application/x-sqlite3")
            
        st.markdown("### Database Restore")
        up = st.file_uploader("Upload .db or .sqlite file", type=['db', 'sqlite'])
        if up:
            if st.button("Restore Database", type="primary"):
                summary = import_sqlite_db(up.getvalue())
                st.success(f"Restored: {summary}")
                st.rerun()
                
    with c2:
        st.markdown("### Admin Management")
        with st.form("new_admin"):
            u = st.text_input("Username")
            p = st.text_input("Password", type="password")
            r = st.selectbox("Role", ["admin", "editor"])
            if st.form_submit_button("Create Admin"):
                create_admin_with_role(u, p, r)
                st.success("Admin created")
