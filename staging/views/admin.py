import streamlit as st
import pandas as pd
import json
import os
import re
from ..data_access import (
    get_teams_list, get_all_players, get_match_weeks, get_completed_matches,
    upsert_match_maps, get_conn, import_sqlite_db, export_db_bytes, reset_db,
    get_match_maps, get_team_history_counts, get_agents_list
)
from ..utils import parse_tracker_json, backup_db_to_github
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
                
                # Session State keys for preview
                preview_key = f"preview_data_{mid}"
                
                with st.expander("Result Entry", expanded=True):
                    # 1. Match Link (Preferred)
                    tracker_url = st.text_input("Tracker.gg Match URL", placeholder="https://tracker.gg/valorant/match/...", key=f"url_{mid}")
                    
                    # 2. File Uploader (Backup)
                    uploaded_file = st.file_uploader("Or Upload JSON", type=['json'], key=f"u_{mid}")
                    
                    if st.button("Parse & Preview", type="primary"):
                        js_data = None
                        
                        # Logic to get JSON
                        if tracker_url:
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

                        if js_data:
                            try:
                                # Get match details for team IDs
                                conn = get_conn()
                                m_info = pd.read_sql("SELECT * FROM matches WHERE id=?", conn, params=(mid,)).iloc[0]
                                conn.close()
                                
                                # Get all players for mapping
                                all_players = get_all_players()
                                
                                # Parse JSON
                                suggestions, map_name, t1_r, t2_r = parse_tracker_json(js_data, m_info['team1_id'], m_info['team2_id'], all_players)
                                
                                # Store in session state
                                st.session_state[preview_key] = {
                                    "suggestions": suggestions,
                                    "map_name": map_name,
                                    "t1_r": t1_r,
                                    "t2_r": t2_r,
                                    "m_info": m_info
                                }
                                st.rerun() # Rerun to show preview
                                
                            except Exception as e:
                                st.error(f"Error processing match data: {str(e)}")

                # Display Preview if available
                if preview_key in st.session_state:
                    p_data = st.session_state[preview_key]
                    st.markdown("---")
                    st.subheader("Match Preview")
                    st.markdown(f"**Map:** {p_data['map_name']} | **Score:** {p_data['t1_r']} - {p_data['t2_r']}")
                    
                    # Convert suggestions to DataFrame for display
                    sugg_data = []
                    for rid, s in p_data['suggestions'].items():
                        sugg_data.append({
                            "Tracker Name": s['tracker_name'],
                            "DB Name": s['name'] if s['name'] else "❌ Not Found",
                            "Team": "Team 1" if s['team_num'] == 1 else "Team 2",
                            "Agent": s['agent'],
                            "ACS": s['acs'],
                            "K/D/A": f"{s['k']}/{s['d']}/{s['a']}"
                        })
                    
                    st.dataframe(pd.DataFrame(sugg_data), use_container_width=True)
                    
                    if st.button("CONFIRM & SAVE MATCH", type="primary", key=f"save_{mid}"):
                        save_match_result(mid, p_data['map_name'], p_data['t1_r'], p_data['t2_r'], p_data['suggestions'], p_data['m_info'])
                        del st.session_state[preview_key]
                        st.toast("Match saved successfully!")
                        st.rerun()
                        
                    if st.button("Cancel", key=f"cancel_{mid}"):
                        del st.session_state[preview_key]
                        st.rerun()

    elif action == "Edit Schedule":
        st.markdown("### Bulk Schedule Editor")
        st.info("Paste the schedule text below. Format:\n\n——— GROUP NAME —————————\nTeam A vs Team B")
        
        # Week Selection
        week_num = st.number_input("Week Number", min_value=1, max_value=10, value=1)
        
        schedule_text = st.text_area("Paste Schedule Text", height=300)
        
        if st.button("Parse & Schedule Matches", type="primary"):
            if schedule_text:
                parse_and_schedule(schedule_text, week_num)
            else:
                st.warning("Please paste some text.")
        
    elif action == "Manage Playoffs":
        st.info("Playoff management feature coming soon.")

def parse_and_schedule(text, week):
    lines = text.split('\n')
    current_group = None
    teams_list = get_teams_list()
    
    # Create name lookup
    name_to_id = {r['name'].lower(): r['id'] for _, r in teams_list.iterrows()}
    
    matches_to_add = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Check for group header
        # Regex: ——— GROUP —————————
        # Handle various dashes and spacing
        group_match = re.match(r'^[—\-]+\s*(.+?)\s*[—\-]+$', line)
        if group_match:
            current_group = group_match.group(1).strip()
            continue
            
        # Check for match
        if " vs " in line:
            parts = line.split(" vs ")
            if len(parts) == 2:
                t1_name = parts[0].strip()
                t2_name = parts[1].strip()
                
                t1_id = name_to_id.get(t1_name.lower())
                t2_id = name_to_id.get(t2_name.lower())
                
                if t1_id and t2_id:
                    matches_to_add.append({
                        "week": week,
                        "group": current_group if current_group else "Unknown",
                        "t1_id": t1_id,
                        "t2_id": t2_id,
                        "t1_name": t1_name,
                        "t2_name": t2_name
                    })
                else:
                    st.warning(f"Could not find team(s): {t1_name} (Found: {bool(t1_id)}) vs {t2_name} (Found: {bool(t2_id)})")

    if matches_to_add:
        st.write(f"Found {len(matches_to_add)} matches to schedule:")
        st.dataframe(pd.DataFrame(matches_to_add))
        
        if st.button("Confirm Schedule Import"):
            conn = get_conn()
            try:
                count = 0
                for m in matches_to_add:
                    # Check if exists
                    existing = conn.execute(
                        "SELECT id FROM matches WHERE week=? AND team1_id=? AND team2_id=?", 
                        (m['week'], m['t1_id'], m['t2_id'])
                    ).fetchone()
                    
                    if not existing:
                        conn.execute(
                            "INSERT INTO matches (week, group_name, team1_id, team2_id, status) VALUES (?, ?, ?, ?, 'scheduled')",
                            (m['week'], m['group'], m['t1_id'], m['t2_id'])
                        )
                        count += 1
                conn.commit()
                st.success(f"Successfully scheduled {count} new matches!")
            except Exception as e:
                st.error(f"Database error: {e}")
            finally:
                conn.close()

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
                # If name was found in utils.parse_tracker_json (which returns matched name)
                # We need to find the ID. 
                # Simplified: try to find ID by name in players table
                # A more robust way would be to return ID from parse_tracker_json
                p_row = conn.execute("SELECT id FROM players WHERE name=?", (stats['name'],)).fetchone()
                if p_row:
                    pid = p_row[0]
            
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
    
    # Filters
    col_f1, col_f2, col_f3 = st.columns(3)
    df = get_all_players()
    
    with col_f1:
        search_name = st.text_input("Search Name")
    
    with col_f2:
        teams = get_teams_list()
        team_opts = {r['name']: r['id'] for _, r in teams.iterrows()}
        team_opts["Free Agent"] = None
        filter_team = st.selectbox("Filter Team", ["All"] + list(team_opts.keys()))
        
    with col_f3:
        ranks = ["Iron", "Bronze", "Silver", "Gold", "Platinum", "Diamond", "Ascendant", "Immortal 1", "Immortal 2", "Immortal 3", "Radiant"]
        filter_rank = st.selectbox("Filter Rank", ["All"] + ranks)

    # Filter Logic
    if not df.empty:
        if search_name:
            df = df[df['name'].str.contains(search_name, case=False, na=False)]
        if filter_team != "All":
            tid = team_opts[filter_team]
            if tid is None:
                df = df[df['default_team_id'].isnull()]
            else:
                df = df[df['default_team_id'] == tid]
        if filter_rank != "All":
            # Simple string match for now, could be more complex if "Immortal" covers "Immortal 1/2/3"
            if "Immortal" in filter_rank and " " not in filter_rank:
                 df = df[df['rank'].str.contains("Immortal", case=False, na=False)]
            else:
                 df = df[df['rank'] == filter_rank]

    # Add Player
    with st.expander("Add New Player"):
        with st.form("add_player"):
            name = st.text_input("Name")
            riot_id = st.text_input("Riot ID (Name#Tag)")
            rank = st.selectbox("Rank", ranks)
            
            # Team Selection (Optional)
            team_sel = st.selectbox("Team", ["None"] + list(team_opts.keys()))
            
            if st.form_submit_button("Create Player"):
                if name:
                    conn = get_conn()
                    try:
                        tid = None
                        if team_sel != "None" and team_sel != "Free Agent":
                            tid = team_opts[team_sel]
                            
                        conn.execute(
                            "INSERT INTO players (name, riot_id, rank, default_team_id) VALUES (?, ?, ?, ?)",
                            (name, riot_id, rank, tid)
                        )
                        conn.commit()
                        st.success(f"Player {name} added!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")
                    finally:
                        conn.close()

    # List Players
    st.markdown(f"### Player List ({len(df)})")
    if not df.empty:
        st.dataframe(df, use_container_width=True)

def show_admin_teams():
    st.subheader("Manage Teams")
    
    with st.expander("Add New Team"):
        with st.form("add_team"):
            name = st.text_input("Team Name")
            tag = st.text_input("Tag (2-4 chars)")
            group = st.selectbox("Group", ["A", "B", "KINGDOM", "OMEGA", "ALPHA", "ATLAS"])
            
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
                        
    # List Teams
    teams = get_teams_list()
    st.dataframe(teams, use_container_width=True)

def show_admin_system():
    st.subheader("System Operations")
    
    c1, c2 = st.columns(2)
    
    with c1:
        st.markdown("### Database Backup")
        data = export_db_bytes()
        if data:
            st.download_button("Download Database (.sqlite)", data, "valorant_s23.db", "application/x-sqlite3")
            
        st.markdown("### Cloud Backup")
        if st.button("Backup DB to GitHub", type="primary"):
            with st.spinner("Backing up to GitHub..."):
                success, msg = backup_db_to_github()
                if success:
                    st.success(msg)
                else:
                    st.error(msg)
            
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
