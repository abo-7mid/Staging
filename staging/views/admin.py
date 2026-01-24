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
                                m_info = pd.read_sql("""
                                    SELECT m.*, t1.name as t1, t2.name as t2 
                                    FROM matches m 
                                    LEFT JOIN teams t1 ON m.team1_id = t1.id 
                                    LEFT JOIN teams t2 ON m.team2_id = t2.id 
                                    WHERE m.id=?
                                """, conn, params=(mid,)).iloc[0]
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
                    
                    # Prepare Data for Comparison
                    all_players_df = get_all_players()
                    t1_id = p_data['m_info']['team1_id']
                    t2_id = p_data['m_info']['team2_id']
                    
                    # Get IDs of players currently on these teams
                    t1_roster_ids = set(all_players_df[all_players_df['default_team_id'] == t1_id]['id'].tolist())
                    t2_roster_ids = set(all_players_df[all_players_df['default_team_id'] == t2_id]['id'].tolist())
                    
                    # 1. Identify Present Players (First Pass)
                    t1_present_ids = set()
                    t2_present_ids = set()
                    
                    for rid, s in p_data['suggestions'].items():
                        if s['name']:
                            p_row = all_players_df[all_players_df['name'] == s['name']]
                            if not p_row.empty:
                                pid = p_row.iloc[0]['id']
                                if s['team_num'] == 1:
                                    t1_present_ids.add(pid)
                                else:
                                    t2_present_ids.add(pid)

                    # 2. Identify Missing Players (Candidates for Subbing)
                    t1_missing_ids = list(t1_roster_ids - t1_present_ids)
                    t2_missing_ids = list(t2_roster_ids - t2_present_ids)
                    
                    # Helper to get name
                    def get_name(pid):
                        r = all_players_df[all_players_df['id'] == pid]
                        if not r.empty:
                            return r.iloc[0]['name']
                        return "Unknown"

                    # 3. Process players for display (Second Pass)
                    t1_preview_list = []
                    t2_preview_list = []
                    
                    for rid, s in p_data['suggestions'].items():
                        # Find Player ID
                        pid = None
                        if s['name']:
                            p_row = all_players_df[all_players_df['name'] == s['name']]
                            if not p_row.empty:
                                pid = p_row.iloc[0]['id']
                        
                        # Determine Status
                        status = "OK"
                        status_label = "VERIFIED"
                        icon = "✅"
                        color = "rgba(19, 195, 125, 0.1)" # Green
                        border = "#13C37D"
                        subbed_for_name = None
                        
                        if not pid:
                            status = "NOT_FOUND"
                            status_label = "NOT FOUND"
                            icon = "❌"
                            color = "rgba(255, 75, 75, 0.1)" # Red
                            border = "#FF4B4B"
                        else:
                            # Check roster
                            assigned_roster_ids = t1_roster_ids if s['team_num'] == 1 else t2_roster_ids
                            if pid not in assigned_roster_ids:
                                status = "SUB"
                                status_label = "SUBSTITUTION"
                                icon = "⚠️"
                                color = "rgba(255, 164, 37, 0.1)" # Orange
                                border = "#FFA425"
                                
                                # Determine who they are subbing for
                                if s['team_num'] == 1:
                                    if t1_missing_ids:
                                        mid = t1_missing_ids.pop(0)
                                        subbed_for_name = get_name(mid)
                                else:
                                    if t2_missing_ids:
                                        mid = t2_missing_ids.pop(0)
                                        subbed_for_name = get_name(mid)
                        
                        item = {
                            "data": s,
                            "status": status,
                            "label": status_label,
                            "icon": icon,
                            "color": color,
                            "border": border,
                            "subbed_for": subbed_for_name
                        }
                        
                        if s['team_num'] == 1:
                            t1_preview_list.append(item)
                        else:
                            t2_preview_list.append(item)
                            
                    # Display Columns
                    col1, col2 = st.columns(2)
                    
                    def render_player_card(item):
                        s = item['data']
                        sub_text = ""
                        if item.get('subbed_for'):
                            sub_text = f"<div style='font-size: 0.8em; color: {item['border']};'>Subbing for: {item['subbed_for']}</div>"
                            
                        st.markdown(
                            f"""
                            <div style="
                                background-color: {item['color']}; 
                                border: 1px solid {item['border']}; 
                                border-radius: 8px; 
                                padding: 10px; 
                                margin-bottom: 8px;
                                display: flex; 
                                justify-content: space-between; 
                                align-items: center;
                            ">
                                <div>
                                    <div style="font-weight: bold; font-size: 1em;">
                                        {item['icon']} {s['name'] if s['name'] else '<span style="color:#FF4B4B">Unknown Player</span>'}
                                    </div>
                                    <div style="font-size: 0.8em; color: #aaa;">
                                        Tracker: {s['tracker_name']}
                                    </div>
                                    <div style="font-size: 0.7em; font-weight: bold; color: {item['border']}; margin-top: 2px;">
                                        {item['label']}
                                    </div>
                                    {sub_text}
                                </div>
                                <div style="text-align: right;">
                                    <div style="font-weight: bold;">{s['agent']}</div>
                                    <div style="font-size: 0.9em;">{s['acs']} ACS</div>
                                    <div style="font-size: 0.8em; color: #ccc;">{s['k']}/{s['d']}/{s['a']}</div>
                                </div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

                    with col1:
                        st.subheader(f"🛡️ {p_data['m_info']['t1']}")
                        for item in t1_preview_list:
                            render_player_card(item)
                            
                    with col2:
                        st.subheader(f"⚔️ {p_data['m_info']['t2']}")
                        for item in t2_preview_list:
                            render_player_card(item)

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
        
        # Get team rosters to determine missing players (for sub mapping)
        # We need to find who is supposed to be playing but isn't
        t1_roster_df = pd.read_sql("SELECT id FROM players WHERE default_team_id=?", conn, params=(match_info['team1_id'],))
        t2_roster_df = pd.read_sql("SELECT id FROM players WHERE default_team_id=?", conn, params=(match_info['team2_id'],))
        
        t1_roster_ids = set(t1_roster_df['id'].tolist())
        t2_roster_ids = set(t2_roster_df['id'].tolist())
        
        # Pre-resolve players to find who is playing
        resolved_players = []
        t1_present_ids = set()
        t2_present_ids = set()
        
        for riot_id, stats in player_stats.items():
            pid = None
            default_tid = None
            
            if stats['name']:
                p_row = conn.execute("SELECT id, default_team_id FROM players WHERE name=?", (stats['name'],)).fetchone()
                if p_row:
                    pid = p_row[0]
                    default_tid = p_row[1]
            
            if pid:
                if stats['team_num'] == 1:
                    t1_present_ids.add(pid)
                else:
                    t2_present_ids.add(pid)
            
            resolved_players.append({
                'riot_id': riot_id,
                'stats': stats,
                'pid': pid,
                'default_tid': default_tid
            })
            
        # Identify missing players (candidates for being subbed out)
        t1_missing = list(t1_roster_ids - t1_present_ids)
        t2_missing = list(t2_roster_ids - t2_present_ids)
        
        for p in resolved_players:
            stats = p['stats']
            pid = p['pid']
            default_tid = p['default_tid']
            
            # Determine Team ID (Match Team)
            # Ensure int for database consistency
            team_id = int(match_info['team1_id']) if stats['team_num'] == 1 else int(match_info['team2_id'])
            
            is_sub = 0
            subbed_for_id = None
            
            if pid:
                # Check for Sub
                # If player's default team is different from the team they played for -> Sub
                # Note: default_tid can be None (Free Agent) -> counted as Sub if playing for a team
                if default_tid != team_id:
                    is_sub = 1
                    
                    # Assign subbed_for_id
                    # Heuristic: Assign to the first missing player from the roster
                    if stats['team_num'] == 1:
                        if t1_missing:
                            subbed_for_id = t1_missing.pop(0)
                    else:
                        if t2_missing:
                            subbed_for_id = t2_missing.pop(0)
            
            # Insert Stats
            conn.execute(
                """
                INSERT INTO match_stats_map (match_id, map_index, team_id, player_id, agent, acs, kills, deaths, assists, is_sub, subbed_for_id)
                VALUES (?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (match_id, team_id, pid, stats['agent'], stats['acs'], stats['k'], stats['d'], stats['a'], is_sub, subbed_for_id)
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
        ranks = ["Iron/Bronze", "Silver", "Gold", "Platinum", "Diamond", "Ascendant", "Immortal 1/2", "Immortal 3/Radiant"]
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
