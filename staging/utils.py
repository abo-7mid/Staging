import os
import streamlit as st
import requests
import base64
from .config import ROOT_DIR

def get_secret(key, default=None):
    # Try direct access first
    if key in st.secrets:
        return st.secrets[key]
        
    # Check specifically in 'admin' section if not found at root
    if "admin" in st.secrets and isinstance(st.secrets["admin"], dict):
        if key in st.secrets["admin"]:
            return st.secrets["admin"][key]
            
    # Fallback to env
    return os.getenv(key, default)

def is_safe_path(path):
    if not path:
        return False
    # Allow relative paths that might contain 'assets' but prevent escaping project root
    clean_path = path.replace('\\', '/')
    if ".." in clean_path or clean_path.startswith('/') or ":" in clean_path:
        return False
    return True

def get_visitor_ip():
    # 1. Try a fingerprint-based pseudo-IP FIRST for maximum stability
    try:
        # Use new context headers if available (Streamlit 1.34+)
        if hasattr(st, "context") and hasattr(st.context, "headers"):
            h = st.context.headers
            if h:
                import hashlib
                fingerprint_str = f"{h.get('User-Agent', '')}{h.get('Accept-Language', '')}{h.get('Accept', '')}"
                if fingerprint_str.strip():
                    return f"fp_{hashlib.md5(fingerprint_str.encode()).hexdigest()[:12]}"
    except Exception:
        pass

    # 2. Fallback to st.context IP directly
    try:
        if hasattr(st, "context"):
            if hasattr(st.context, "remote_ip") and st.context.remote_ip:
                return st.context.remote_ip
            
            headers = st.context.headers
            for header in ["X-Forwarded-For", "X-Real-IP", "Forwarded"]:
                val = headers.get(header)
                if val:
                    return val.split(",")[0].strip()
    except Exception:
        pass

    # 3. Absolute last resort (will change on refresh)
    if 'pseudo_ip' not in st.session_state:
        import uuid
        st.session_state['pseudo_ip'] = f"tmp_{uuid.uuid4().hex[:8]}"
    return st.session_state['pseudo_ip']

@st.cache_data(ttl=3600)
def get_base64_image(image_path):
    if not image_path:
        return None
    
    # Resolve relative path against ROOT_DIR
    if not os.path.isabs(image_path):
        full_path = os.path.join(ROOT_DIR, image_path)
    else:
        full_path = image_path

    if not os.path.exists(full_path):
        return None
        
    try:
        with open(full_path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except Exception:
        return None

def fetch_match_from_github(match_id):
    """
    Attempts to fetch a match JSON from the GitHub repository.
    """
    owner = get_secret("GH_OWNER")
    repo = get_secret("GH_REPO")
    token = get_secret("GH_TOKEN")
    branch = get_secret("GH_BRANCH", "main")
    
    if not owner or not repo:
        return None, "GitHub configuration missing (GH_OWNER/GH_REPO)"
        
    # Use API for both public and private repos if token is available
    if token:
        url = f"https://api.github.com/repos/{owner}/{repo}/contents/assets/matches/match_{match_id}.json?ref={branch}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.raw"
        }
        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                return r.json(), None
            else:
                return None, f"GitHub API error: {r.status_code}"
        except Exception as e:
            return None, f"GitHub API fetch error: {str(e)}"
    else:
        # Fallback to public raw URL
        raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/assets/matches/match_{match_id}.json"
        try:
            r = requests.get(raw_url, timeout=10)
            if r.status_code == 200:
                return r.json(), None
            else:
                return None, f"GitHub file not found (Status: {r.status_code})"
        except Exception as e:
            return None, f"GitHub fetch error: {str(e)}"

def parse_tracker_json(jsdata, team1_id, team2_id, all_players_df):
    """
    Parses Tracker.gg JSON data and matches it to team1_id and team2_id.
    Returns (json_suggestions, map_name, t1_rounds, t2_rounds)
    """
    import re
    import pandas as pd
    
    json_suggestions = {}
    segments = jsdata.get("data", {}).get("segments", [])
    
    # First pass: find team names/IDs to identify which Tracker team is which
    tracker_team_1_id = None
    team_segments = [s for s in segments if s.get("type") == "team-summary"]
    
    riot_id_to_name = {}
    name_to_name = {}
    if not all_players_df.empty:
        # Create a case-insensitive map of riot_id -> player name
        riot_id_to_name = {str(r).strip().lower(): str(n) for r, n in zip(all_players_df['riot_id'], all_players_df['name']) if pd.notna(r)}
        # Also map name -> name for fallback
        name_to_name = {str(n).strip().lower(): str(n) for n in all_players_df['name'] if pd.notna(n)}

    if len(team_segments) >= 2:
        # Use Riot IDs to match teams
        t1_id_int = int(team1_id) if team1_id is not None else None
        t2_id_int = int(team2_id) if team2_id is not None else None
        
        # Team 1 Roster
        t1_roster_df = all_players_df[all_players_df['default_team_id'] == t1_id_int]
        t1_rids = [str(r).strip().lower() for r in t1_roster_df['riot_id'].dropna()]
        t1_names = [str(n).strip().lower() for n in t1_roster_df['name'].dropna()]
        t1_names_clean = [n.replace('@', '').strip() for n in t1_names]
        
        # Team 2 Roster
        t2_roster_df = all_players_df[all_players_df['default_team_id'] == t2_id_int]
        t2_rids = [str(r).strip().lower() for r in t2_roster_df['riot_id'].dropna()]
        t2_names = [str(n).strip().lower() for n in t2_roster_df['name'].dropna()]
        t2_names_clean = [n.replace('@', '').strip() for n in t2_names]
        
        team_ids_in_json = [ts.get("attributes", {}).get("teamId") for ts in team_segments]
        
        # Count matches for each Tracker team against our rosters
        # score[tracker_team_id][db_team_id]
        scores = {tid: {1: 0, 2: 0} for tid in team_ids_in_json}
        
        for p_seg in [s for s in segments if s.get("type") == "player-summary"]:
            t_id = p_seg.get("metadata", {}).get("teamId")
            if t_id in scores:
                rid = p_seg.get("metadata", {}).get("platformInfo", {}).get("platformUserIdentifier")
                if not rid: rid = p_seg.get("metadata", {}).get("platformInfo", {}).get("platformUserHandle")
                
                if rid:
                    rid_clean = str(rid).strip().lower()
                    name_part = rid_clean.split('#')[0]
                    
                    # Match vs Team 1
                    is_t1 = rid_clean in t1_rids or rid_clean in t1_names or name_part in t1_names or name_part in t1_names_clean
                    if not is_t1:
                        # Try partial match for name_part
                        for tn in t1_names_clean:
                            if name_part in tn or tn in name_part:
                                is_t1 = True
                                break
                    if is_t1: scores[t_id][1] += 1
                    
                    # Match vs Team 2
                    is_t2 = rid_clean in t2_rids or rid_clean in t2_names or name_part in t2_names or name_part in t2_names_clean
                    if not is_t2:
                        # Try partial match for name_part
                        for tn in t2_names_clean:
                            if name_part in tn or tn in name_part:
                                is_t2 = True
                                break
                    if is_t2: scores[t_id][2] += 1
        
        # Decision logic:
        # Option A: TrackerTeam0 is Team 1, TrackerTeam1 is Team 2
        score_a = scores[team_ids_in_json[0]][1] + scores[team_ids_in_json[1]][2]
        # Option B: TrackerTeam0 is Team 2, TrackerTeam1 is Team 1
        score_b = scores[team_ids_in_json[0]][2] + scores[team_ids_in_json[1]][1]
        
        if score_a >= score_b and score_a > 0:
            tracker_team_1_id = team_ids_in_json[0]
        elif score_b > score_a:
            tracker_team_1_id = team_ids_in_json[1]
        else:
            # Tie or 0 matches? Default to first team
            tracker_team_1_id = team_ids_in_json[0]
    else:
        if team_segments:
            tracker_team_1_id = team_segments[0].get("attributes", {}).get("teamId")
        else:
            tracker_team_1_id = None

    for seg in segments:
        if seg.get("type") == "player-summary":
            metadata = seg.get("metadata", {})
            platform_info = metadata.get("platformInfo", {})
            rid = platform_info.get("platformUserIdentifier")
            
            # Tracker sometimes puts the name in platformUserHandle or platformUserIdentifier
            if not rid:
                rid = platform_info.get("platformUserHandle")
            
            if rid:
                rid = str(rid).strip()
            
            agent = metadata.get("agentName")
            st_map = seg.get("stats", {})
            acs = st_map.get("scorePerRound", {}).get("value", 0)
            k = st_map.get("kills", {}).get("value", 0)
            d = st_map.get("deaths", {}).get("value", 0)
            a = st_map.get("assists", {}).get("value", 0)
            t_id = metadata.get("teamId")
            
            our_team_num = 1 if t_id == tracker_team_1_id else 2
            
            if rid:
                rid_lower = rid.lower()
                # Try to find a match in our DB if direct match fails
                matched_name = riot_id_to_name.get(rid_lower)
                
                # If still no match, try matching the name part of rid (if it's Name#Tag) or rid itself against DB names
                if not matched_name:
                    name_part = rid.split('#')[0].lower()
                    matched_name = name_to_name.get(name_part) or name_to_name.get(rid_lower)
                
                # Store by riot_id but also provide the matched name if found
                json_suggestions[rid_lower] = {
                    'name': matched_name, # Found in DB or None
                    'tracker_name': rid,  # Original name from Tracker
                    'acs': int(acs) if acs is not None else 0, 
                    'k': int(k) if k is not None else 0, 
                    'd': int(d) if d is not None else 0, 
                    'a': int(a) if a is not None else 0, 
                    'agent': agent,
                    'team_num': our_team_num,
                    'conf': 100.0 if matched_name else 80.0
                }
    
    # Extract map name and rounds
    map_name = jsdata.get("data", {}).get("metadata", {}).get("mapName")
    t1_r = 0
    t2_r = 0
    
    if len(team_segments) >= 2:
        if tracker_team_1_id == team_segments[0].get("attributes", {}).get("teamId"):
            t1_r = team_segments[0].get("stats", {}).get("roundsWon", {}).get("value", 0)
            t2_r = team_segments[1].get("stats", {}).get("roundsWon", {}).get("value", 0)
        else:
            t1_r = team_segments[1].get("stats", {}).get("roundsWon", {}).get("value", 0)
            t2_r = team_segments[0].get("stats", {}).get("roundsWon", {}).get("value", 0)
            
    return json_suggestions, map_name, int(t1_r), int(t2_r)

def apply_plotly_theme(fig):
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='#ECE8E1',
        font_family='Inter',
        title_font_family='Orbitron',
        title_font_color='#3FD1FF',
        xaxis=dict(
            gridcolor='rgba(255,255,255,0.05)', 
            zerolinecolor='rgba(255,255,255,0.1)',
            tickfont=dict(color='#8B97A5'),
            title_font=dict(color='#8B97A5')
        ),
        yaxis=dict(
            gridcolor='rgba(255,255,255,0.05)', 
            zerolinecolor='rgba(255,255,255,0.1)',
            tickfont=dict(color='#8B97A5'),
            title_font=dict(color='#8B97A5')
        ),
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(
            bgcolor='rgba(0,0,0,0)',
            bordercolor='rgba(255,255,255,0.1)',
            font=dict(color='#ECE8E1')
        )
    )
    return fig
