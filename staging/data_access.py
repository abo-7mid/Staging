import pandas as pd
import numpy as np
import os
import streamlit as st
from .db import (
    get_conn, DB_PATH, ensure_base_schema, init_admin_table, 
    init_session_activity_table, init_match_stats_map_table, 
    ensure_upgrade_schema, import_sqlite_db, export_db_bytes, reset_db
)
from .auth import ensure_seed_admins

@st.cache_data(ttl=300)
def get_substitutions_log():
    conn = get_conn()
    try:
        df = pd.read_sql(
            """
            SELECT msm.match_id, msm.map_index, m.week, m.group_name,
                   t.name AS team, p.name AS player, p.riot_id AS player_riot,
                   sp.name AS subbed_for, sp.riot_id AS sub_riot,
                   msm.agent, msm.acs, msm.kills, msm.deaths, msm.assists
            FROM match_stats_map msm
            JOIN matches m ON msm.match_id = m.id
            LEFT JOIN teams t ON msm.team_id = t.id
            LEFT JOIN players p ON msm.player_id = p.id
            LEFT JOIN players sp ON msm.subbed_for_id = sp.id
            WHERE msm.is_sub = 1 AND m.status = 'completed'
            ORDER BY m.week, msm.match_id, msm.map_index
            """,
            conn,
        )
        if not df.empty:
            df['player'] = df.apply(lambda r: f"{r['player']} ({r['player_riot']})" if r['player_riot'] and str(r['player_riot']).strip() else r['player'], axis=1)
            df['subbed_for'] = df.apply(lambda r: f"{r['subbed_for']} ({r['sub_riot']})" if r['sub_riot'] and str(r['sub_riot']).strip() else r['subbed_for'], axis=1)
            df = df.drop(columns=['player_riot', 'sub_riot'])
    except Exception:
        conn.close()
        return pd.DataFrame()
    conn.close()
    return df

@st.cache_data(ttl=300)
def get_player_profile(player_id):
    conn = get_conn()
    try:
        info = pd.read_sql(
            "SELECT p.id, p.name, p.riot_id, p.rank, t.tag as team FROM players p LEFT JOIN teams t ON p.default_team_id=t.id WHERE p.id=?",
            conn,
            params=(int(player_id),),
        )
        if info.empty:
            conn.close()
            return {}
            
        # Format name to include Riot ID if available
        p_name = info.iloc[0]['name']
        p_riot = info.iloc[0]['riot_id']
        display_name = f"{p_name} ({p_riot})" if p_riot and str(p_riot).strip() else p_name
        
        rank_val = info.iloc[0]['rank']
        
        # Stats with match metadata in one go
        stats = pd.read_sql(
            """
            SELECT msm.match_id, msm.map_index, msm.agent, msm.acs, msm.kills, msm.deaths, msm.assists, msm.is_sub, m.week, mm.map_name
            FROM match_stats_map msm
            JOIN matches m ON msm.match_id = m.id
            LEFT JOIN match_maps mm ON msm.match_id = mm.match_id AND msm.map_index = mm.map_index
            WHERE msm.player_id=? AND m.status = 'completed'
            """,
            conn,
            params=(int(player_id),),
        )
        
        # Combined Benchmarks
        bench = pd.read_sql(
            """
            SELECT 
                AVG(msm.acs) as lg_acs, AVG(msm.kills) as lg_k, AVG(msm.deaths) as lg_d, AVG(msm.assists) as lg_a,
                AVG(CASE WHEN p.rank = ? THEN msm.acs ELSE NULL END) as r_acs,
                AVG(CASE WHEN p.rank = ? THEN msm.kills ELSE NULL END) as r_k,
                AVG(CASE WHEN p.rank = ? THEN msm.deaths ELSE NULL END) as r_d,
                AVG(CASE WHEN p.rank = ? THEN msm.assists ELSE NULL END) as r_a
            FROM match_stats_map msm
            JOIN matches m ON msm.match_id = m.id
            JOIN players p ON msm.player_id = p.id
            WHERE m.status = 'completed'
            """,
            conn,
            params=(rank_val, rank_val, rank_val, rank_val)
        ).iloc[0]
        
        trend = pd.DataFrame()
        if not stats.empty:
            agg = stats.groupby('match_id').agg({'acs':'mean','kills':'sum','deaths':'sum','week':'first'}).reset_index()
            agg['kda'] = agg['kills'] / agg['deaths'].replace(0, 1)
            agg['label'] = 'W' + agg['week'].fillna(0).astype(int).astype(str) + '-M' + agg['match_id'].astype(int).astype(str)
            agg = agg.rename(columns={'acs':'avg_acs'})
            trend = agg[['label','avg_acs','kda']]
            
        conn.close()
    except Exception:
        if 'conn' in locals(): conn.close()
        return {}
        
    games = stats['match_id'].nunique() if not stats.empty else 0
    avg_acs = float(stats['acs'].mean()) if not stats.empty else 0.0
    total_k = int(stats['kills'].sum()) if not stats.empty else 0
    total_d = int(stats['deaths'].sum()) if not stats.empty else 0
    total_a = int(stats['assists'].sum()) if not stats.empty else 0
    kd = (total_k / (total_d if total_d != 0 else 1)) if not stats.empty else 0.0
    
    sub_impact = None
    if not stats.empty:
        s_sub = stats[stats['is_sub'] == 1]
        s_sta = stats[stats['is_sub'] == 0]
        sub_impact = {
            'sub_acs': float(s_sub['acs'].mean()) if not s_sub.empty else 0.0,
            'starter_acs': float(s_sta['acs'].mean()) if not s_sta.empty else 0.0,
            'sub_kda': float((s_sub['kills'].sum() / max(s_sub['deaths'].sum(), 1))) if not s_sub.empty else 0.0,
            'starter_kda': float((s_sta['kills'].sum() / max(s_sta['deaths'].sum(), 1))) if not s_sta.empty else 0.0,
        }

    return {
        'info': info.iloc[0].to_dict(),
        'display_name': display_name,
        'games': int(games),
        'avg_acs': round(avg_acs, 1),
        'total_kills': total_k,
        'total_deaths': total_d,
        'total_assists': total_a,
        'kd_ratio': round(kd, 2),
        'sr_avg_acs': round(float(bench['r_acs'] or 0), 1),
        'sr_k': round(float(bench['r_k'] or 0), 1),
        'sr_d': round(float(bench['r_d'] or 0), 1),
        'sr_a': round(float(bench['r_a'] or 0), 1),
        'lg_avg_acs': round(float(bench['lg_acs'] or 0), 1),
        'lg_k': round(float(bench['lg_k'] or 0), 1),
        'lg_d': round(float(bench['lg_d'] or 0), 1),
        'lg_a': round(float(bench['lg_a'] or 0), 1),
        'maps': stats,
        'trend': trend,
        'sub_impact': sub_impact,
    }

def upsert_match_maps(match_id, maps_data):
    conn = get_conn()
    c = conn.cursor()
    for m in maps_data:
        c.execute("SELECT id FROM match_maps WHERE match_id=? AND map_index=?", (match_id, m['map_index']))
        ex = c.fetchone()
        if ex:
            c.execute(
                "UPDATE match_maps SET map_name=?, team1_rounds=?, team2_rounds=?, winner_id=?, is_forfeit=? WHERE id=?",
                (m['map_name'], m['team1_rounds'], m['team2_rounds'], m['winner_id'], m.get('is_forfeit', 0), ex[0])
            )
        else:
            c.execute(
                "INSERT INTO match_maps (match_id, map_index, map_name, team1_rounds, team2_rounds, winner_id, is_forfeit) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (match_id, m['map_index'], m['map_name'], m['team1_rounds'], m['team2_rounds'], m['winner_id'], m.get('is_forfeit', 0))
            )
    conn.commit()
    conn.close()

@st.cache_data(ttl=300)
def get_standings():
    conn = get_conn()
    try:
        # Optimized SQL Query to replace Pandas aggregation
        query = """
        WITH team_matches AS (
            -- Matches where team is team1
            SELECT 
                team1_id as team_id,
                CASE WHEN COALESCE(mm.team1_rounds, m.score_t1) > COALESCE(mm.team2_rounds, m.score_t2) THEN 1 ELSE 0 END as win,
                CASE WHEN COALESCE(mm.team1_rounds, m.score_t1) < COALESCE(mm.team2_rounds, m.score_t2) THEN 1 ELSE 0 END as loss,
                CASE 
                    WHEN COALESCE(mm.team1_rounds, m.score_t1) > COALESCE(mm.team2_rounds, m.score_t2) THEN 15 
                    ELSE MIN(COALESCE(mm.team1_rounds, m.score_t1), 12) 
                END as points,
                CASE 
                    WHEN COALESCE(mm.team2_rounds, m.score_t2) > COALESCE(mm.team1_rounds, m.score_t1) THEN 15 
                    ELSE MIN(COALESCE(mm.team2_rounds, m.score_t2), 12) 
                END as points_against
            FROM matches m
            LEFT JOIN match_maps mm ON m.id = mm.match_id AND mm.map_index = 0
            WHERE m.status = 'completed' AND m.match_type = 'regular' AND (m.format IS NULL OR UPPER(m.format)='BO1')

            UNION ALL

            -- Matches where team is team2
            SELECT 
                team2_id as team_id,
                CASE WHEN COALESCE(mm.team2_rounds, m.score_t2) > COALESCE(mm.team1_rounds, m.score_t1) THEN 1 ELSE 0 END as win,
                CASE WHEN COALESCE(mm.team2_rounds, m.score_t2) < COALESCE(mm.team1_rounds, m.score_t1) THEN 1 ELSE 0 END as loss,
                CASE 
                    WHEN COALESCE(mm.team2_rounds, m.score_t2) > COALESCE(mm.team1_rounds, m.score_t1) THEN 15 
                    ELSE MIN(COALESCE(mm.team2_rounds, m.score_t2), 12) 
                END as points,
                CASE 
                    WHEN COALESCE(mm.team1_rounds, m.score_t1) > COALESCE(mm.team2_rounds, m.score_t2) THEN 15 
                    ELSE MIN(COALESCE(mm.team1_rounds, m.score_t1), 12) 
                END as points_against
            FROM matches m
            LEFT JOIN match_maps mm ON m.id = mm.match_id AND mm.map_index = 0
            WHERE m.status = 'completed' AND m.match_type = 'regular' AND (m.format IS NULL OR UPPER(m.format)='BO1')
        )
        SELECT 
            t.id, t.name, t.group_name, t.logo_path,
            COALESCE(SUM(tm.win), 0) as Wins,
            COALESCE(SUM(tm.loss), 0) as Losses,
            COALESCE(SUM(tm.points), 0) as Points,
            COALESCE(SUM(tm.points_against), 0) as "Points Against",
            COALESCE(COUNT(tm.team_id), 0) as Played,
            (COALESCE(SUM(tm.points), 0) - COALESCE(SUM(tm.points_against), 0)) as PD
        FROM teams t
        LEFT JOIN team_matches tm ON t.id = tm.team_id
        GROUP BY t.id
        ORDER BY Points DESC, "Points Against" ASC
        """
        df = pd.read_sql_query(query, conn)
        
        # Filter out dummy teams if needed
        df = df[~df['name'].isin(['FAT1', 'FAT2'])]
        
    except Exception:
        conn.close()
        return pd.DataFrame()
    conn.close()
    
    # Pre-calculate logo display safety
    if not df.empty:
        df['logo_display'] = [
            p if p and not (".." in p or p.startswith("/") or p.startswith("\\")) and os.path.exists(p) 
            else None 
            for p in df['logo_path']
        ]
    
    return df


@st.cache_data(ttl=60)
def get_player_leaderboard():
    conn = get_conn()
    try:
        df = pd.read_sql_query(
            """
            SELECT p.id as player_id,
                   p.name,
                   p.riot_id,
                   t.tag as team,
                   COUNT(DISTINCT msm.match_id) as games,
                   AVG(msm.acs) as avg_acs,
                   SUM(msm.kills) as total_kills,
                   SUM(msm.deaths) as total_deaths,
                   SUM(msm.assists) as total_assists
            FROM match_stats_map msm
            JOIN matches m ON msm.match_id = m.id
            JOIN players p ON msm.player_id = p.id
            LEFT JOIN teams t ON p.default_team_id = t.id
            WHERE m.status = 'completed'
            GROUP BY p.id, p.name, p.riot_id
            HAVING games > 0
            """,
            conn,
        )
    except Exception:
        conn.close()
        return pd.DataFrame()
    conn.close()
    
    if not df.empty:
        # Format name to include Riot ID if available
        df['name'] = df.apply(lambda r: f"{r['name']} ({r['riot_id']})" if r['riot_id'] and str(r['riot_id']).strip() else r['name'], axis=1)
        df = df.drop(columns=['riot_id'])
        df['kd_ratio'] = df['total_kills'] / df['total_deaths'].replace(0, 1)
        df['avg_acs'] = df['avg_acs'].round(1)
        df['kd_ratio'] = df['kd_ratio'].round(2)
    return df.sort_values('avg_acs', ascending=False)

@st.cache_data(ttl=60)
def get_week_matches(week):
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT m.id, m.week, m.group_name, m.status, m.format, m.maps_played, m.is_forfeit,
               t1.name as t1_name, t2.name as t2_name,
               m.score_t1, m.score_t2, t1.id as t1_id, t2.id as t2_id,
               mm.team1_rounds, mm.team2_rounds
        FROM matches m
        JOIN teams t1 ON m.team1_id = t1.id
        JOIN teams t2 ON m.team2_id = t2.id
        LEFT JOIN match_maps mm ON m.id = mm.match_id AND mm.map_index = 0
        WHERE m.week = ? AND m.match_type = 'regular'
        ORDER BY m.id
        """,
        conn,
        params=(week,),
    )
    # For BO1, if we have map rounds, use them as the primary scores for display
    if not df.empty and 'team1_rounds' in df.columns:
        is_bo1 = (df['format'].str.upper() == 'BO1') | (df['format'].isna())
        df.loc[is_bo1 & df['team1_rounds'].notna(), 'score_t1'] = df.loc[is_bo1 & df['team1_rounds'].notna(), 'team1_rounds']
        df.loc[is_bo1 & df['team2_rounds'].notna(), 'score_t2'] = df.loc[is_bo1 & df['team2_rounds'].notna(), 'team2_rounds']
        
        # Ensure integer type for scores to avoid .0 display
        df['score_t1'] = df['score_t1'].astype(int)
        df['score_t2'] = df['score_t2'].astype(int)
    conn.close()
    return df

@st.cache_data(ttl=300)
def get_playoff_matches():
    conn = get_conn()
    df = pd.read_sql_query(
        """
        SELECT m.id, m.playoff_round, m.bracket_pos, m.status, m.format, m.maps_played, m.is_forfeit,
               m.bracket_label,
               t1.name as t1_name, t2.name as t2_name,
               m.score_t1, m.score_t2, t1.id as t1_id, t2.id as t2_id,
               m.winner_id,
               mm.team1_rounds, mm.team2_rounds
        FROM matches m
        LEFT JOIN teams t1 ON m.team1_id = t1.id
        LEFT JOIN teams t2 ON m.team2_id = t2.id
        LEFT JOIN match_maps mm ON m.id = mm.match_id AND mm.map_index = 0
        WHERE m.match_type = 'playoff'
        ORDER BY m.playoff_round ASC, m.bracket_pos ASC
        """,
        conn
    )
    # For BO1, if we have map rounds, use them as the primary scores for display
    if not df.empty and 'team1_rounds' in df.columns:
        is_bo1 = (df['format'].str.upper() == 'BO1') | (df['format'].isna())
        df.loc[is_bo1 & df['team1_rounds'].notna(), 'score_t1'] = df.loc[is_bo1 & df['team1_rounds'].notna(), 'team1_rounds']
        df.loc[is_bo1 & df['team2_rounds'].notna(), 'score_t2'] = df.loc[is_bo1 & df['team2_rounds'].notna(), 'team2_rounds']
        
        # Ensure integer type for scores to avoid .0 display
        df['score_t1'] = df['score_t1'].astype(int)
        df['score_t2'] = df['score_t2'].astype(int)
    conn.close()
    return df

@st.cache_data(ttl=300)
def get_match_maps(match_id):
    conn = get_conn()
    df = pd.read_sql_query(
        "SELECT map_index, map_name, team1_rounds, team2_rounds, winner_id, is_forfeit FROM match_maps WHERE match_id=? ORDER BY map_index",
        conn,
        params=(match_id,),
    )
    conn.close()
    return df

@st.cache_data(ttl=300)
def get_all_players_directory(format_names=True):
    conn = get_conn()
    try:
        df = pd.read_sql(
            """
            SELECT p.id, p.name, p.riot_id, p.rank, t.name as team
            FROM players p
            LEFT JOIN teams t ON p.default_team_id = t.id
            ORDER BY p.name
            """,
            conn
        )
    except Exception:
        df = pd.DataFrame(columns=['id','name','riot_id','rank','team'])
    finally:
        conn.close()
    
    if not df.empty and format_names:
        df['name'] = df.apply(lambda r: f"{r['name']} ({r['riot_id']})" if r['riot_id'] and str(r['riot_id']).strip() else r['name'], axis=1)
    
    return df

@st.cache_data(ttl=300)
def get_map_stats(match_id, map_index, team_id):
    conn = get_conn()
    try:
        df = pd.read_sql(
            """
            SELECT p.name, p.riot_id, ms.agent, ms.acs, ms.kills, ms.deaths, ms.assists, ms.is_sub 
            FROM match_stats_map ms 
            JOIN players p ON ms.player_id=p.id 
            WHERE ms.match_id=? AND ms.map_index=? AND ms.team_id=?
            """, 
            conn, 
            params=(int(match_id), int(map_index), int(team_id))
        )
        if not df.empty:
            df['name'] = df.apply(lambda r: f"{r['name']} ({r['riot_id']})" if r['riot_id'] and str(r['riot_id']).strip() else r['name'], axis=1)
            df = df.drop(columns=['riot_id'])
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df

@st.cache_data(ttl=300)
def get_team_history_counts():
    conn = get_conn()
    try:
        df = pd.read_sql_query(
            "SELECT team_id, COUNT(DISTINCT season_id) as season_count FROM team_history GROUP BY team_id",
            conn,
        )
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df

@st.cache_data(ttl=300)
def get_all_players():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, name, riot_id, rank, default_team_id FROM players ORDER BY name", conn)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df

@st.cache_data(ttl=300)
def get_teams_list_full():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT id, name, tag, group_name, logo_path FROM teams ORDER BY name", conn)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df

@st.cache_data(ttl=300)
def get_teams_list():
    df = get_teams_list_full()
    return df[['id', 'name']] if not df.empty else pd.DataFrame(columns=['id', 'name'])

@st.cache_data(ttl=3600)
def get_agents_list():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT name FROM agents ORDER BY name", conn)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df['name'].tolist() if not df.empty else []

@st.cache_data(ttl=300)
def get_match_weeks():
    conn = get_conn()
    try:
        df = pd.read_sql_query("SELECT DISTINCT week FROM matches ORDER BY week", conn)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df['week'].tolist() if not df.empty else []

@st.cache_data(ttl=300)
def get_latest_played_week():
    conn = get_conn()
    try:
        # Get the max week that has at least one completed match
        res = conn.execute("SELECT MAX(week) FROM matches WHERE status='completed'").fetchone()
        if res and res[0]:
            return int(res[0])
    except Exception:
        pass
    conn.close()
    return 1 # Default to week 1 if no matches played

@st.cache_data(ttl=300)
def get_completed_matches():
    conn = get_conn()
    try:
        df = pd.read_sql("SELECT * FROM matches WHERE status='completed'", conn)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df

