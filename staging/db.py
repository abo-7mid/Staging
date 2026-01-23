import sqlite3
import os
import streamlit as st
import pandas as pd
from .config import ROOT_DIR, CURRENT_DIR
from .utils import get_secret

# Use data folder for database
DEFAULT_DB_PATH = os.path.join(ROOT_DIR, "data", "valorant_s23.db")
SECRET_DB_PATH = get_secret("DB_PATH")

if SECRET_DB_PATH:
    # If the secret path exists as is, use it
    if os.path.exists(SECRET_DB_PATH):
        DB_PATH = SECRET_DB_PATH
    # If it's just a filename and exists in the data folder, use that
    elif os.path.exists(os.path.join(ROOT_DIR, "data", os.path.basename(SECRET_DB_PATH))):
        DB_PATH = os.path.join(ROOT_DIR, "data", os.path.basename(SECRET_DB_PATH))
    # Otherwise, fallback to secret but it might create an empty DB
    else:
        DB_PATH = SECRET_DB_PATH
else:
    DB_PATH = DEFAULT_DB_PATH

@st.cache_resource(ttl=3600)
def get_snowflake_conn(account, user, password, warehouse, database, schema):
    import snowflake.connector
    return snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

@st.cache_resource(ttl=3600)
def get_postgres_conn(host, database, user, password):
    import psycopg2
    return psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )

def get_conn():
    # Phase 1: Cloud DB Support
    cloud_type = get_secret("CLOUD_DB_TYPE")
    
    if cloud_type == "snowflake":
        try:
            return get_snowflake_conn(
                account=get_secret("SNOWFLAKE_ACCOUNT"),
                user=get_secret("SNOWFLAKE_USER"),
                password=get_secret("SNOWFLAKE_PASSWORD"),
                warehouse=get_secret("SNOWFLAKE_WAREHOUSE"),
                database=get_secret("SNOWFLAKE_DATABASE"),
                schema=get_secret("SNOWFLAKE_SCHEMA")
            )
        except ImportError:
            st.error("Snowflake connector not installed.")
            return None
        except Exception as e:
            st.error(f"Snowflake connection failed: {e}")
            return None
            
    elif cloud_type == "postgres":
        try:
            return get_postgres_conn(
                host=get_secret("POSTGRES_HOST"),
                database=get_secret("POSTGRES_DB"),
                user=get_secret("POSTGRES_USER"),
                password=get_secret("POSTGRES_PASSWORD")
            )
        except ImportError:
            st.error("psycopg2 not installed.")
            return None
        except Exception as e:
            st.error(f"Postgres connection failed: {e}")
            return None

    # Ensure directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)

def init_admin_table(conn=None):
    should_close = False
    if conn is None:
        conn = get_conn()
        should_close = True
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS admins (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash BLOB NOT NULL,
            salt BLOB NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    if should_close:
        conn.commit()
        conn.close()

def init_session_activity_table(conn=None):
    should_close = False
    if conn is None:
        conn = get_conn()
        should_close = True
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS session_activity (
            session_id TEXT PRIMARY KEY,
            username TEXT,
            role TEXT,
            last_activity REAL,
            ip_address TEXT
        )
        """
    )
    # Check if ip_address column exists (for existing databases)
    cursor = conn.execute("PRAGMA table_info(session_activity)")
    columns = [row[1] for row in cursor.fetchall()]
    if 'ip_address' not in columns:
        conn.execute("ALTER TABLE session_activity ADD COLUMN ip_address TEXT")
        
    if should_close:
        conn.commit()
        conn.close()

def ensure_base_schema(conn=None):
    should_close = False
    if conn is None:
        conn = get_conn()
        should_close = True
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS teams (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tag TEXT,
        name TEXT UNIQUE,
        group_name TEXT,
        captain TEXT,
        co_captain TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS players (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE,
        riot_id TEXT,
        rank TEXT,
        default_team_id INTEGER,
        FOREIGN KEY(default_team_id) REFERENCES teams(id)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS matches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        week INTEGER,
        group_name TEXT,
        team1_id INTEGER,
        team2_id INTEGER,
        winner_id INTEGER,
        score_t1 INTEGER DEFAULT 0,
        score_t2 INTEGER DEFAULT 0,
        status TEXT DEFAULT 'scheduled',
        format TEXT,
        maps_played INTEGER DEFAULT 0,
        FOREIGN KEY(team1_id) REFERENCES teams(id),
        FOREIGN KEY(team2_id) REFERENCES teams(id),
        FOREIGN KEY(winner_id) REFERENCES teams(id)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS match_maps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id INTEGER NOT NULL,
        map_index INTEGER NOT NULL,
        map_name TEXT,
        team1_rounds INTEGER,
        team2_rounds INTEGER,
        winner_id INTEGER
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS match_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id INTEGER NOT NULL,
        player_id INTEGER NOT NULL,
        team_id INTEGER,
        acs INTEGER,
        kills INTEGER,
        deaths INTEGER,
        assists INTEGER
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS agents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE
    )''')
    if should_close:
        conn.commit()
        conn.close()

def ensure_column(table, column_name, column_def_sql, conn=None):
    # Allowed tables for security validation
    ALLOWED_TABLES = {"teams", "players", "matches", "match_maps", "match_stats_map", "match_stats", "agents", "seasons", "team_history", "admins"}
    if table not in ALLOWED_TABLES:
        return # Skip if table is not allowed
    
    should_close = False
    if conn is None:
        conn = get_conn()
        should_close = True
    
    c = conn.cursor()
    # Use string interpolation only for table names which are now validated against ALLOWED_TABLES
    cols = [r[1] for r in c.execute(f"PRAGMA table_info({table})").fetchall()]
    if column_name not in cols:
        try:
            # Validate column_def_sql basic structure to prevent injection if it comes from untrusted source
            # In this app, it is hardcoded, but good practice to validate
            c.execute(f"ALTER TABLE {table} ADD COLUMN {column_def_sql}")
        except sqlite3.OperationalError:
            pass
    
    if should_close:
        conn.commit()
        conn.close()

def ensure_upgrade_schema(conn=None):
    should_close = False
    if conn is None:
        conn = get_conn()
        should_close = True
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS seasons (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE,
        start_date TEXT,
        end_date TEXT,
        is_active BOOLEAN DEFAULT 0
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS team_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        team_id INTEGER,
        season_id INTEGER,
        final_rank INTEGER,
        group_name TEXT,
        FOREIGN KEY(team_id) REFERENCES teams(id),
        FOREIGN KEY(season_id) REFERENCES seasons(id)
    )''')
    
    ensure_column("teams", "logo_path", "logo_path TEXT", conn=conn)
    ensure_column("players", "rank", "rank TEXT", conn=conn)
    ensure_column("matches", "format", "format TEXT", conn=conn)
    ensure_column("matches", "maps_played", "maps_played INTEGER DEFAULT 0", conn=conn)
    ensure_column("seasons", "is_active", "is_active BOOLEAN DEFAULT 0", conn=conn)
    ensure_column("admins", "role", "role TEXT DEFAULT 'admin'", conn=conn)
    ensure_column("matches", "match_type", "match_type TEXT DEFAULT 'regular'", conn=conn)
    ensure_column("matches", "playoff_round", "playoff_round INTEGER", conn=conn)
    ensure_column("matches", "bracket_pos", "bracket_pos INTEGER", conn=conn)
    ensure_column("matches", "is_forfeit", "is_forfeit BOOLEAN DEFAULT 0", conn=conn)
    ensure_column("matches", "bracket_label", "bracket_label TEXT", conn=conn)
    ensure_column("match_maps", "is_forfeit", "is_forfeit INTEGER DEFAULT 0", conn=conn)
    
    try:
        c.execute("INSERT OR IGNORE INTO seasons (id, name, is_active) VALUES (22, 'Season 22', 0)")
        c.execute("INSERT OR IGNORE INTO seasons (id, name, is_active) VALUES (23, 'Season 23', 1)")
    except Exception:
        pass
    try:
        c.execute("INSERT OR IGNORE INTO team_history (team_id, season_id, group_name) SELECT id, 23, group_name FROM teams")
    except Exception:
        pass
    
    if should_close:
        conn.commit()
        conn.close()

def init_match_stats_map_table(conn=None):
    should_close = False
    if conn is None:
        conn = get_conn()
        should_close = True
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS match_stats_map (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER NOT NULL,
            map_index INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            player_id INTEGER,
            is_sub INTEGER DEFAULT 0,
            subbed_for_id INTEGER,
            agent TEXT,
            acs INTEGER,
            kills INTEGER,
            deaths INTEGER,
            assists INTEGER
        )
        """
    )
    if should_close:
        conn.commit()
        conn.close()

def import_sqlite_db(upload_bytes):
    import tempfile
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    try:
        tmp.write(upload_bytes)
        tmp.flush()
        src = sqlite3.connect(tmp.name)
        tgt = get_conn()
        tables = [
            "teams","players","matches","match_maps","match_stats_map","match_stats","agents","seasons","team_history"
        ]
        summary = {}
        for t in tables:
            try:
                df = pd.read_sql(f"SELECT * FROM {t}", src)
            except Exception:
                continue
            if df.empty:
                continue
            cols = [r[1] for r in tgt.execute(f"PRAGMA table_info({t})").fetchall()]
            use = [c for c in df.columns if c in cols]
            if not use:
                continue
            q = f"INSERT OR REPLACE INTO {t} (" + ",".join(use) + ") VALUES (" + ",".join(["?"]*len(use)) + ")"
            vals = df[use].values.tolist()
            tgt.executemany(q, vals)
            summary[t] = len(vals)
        tgt.commit()
        src.close()
        tgt.close()
        return summary
    finally:
        tmp.close()
        if os.path.exists(tmp.name):
            try:
                os.remove(tmp.name)
            except Exception:
                pass

def export_db_bytes():
    p = os.path.abspath(DB_PATH)
    try:
        if os.path.exists(p):
            with open(p, 'rb') as f:
                return f.read()
    except Exception:
        return None
    return None

def reset_db():
    conn = get_conn()
    c = conn.cursor()
    for t in [
        "admins","match_stats_map","match_stats","match_maps","matches","players","teams","agents","team_history","seasons"
    ]:
        try:
            c.execute(f"DROP TABLE IF EXISTS {t}")
        except Exception:
            pass
    conn.commit()
    conn.close()
    ensure_base_schema()
    init_admin_table()
    init_match_stats_map_table()
    ensure_upgrade_schema()
