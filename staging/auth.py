import time
import streamlit as st
import secrets
import hashlib
import hmac
from .db import get_conn
from .utils import get_visitor_ip, get_secret

def hash_password(password, salt=None):
    if salt is None:
        salt = secrets.token_bytes(16)
    hashed = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 200000)
    return salt, hashed

def verify_password(password, salt, stored_hash):
    calc = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 200000)
    return hmac.compare_digest(calc, stored_hash)

def admin_exists():
    conn = get_conn()
    cur = conn.execute("SELECT COUNT(*) FROM admins WHERE is_active=1")
    count = cur.fetchone()[0]
    conn.close()
    return count > 0

def create_admin(username, password):
    salt, ph = hash_password(password)
    conn = get_conn()
    role = get_secret("ADMIN_SEED_ROLE", "admin") if not admin_exists() else "admin"
    conn.execute("INSERT INTO admins (username, password_hash, salt, is_active, role) VALUES (?, ?, ?, 1, ?)", (username, ph, salt, role))
    conn.commit()
    conn.close()

def create_admin_with_role(username, password, role):
    salt, ph = hash_password(password)
    conn = get_conn()
    conn.execute("INSERT INTO admins (username, password_hash, salt, is_active, role) VALUES (?, ?, ?, 1, ?)", (username, ph, salt, role))
    conn.commit()
    conn.close()

def ensure_seed_admins(conn=None):
    su = get_secret("ADMIN_SEED_USER")
    sp = get_secret("ADMIN_SEED_PWD")
    sr = get_secret("ADMIN_SEED_ROLE", "admin")
    
    should_close = False
    if conn is None:
        conn = get_conn()
        should_close = True
    
    c = conn.cursor()
    if su and sp:
        row = c.execute("SELECT id, role FROM admins WHERE username=?", (su,)).fetchone()
        if not row:
            salt, ph = hash_password(sp)
            c.execute(
                "INSERT INTO admins (username, password_hash, salt, is_active, role) VALUES (?, ?, ?, 1, ?)",
                (su, ph, salt, sr)
            )
        else:
            # Always update password and role to match secrets.toml
            salt, ph = hash_password(sp)
            c.execute("UPDATE admins SET role=?, password_hash=?, salt=? WHERE id=?", (sr, ph, salt, int(row[0])))
    su2 = get_secret("ADMIN2_USER")
    sp2 = get_secret("ADMIN2_PWD")
    sr2 = get_secret("ADMIN2_ROLE", "admin")
    if su2 and sp2:
        row2 = c.execute("SELECT id FROM admins WHERE username=?", (su2,)).fetchone()
        if not row2:
            salt2, ph2 = hash_password(sp2)
            c.execute(
                "INSERT INTO admins (username, password_hash, salt, is_active, role) VALUES (?, ?, ?, 1, ?)",
                (su2, ph2, salt2, sr2)
            )
    
    if should_close:
        conn.commit()
        conn.close()

def authenticate(username, password):
    conn = get_conn()
    row = conn.execute("SELECT username, password_hash, salt, role FROM admins WHERE username=? AND is_active=1", (username,)).fetchone()
    conn.close()
    if not row:
        return None
    u, ph, salt, role = row
    if verify_password(password, salt, ph):
        return {"username": u, "role": role}
    return None

def track_user_activity():
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx
        ctx = get_script_run_ctx()
        if not ctx:
            return
        session_id = ctx.session_id
        
        # Throttling: Only update if > 5 minutes have passed
        current_ts = time.time()
        last_ts = st.session_state.get('last_activity_ts', 0)
        
        if current_ts - last_ts < 300:
            return
            
        st.session_state['last_activity_ts'] = current_ts
        
        username = st.session_state.get('username')
        is_admin = st.session_state.get('is_admin', False)
        app_mode = st.session_state.get('app_mode', 'portal')
        
        role = 'visitor'
        if is_admin:
            role = st.session_state.get('role', 'admin')
        elif app_mode == 'admin':
            role = 'visitor' # Attempting to login
            
        ip_address = get_visitor_ip()
        conn = get_conn()
        
        # Always update current session
        conn.execute(
            "INSERT OR REPLACE INTO session_activity (session_id, username, role, last_activity, ip_address) VALUES (?, ?, ?, ?, ?)",
            (session_id, username, role, time.time(), ip_address)
        )
        # Cleanup old sessions (older than 30 minutes)
        conn.execute("DELETE FROM session_activity WHERE last_activity < ?", (time.time() - 1800,))
        conn.commit()
        conn.close()
    except Exception:
        pass

def get_active_user_count():
    conn = get_conn()
    # Count distinct IPs active in last 5 minutes
    res = conn.execute("SELECT COUNT(DISTINCT ip_address) FROM session_activity WHERE last_activity > ?", (time.time() - 300,)).fetchone()
    conn.close()
    return res[0] if res else 0

def get_active_admin_session():
    conn = get_conn()
    # Check for active admin/dev sessions in last 300 seconds (5 mins)
    curr_ip = get_visitor_ip()
    
    # Get all active admin sessions
    res = conn.execute(
        "SELECT username, role, ip_address FROM session_activity WHERE (role='admin' OR role='dev') AND last_activity > ?", 
        (time.time() - 300,)
    ).fetchall()
    conn.close()
    
    # Filter out current IP manually to handle potential logic issues
    for row in res:
        if row[2] != curr_ip:
            return row # Return the first session that isn't us
            
    return None
