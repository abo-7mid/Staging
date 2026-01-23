import streamlit as st
import os
import sys
import time
import hmac
import html

# Add project root to path to allow 'staging' package imports
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from staging.config import GLOBAL_STYLES, FONTS_HTML
from staging.db import (
    get_conn, ensure_base_schema, ensure_upgrade_schema, 
    init_admin_table, init_session_activity_table, init_match_stats_map_table
)
from staging.auth import (
    authenticate, track_user_activity, ensure_seed_admins, 
    get_active_admin_session, admin_exists
)
from staging.utils import get_visitor_ip, get_secret

# View Imports
from staging.views.home import show_home
from staging.views.standings import show_standings
from staging.views.matches import show_matches
from staging.views.stats import show_stats
from staging.views.teams import show_teams
from staging.views.admin import show_admin_panel
from staging.views.predictor import show_predictor
from staging.views.summary import show_summary
from staging.views.directory import show_directory
from staging.views.substitutions import show_substitutions
from staging.views.profile import show_profile

# Page Config
st.set_page_config(
    page_title="VALORANT S23 PORTAL",
    page_icon="🏆",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Initialize DB
ensure_base_schema()
init_admin_table()
init_session_activity_table()
init_match_stats_map_table()
ensure_upgrade_schema()
ensure_seed_admins()

# Inject CSS
st.markdown(FONTS_HTML, unsafe_allow_html=True)
st.markdown(GLOBAL_STYLES, unsafe_allow_html=True)

# App Mode Logic
if 'login_attempts' not in st.session_state:
    st.session_state['login_attempts'] = 0
if 'last_login_attempt' not in st.session_state:
    st.session_state['last_login_attempt'] = 0
if 'app_mode' not in st.session_state:
    st.session_state['app_mode'] = 'portal'
if 'page' not in st.session_state:
    st.session_state['page'] = 'Overview & Standings'
if 'is_admin' not in st.session_state:
    st.session_state['is_admin'] = False

# Track Activity
track_user_activity()

# Use a placeholder to clear the screen during transitions
main_container = st.empty()

if st.session_state['app_mode'] == 'portal':
    with main_container.container():
        st.markdown("""<div class="portal-container">
<h1 class="portal-header">VALORANT S23 PORTAL</h1>
<p class="portal-subtitle">System Status & Access Terminal</p>
<div class="status-grid">
<div class="status-indicator status-online">● VISITOR ACCESS: LIVE</div>
<div class="status-indicator status-offline">● TEAM PANEL: STAGING</div>
<div class="status-indicator status-online">● ADMIN CORE: SECURE</div>
</div>
<div class="portal-options">""", unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown('<div class="portal-card-wrapper"><div class="portal-card-content"><h3>VISITOR</h3><p style="color: var(--text-dim); font-size: 0.9rem;">Browse tournament statistics, match history, and player standings.</p></div><div class="portal-card-footer">', unsafe_allow_html=True)
            if st.button("ENTER PORTAL", key="enter_visitor", use_container_width=True, type="primary"):
                st.session_state['app_mode'] = 'visitor'
                st.rerun()
            st.markdown('</div></div>', unsafe_allow_html=True)
            
        with col2:
            st.markdown('<div class="portal-card-wrapper disabled"><div class="portal-card-content"><h3>TEAM LEADER</h3><p style="color: var(--text-dim); font-size: 0.9rem;">Manage your team roster, submit scores, and track performance.</p></div><div class="portal-card-footer">', unsafe_allow_html=True)
            st.button("LOCKED", key="enter_team", use_container_width=True, disabled=True)
            st.markdown('</div></div>', unsafe_allow_html=True)
            
        with col3:
            st.markdown('<div class="portal-card-wrapper"><div class="portal-card-content"><h3>ADMIN</h3><p style="color: var(--text-dim); font-size: 0.9rem;">Full system administration, data management, and tournament control.</p></div><div class="portal-card-footer">', unsafe_allow_html=True)
            if st.button("ADMIN LOGIN", key="enter_admin", use_container_width=True):
                st.session_state['app_mode'] = 'admin'
                st.rerun()
            st.markdown('</div></div>', unsafe_allow_html=True)
            
        st.markdown('</div></div>', unsafe_allow_html=True)
    st.stop()

# Admin Login Screen Logic
if st.session_state['app_mode'] == 'admin' and not st.session_state.get('is_admin'):
    # Show a simplified nav for login screen
    st.markdown('<div class="nav-wrapper"><div class="nav-logo" style="margin-left: auto; margin-right: auto;">VALORANT S23 • ADMIN PORTAL</div></div>', unsafe_allow_html=True)
    
    st.markdown('<div style="margin-top: 40px;"></div>', unsafe_allow_html=True)
    st.markdown('<h1 class="main-header">ADMIN ACCESS</h1>', unsafe_allow_html=True)
    
    col1, col2 = st.columns([1, 1])
    with col1:
        st.info("Please enter your administrator credentials to proceed.")
        
        # Check for active admin sessions first
        active_admin = get_active_admin_session()
        
        if active_admin:
            st.error(f"Access Denied: Someone is actively working on the admin panel.")
            st.warning(f"Active User: {active_admin[0]} ({active_admin[1]})")
            
            with st.expander("🔍 UNLOCK ACCESS (Click here if you are stuck)"):
                curr_ip = get_visitor_ip()
                st.write(f"**Your Current ID:** `{curr_ip}`")
                st.write(f"**Blocking ID:** `{active_admin[2]}`")
                st.write("---")
                st.write("### Option 1: Unlock your specific ID")
                if st.button("🔓 UNLOCK MY ID", use_container_width=True):
                    try:
                        conn = get_conn()
                        conn.execute("DELETE FROM session_activity WHERE ip_address = ? AND (role = 'admin' OR role = 'dev')", (curr_ip,))
                        conn.commit()
                        conn.close()
                        st.success("Your ID has been cleared. Try logging in below.")
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {str(e)}")
                
                st.write("---")
                st.write("### Option 2: Force Unlock Everything (Requires Special Token)")
                force_token = st.text_input("Force Unlock Token", type="password", key="force_token_input")
                if st.button("☢️ FORCE UNLOCK EVERYTHING", use_container_width=True):
                    env_tok = get_secret("FORCE_UNLOCK_TOKEN", None)
                    # If FORCE_UNLOCK_TOKEN is not set, fallback to ADMIN_LOGIN_TOKEN as a safety measure
                    if env_tok is None:
                        env_tok = get_secret("ADMIN_LOGIN_TOKEN", None)
                        
                    if env_tok and hmac.compare_digest(force_token or "", env_tok):
                        try:
                            conn = get_conn()
                            conn.execute("DELETE FROM session_activity WHERE role = 'admin' OR role = 'dev'")
                            conn.commit()
                            conn.close()
                            st.success("ALL admin sessions cleared. You can now login.")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {str(e)}")
                    else:
                        st.error("Invalid Force Unlock Token.")
            st.markdown("---")
        
        # Simple rate limiting
        if st.session_state['login_attempts'] >= 5:
            time_since_last = time.time() - st.session_state['last_login_attempt']
            if time_since_last < 300: # 5 minute lockout
                st.error(f"Too many failed attempts. Please wait {int(300 - time_since_last)} seconds.")
                if st.button("← BACK TO SELECTION"):
                    st.session_state['app_mode'] = 'portal'
                    st.rerun()
                st.stop()
            else:
                st.session_state['login_attempts'] = 0

        with st.form("admin_login_main"):
            u = st.text_input("Username")
            p = st.text_input("Password", type="password")
            
            # Check if token is required
            env_tok = get_secret("ADMIN_LOGIN_TOKEN", None)
            tok = ""
            if env_tok:
                tok = st.text_input("Admin Token", type="password")
            else:
                 # Fallback if secret not loaded but we want to enforce it or just show it
                 st.warning("Admin Token not configured in secrets.")
            
            if st.form_submit_button("LOGIN TO ADMIN PANEL", use_container_width=True):
                # Check for active admin sessions first
                active_admin = get_active_admin_session()
                if active_admin:
                    st.error(f"Access Denied: Someone is actively working on the admin panel.")
                    st.warning(f"Active User: {active_admin[0]} ({active_admin[1]})")
                else:
                    auth_res = authenticate(u, p)
                    
                    token_valid = False
                    if env_tok:
                         token_valid = hmac.compare_digest(tok or "", env_tok)
                    else:
                        # If no token configured, allow login? Or fail? 
                        # User wants token verification back.
                        # If env_tok is missing, we should probably fail safe or warn.
                        # Assuming if no token set in secrets, we skip check (but user said they want it).
                        # I've restored it in secrets.toml, so it should be there.
                        # If it's not there, default to False to be safe.
                        token_valid = False
                        st.error("System Error: Admin Token configuration missing.")

                    if auth_res and token_valid:
                        st.session_state['is_admin'] = True
                        st.session_state['username'] = auth_res['username']
                        st.session_state['role'] = auth_res['role']
                        st.session_state['page'] = "⚙️ ADMIN PANEL"
                        st.session_state['login_attempts'] = 0
                        # Update activity immediately with new role
                        track_user_activity()
                        st.success("Access Granted")
                        st.rerun()
                    else:
                        st.session_state['last_login_attempt'] = time.time()
                        st.session_state['login_attempts'] += 1
                        st.error(f"Invalid credentials (Attempt {st.session_state['login_attempts']}/5)")
        if st.button("← BACK TO SELECTION"):
            st.session_state['app_mode'] = 'portal'
            st.rerun()
    st.stop()

# Main App Navigation & Layout (Visitor/Admin)

pages = [
    "🏆 STANDINGS",
    "⚔️ MATCHES",
    "📈 LEADERBOARD",
    "🛡️ TEAMS",
    "🔮 PREDICTOR", 
    "📊 SUMMARY",   
    "📇 DIRECTORY", 
    "🔄 SUBSTITUTIONS", 
    "👤 PROFILE",    
]
if st.session_state['is_admin']:
    if "👑 PLAYOFFS" not in pages:
        pages.insert(pages.index("⚙️ ADMIN PANEL") if "⚙️ ADMIN PANEL" in pages else len(pages), "👑 PLAYOFFS")
    if "⚙️ ADMIN PANEL" not in pages:
        pages.append("⚙️ ADMIN PANEL")

# Top Navigation Bar
st.markdown('<div class="nav-wrapper"><div class="nav-logo">VALORANT S23 • PORTAL</div></div>', unsafe_allow_html=True)

# Navigation Layout
st.markdown('<div class="sub-nav-wrapper">', unsafe_allow_html=True)

# Define columns based on whether admin is logged in (to add logout button)
nav_cols_spec = [0.6] + [1] * len(pages)
if st.session_state['is_admin']:
    nav_cols_spec.append(0.8) # Column for logout

cols = st.columns(nav_cols_spec)

with cols[0]:
    st.markdown('<div class="exit-btn">', unsafe_allow_html=True)
    if st.button("🏠 EXIT", key="exit_portal", use_container_width=True):
        st.session_state['app_mode'] = 'portal'
        st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)
    
for i, p in enumerate(pages):
    with cols[i+1]:
        is_active = st.session_state['page'] == p
        st.markdown(f'<div class="{"active-nav" if is_active else ""}">', unsafe_allow_html=True)
        if st.button(p, key=f"nav_{p}", use_container_width=True, 
                     type="primary" if is_active else "secondary"):
            st.session_state['page'] = p
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)
        
        if is_active:
            st.markdown('<div style="height: 3px; background: var(--primary-red); margin-top: -8px; box-shadow: 0 0 10px var(--primary-red); border-radius: 2px;"></div>', unsafe_allow_html=True)

# Add Logout button if admin
if st.session_state['is_admin']:
    with cols[-1]:
        st.markdown('<div class="exit-btn">', unsafe_allow_html=True)
        if st.button(f"🚪 LOGOUT ({st.session_state['username']})", key="logout_btn", use_container_width=True):
            st.session_state['is_admin'] = False
            st.session_state['username'] = None
            st.session_state['app_mode'] = 'portal'
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# Render Page Content
# (Removed unconditional reset to allow navigation to work)

if st.session_state['page'] == "🏆 STANDINGS":
    show_standings()
elif st.session_state['page'] == "⚔️ MATCHES":
    show_matches()
elif st.session_state['page'] == "📈 LEADERBOARD":
    show_stats()
elif st.session_state['page'] == "🛡️ TEAMS":
    show_teams()
elif st.session_state['page'] == "⚙️ ADMIN PANEL":
    show_admin_panel()
elif st.session_state['page'] == "🔮 PREDICTOR":
    show_predictor()
elif st.session_state['page'] == "📊 SUMMARY":
    show_summary()
elif st.session_state['page'] == "📇 DIRECTORY":
    show_directory()
elif st.session_state['page'] == "🔄 SUBSTITUTIONS":
    show_substitutions()
elif st.session_state['page'] == "👤 PROFILE":
    show_profile()
elif st.session_state['page'] == "👑 PLAYOFFS":
    st.title("PLAYOFFS BRACKET")
    st.info("Coming Soon")
