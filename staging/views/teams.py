import streamlit as st
import pandas as pd
import html
from ..data_access import get_teams_list_full, get_all_players, get_conn
from ..utils import get_base64_image, is_safe_path

def show_teams():
    st.markdown('<h1 class="main-header">TEAMS</h1>', unsafe_allow_html=True)
    
    teams = get_teams_list_full()
    all_players = get_all_players()
    
    # Pre-group rosters for efficiency
    rosters_by_team = {}
    if not all_players.empty:
        all_players = all_players.copy()
        # Create display name for the table
        all_players['display_name'] = all_players.apply(lambda r: f"{r['name']} ({r['riot_id']})" if r['riot_id'] and str(r['riot_id']).strip() else r['name'], axis=1)
        for tid, group in all_players.groupby('default_team_id'):
            # Keep all columns for admin management, but we'll filter for display
            rosters_by_team[int(tid)] = group
    
    groups = ["All"] + sorted(teams['group_name'].dropna().unique().tolist())
    
    with st.container():
        st.markdown('<div class="custom-card">', unsafe_allow_html=True)
        c1, c2 = st.columns([1, 2])
        with c1:
            g = st.selectbox("Filter by Group", groups)
        with c2:
            search_q = st.text_input("Search Team", placeholder="Search by team name or tag...")
        st.markdown('</div>', unsafe_allow_html=True)
        
    st.markdown("<br>", unsafe_allow_html=True)
    
    show = teams if g == "All" else teams[teams['group_name'] == g]
    if search_q:
        s = search_q.lower()
        show = show[
            show['name'].str.lower().fillna("").str.contains(s) | 
            show['tag'].str.lower().fillna("").str.contains(s)
        ]

    if show.empty:
        st.info("No teams found matching your criteria.")
    
    for row in show.itertuples():
        with st.container():
            # Team Header Card
            b64 = get_base64_image(row.logo_path)
            logo_img_html = f"<img src='data:image/png;base64,{b64}' width='60'/>" if b64 else "<div style='width:60px;height:60px;background:rgba(255,255,255,0.05);border-radius:8px;display:flex;align-items:center;justify-content:center;color:var(--text-dim);'>?</div>"
            
            st.markdown(f"""<div class="custom-card" style="margin-bottom: 10px;">
<div style="display: flex; align-items: center; gap: 20px;">
<div style="flex-shrink: 0;">
{logo_img_html}
</div>
<div>
<h3 style="margin: 0; color: var(--primary-blue); font-family: 'Orbitron';">{html.escape(str(row.name))} <span style="color: var(--text-dim); font-size: 0.9rem;">[{html.escape(str(row.tag or ''))}]</span></h3>
<div style="color: var(--text-dim); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 1px;">Group {html.escape(str(row.group_name))}</div>
</div>
</div>
</div>""", unsafe_allow_html=True)
            
            with st.expander("Manage Roster & Details"):
                roster = rosters_by_team.get(int(row.id), pd.DataFrame())
                
                if roster.empty:
                    st.info("No players yet")
                else:
                    # Generate HTML Table for Roster
                    r_html = '<table class="valorant-table" style="margin-top:0;">'
                    r_html += '<thead><tr><th>Name</th><th>Rank</th></tr></thead><tbody>'
                    
                    for _, r_row in roster.iterrows():
                        r_html += f'<tr><td>{html.escape(str(r_row["display_name"]))}</td><td>{html.escape(str(r_row["rank"] or ""))}</td></tr>'
                    
                    r_html += '</tbody></table>'
                    st.markdown(r_html, unsafe_allow_html=True)
                
                if st.session_state.get('is_admin'):
                    st.markdown("---")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.caption("Edit Team Details")
                        with st.form(f"edit_team_{row.id}"):
                            new_name = st.text_input("Name", value=row.name)
                            new_tag = st.text_input("Tag", value=row.tag or "")
                            new_group = st.text_input("Group", value=row.group_name or "")
                            new_logo = st.text_input("Logo Path", value=row.logo_path or "")
                            if st.form_submit_button("Update Team"):
                                # Use is_safe_path for validation
                                if new_logo and not is_safe_path(new_logo):
                                    st.error("Invalid logo path. Path traversal or absolute paths are not allowed.")
                                else:
                                    conn_u = get_conn()
                                    try:
                                        conn_u.execute("UPDATE teams SET name=?, tag=?, group_name=?, logo_path=? WHERE id=?", 
                                                      (new_name, new_tag, new_group, new_logo, row.id))
                                        conn_u.commit()
                                        st.success("Updated!")
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Error: {e}")
                                    finally:
                                        conn_u.close()
