import os
import sys

# Path management
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(CURRENT_DIR)
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

# Valorant Map Catalog
MAPS_CATALOG = ["Abyss", "Ascent", "Bind", "Breeze", "Fracture", "Haven", "Icebox", "Lotus", "Pearl", "Split", "Sunset", "Corrode"]

# CSS Styles
GLOBAL_STYLES = """
<style>
/* Hide Streamlit elements */
#MainMenu {visibility: hidden;}
header {visibility: hidden;}
footer {visibility: hidden;}
.stAppDeployButton {display:none;}
[data-testid="stSidebar"] {display: none;}
[data-testid="stSidebarCollapsedControl"] {display: none;}

/* Global Styles */
:root {
    --primary-blue: #3FD1FF;
    --primary-red: #FF4655;
    --bg-dark: #0F1923;
    --card-bg: #1F2933;
    --text-main: #ECE8E1;
    --text-dim: #8B97A5;
    --nav-height: 80px;
}
.stApp {
    background-color: var(--bg-dark);
    background-image: 
        radial-gradient(circle at 20% 30%, rgba(63, 209, 255, 0.08) 0%, transparent 40%), 
        radial-gradient(circle at 80% 70%, rgba(255, 70, 85, 0.08) 0%, transparent 40%),
        linear-gradient(rgba(15, 25, 35, 0.95), rgba(15, 25, 35, 0.95)),
        repeating-linear-gradient(0deg, transparent, transparent 2px, rgba(0, 0, 0, 0.2) 2px, rgba(0, 0, 0, 0.2) 4px);
    background-size: 100% 100%, 100% 100%, 100% 100%, 100% 8px;
    color: var(--text-main);
    font-family: 'Inter', sans-serif;
    transition: opacity 0.5s ease-in-out;
}
.stApp .main .block-container {
    padding-top: var(--padding-top, 60px) !important;
}
.portal-header {
    color: var(--primary-blue);
    font-size: 3.5rem;
    text-shadow: 0 0 30px rgba(63, 209, 255, 0.6);
    margin-bottom: 0;
    text-align: center;
    font-family: 'Orbitron', sans-serif;
    letter-spacing: 2px;
}
.portal-subtitle {
    color: var(--text-dim);
    font-size: 0.9rem;
    letter-spacing: 6px;
    margin-bottom: 3rem;
    text-transform: uppercase;
    text-align: center;
    text-shadow: 0 0 10px rgba(139, 151, 165, 0.3);
}

/* Nav Logo Styling */
.nav-logo {
    font-family: 'Orbitron', sans-serif;
    font-size: 1.5rem;
    font-weight: 700;
    background: linear-gradient(to right, var(--primary-red), var(--primary-blue));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    letter-spacing: 4px;
    text-transform: uppercase;
    text-shadow: 0 0 20px rgba(255, 70, 85, 0.3);
    margin: 0;
    padding: 10px 0;
    text-align: center;
}

/* Status Grid Styling */
.status-grid {
    display: flex;
    justify-content: center;
    gap: 2rem;
    margin-bottom: 4rem;
    flex-wrap: wrap;
}
.status-indicator {
    padding: 8px 16px;
    background: rgba(31, 41, 51, 0.6);
    border: 1px solid rgba(255, 255, 255, 0.1);
    border-radius: 4px;
    font-size: 0.8rem;
    letter-spacing: 1px;
    display: flex;
    align-items: center;
    gap: 8px;
    transition: all 0.3s ease;
}
.status-indicator.status-online {
    border-color: rgba(63, 209, 255, 0.3);
    color: var(--primary-blue);
    box-shadow: 0 0 10px rgba(63, 209, 255, 0.1);
}
.status-indicator.status-offline {
    border-color: rgba(255, 70, 85, 0.3);
    color: var(--primary-red);
    box-shadow: 0 0 10px rgba(255, 70, 85, 0.1);
}

/* Portal Card Styling */
.portal-card-wrapper {
    background: linear-gradient(145deg, rgba(31, 41, 51, 0.9), rgba(20, 25, 30, 0.95));
    border: 1px solid rgba(255, 255, 255, 0.05);
    border-radius: 4px;
    height: 100%;
    min-height: 280px;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    position: relative;
    overflow: hidden;
}
.portal-card-wrapper:hover {
    transform: translateY(-5px);
    border-color: var(--primary-blue);
    box-shadow: 0 10px 30px rgba(0, 0, 0, 0.5);
}
.portal-card-wrapper.disabled {
    opacity: 0.7;
    border-color: rgba(255, 70, 85, 0.1);
}
.portal-card-wrapper::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 2px;
    background: linear-gradient(90deg, transparent, var(--primary-blue), transparent);
    transform: scaleX(0);
    transition: transform 0.5s ease;
}
.portal-card-wrapper:hover::before {
    transform: scaleX(1);
}
.portal-card-content {
    padding: 2rem;
    text-align: center;
}
.portal-card-content h3 {
    font-family: 'Orbitron', sans-serif;
    font-size: 1.5rem;
    margin-bottom: 1rem;
    letter-spacing: 2px;
    color: var(--text-main);
}
.portal-card-footer {
    padding: 1.5rem;
    background: rgba(0, 0, 0, 0.2);
    border-top: 1px solid rgba(255, 255, 255, 0.05);
}

/* Navigation Button Styling */
.stButton > button {
    background: rgba(255, 255, 255, 0.03) !important;
    border: 1px solid rgba(255, 255, 255, 0.1) !important;
    color: var(--text-dim) !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 600 !important;
    transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
    border-radius: 2px !important;
    text-transform: uppercase !important;
    letter-spacing: 1px !important;
    font-size: 0.8rem !important;
    height: 42px !important;
    position: relative;
    overflow: hidden;
}
.stButton > button:hover {
    border-color: var(--primary-blue) !important;
    color: var(--primary-blue) !important;
    background: rgba(63, 209, 255, 0.1) !important;
    box-shadow: 0 0 15px rgba(63, 209, 255, 0.2) !important;
    transform: translateY(-1px);
}
.stButton > button:active {
    transform: translateY(1px);
}
.stButton > button[kind="primary"] {
    background: var(--primary-red) !important;
    border-color: var(--primary-red) !important;
    color: white !important;
    box-shadow: 0 4px 20px rgba(255, 70, 85, 0.3) !important;
}
.stButton > button[kind="primary"]:hover {
    background: #ff5c6a !important;
    box-shadow: 0 0 30px rgba(255, 70, 85, 0.6) !important;
}
/* Active Tab Style */
.active-nav button {
    border-bottom: 2px solid var(--primary-red) !important;
    color: white !important;
    background: linear-gradient(180deg, rgba(255, 255, 255, 0.05) 0%, rgba(255, 70, 85, 0.1) 100%) !important;
    border-radius: 4px 4px 0 0 !important;
    box-shadow: 0 -5px 15px rgba(0,0,0,0.2) !important;
}

/* Valorant Table Styling */
.valorant-table {
    width: 100%;
    border-collapse: separate;
    border-spacing: 0 4px;
    margin: 20px 0;
    font-family: 'Inter', sans-serif;
}
.valorant-table th {
    text-align: left;
    padding: 12px 20px;
    background: rgba(31, 41, 51, 0.95);
    color: var(--text-dim);
    font-family: 'Orbitron', sans-serif;
    font-size: 0.8rem;
    letter-spacing: 1px;
    border-bottom: 2px solid var(--primary-blue);
    text-transform: uppercase;
}
.valorant-table td {
    padding: 15px 20px;
    background: rgba(255, 255, 255, 0.03);
    color: var(--text-main);
    border-top: 1px solid rgba(255, 255, 255, 0.05);
    border-bottom: 1px solid rgba(255, 255, 255, 0.05);
    font-size: 0.95rem;
    transition: all 0.2s ease;
}
.valorant-table tr:hover td {
    background: rgba(255, 255, 255, 0.08);
    border-color: rgba(63, 209, 255, 0.3);
    transform: scale(1.01);
}
.valorant-table td:first-child {
    border-left: 4px solid transparent;
    border-top-left-radius: 4px;
    border-bottom-left-radius: 4px;
    font-weight: bold;
    color: var(--primary-blue);
}
.valorant-table tr:hover td:first-child {
    border-left-color: var(--primary-red);
}
.valorant-table td:last-child {
    border-top-right-radius: 4px;
    border-bottom-right-radius: 4px;
    font-weight: bold;
    text-align: right;
}

/* Sub-nav wrapper adjustments */
/* Target the sibling of the nav-marker (the stHorizontalBlock) */
div[data-testid="stMarkdownContainer"]:has(#nav-marker) + div[data-testid="stHorizontalBlock"] {
    position: fixed;
    top: var(--nav-height);
    left: 0;
    right: 0;
    background: rgba(15, 25, 35, 0.95);
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    padding: 10px 0;
    z-index: 999;
    box-shadow: 0 5px 20px rgba(0,0,0,0.3);
    backdrop-filter: blur(10px);
    overflow-x: auto;
    white-space: nowrap;
    display: flex;
    flex-wrap: nowrap;
    gap: 0.5rem;
}

/* Ensure columns inside don't shrink */
div[data-testid="stMarkdownContainer"]:has(#nav-marker) + div[data-testid="stHorizontalBlock"] [data-testid="column"] {
    flex: 0 0 auto !important;
    width: auto !important;
    min-width: 140px;
}

/* Scrollbar Styling for the container */
div[data-testid="stMarkdownContainer"]:has(#nav-marker) + div[data-testid="stHorizontalBlock"]::-webkit-scrollbar {
    height: 4px;
}
div[data-testid="stMarkdownContainer"]:has(#nav-marker) + div[data-testid="stHorizontalBlock"]::-webkit-scrollbar-track {
    background: rgba(255, 255, 255, 0.05);
}
div[data-testid="stMarkdownContainer"]:has(#nav-marker) + div[data-testid="stHorizontalBlock"]::-webkit-scrollbar-thumb {
    background: var(--primary-red);
    border-radius: 2px;
}
div[data-testid="stMarkdownContainer"]:has(#nav-marker) + div[data-testid="stHorizontalBlock"]::-webkit-scrollbar-thumb:hover {
    background: var(--primary-blue);
}

.sub-nav-content {
    display: flex;
    gap: 10px;
    width: 100%;
    max-width: 1400px;
    overflow-x: auto;
    -ms-overflow-style: none;
    scrollbar-width: none;
}
.sub-nav-content::-webkit-scrollbar {
    display: none;
}

/* Custom Card for Dashboard */
.custom-card {
background: var(--card-bg);
border: 1px solid rgba(255, 255, 255, 0.05);
border-radius: 4px;
padding: 1.5rem;
height: 100%;
}

/* Dataframe Styling */
[data-testid="stDataFrame"] {
border: 1px solid rgba(255, 255, 255, 0.05) !important;
border-radius: 4px !important;
}

@keyframes fadeIn {
from { opacity: 0; transform: translateY(20px); }
to { opacity: 1; transform: translateY(0); }
}

/* Mobile Responsiveness */
@media (max-width: 1024px) {
.portal-header { font-size: 2.5rem; }
.portal-options { grid-template-columns: 1fr; gap: 1rem; }
.nav-wrapper { padding: 0 2rem; }
.sub-nav-wrapper { padding: 10px 2rem; }
}

@media (max-width: 768px) {
.portal-header { font-size: 2rem; }
.portal-subtitle { font-size: 0.7rem; letter-spacing: 2px; margin-bottom: 1.5rem; }
.status-grid { flex-direction: column; gap: 0.8rem; }
.status-indicator { min-width: 100%; }
.portal-options { grid-template-columns: 1fr; gap: 1.5rem; }
.nav-wrapper { height: 60px; padding: 0 1rem; align-items: center; }
.nav-logo { font-size: 0.9rem; letter-spacing: 2px; }
.sub-nav-wrapper { top: 60px; padding: 8px 0.5rem; overflow-x: auto; white-space: nowrap; display: block !important; -webkit-overflow-scrolling: touch; background: rgba(15, 25, 35, 0.95); }
.sub-nav-wrapper [data-testid="stHorizontalBlock"] { display: flex !important; flex-wrap: nowrap !important; width: max-content !important; gap: 12px !important; padding: 0 10px !important; }
.sub-nav-wrapper [data-testid="column"] { width: auto !important; min-width: 130px !important; flex: 0 0 auto !important; }
/* Hide the scrollbar for sub-nav */
.sub-nav-wrapper::-webkit-scrollbar { display: none; }
.sub-nav-wrapper { -ms-overflow-style: none; scrollbar-width: none; }
.main-header { font-size: 1.8rem !important; margin-bottom: 1.5rem !important; }
}
</style>
"""

FONTS_HTML = """<link href='https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700&family=Rajdhani:wght@400;600&family=Inter:wght@400;700&display=swap' rel='stylesheet'>"""

def apply_plotly_theme(fig):
    fig.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#ECE8E1', family="Inter"),
        title_font=dict(color='#3FD1FF', family="Orbitron"),
        xaxis=dict(
            gridcolor='rgba(255,255,255,0.05)',
            zerolinecolor='rgba(255,255,255,0.1)'
        ),
        yaxis=dict(
            gridcolor='rgba(255,255,255,0.05)',
            zerolinecolor='rgba(255,255,255,0.1)'
        ),
        margin=dict(l=40, r=40, t=40, b=40),
        legend=dict(
            bgcolor='rgba(0,0,0,0)',
            bordercolor='rgba(255,255,255,0.1)'
        )
    )
    return fig
