import streamlit as st
import pandas as pd
import re
from datetime import datetime

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(page_title="Outils Prynvision", layout="wide")

# --- STYLE PERSONNALISÉ (CSS) ---
st.markdown("""
    <style>
    /* Style des onglets */
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding: 10px 20px;
        border-radius: 4px 4px 0px 0px;
    }
    .stTabs [aria-selected="true"] {
        color: #1F6FEB !important;
        border-bottom-color: #1F6FEB !important;
        font-weight: bold;
    }
    /* Style du bloc KPI (Total Go) */
    .kpi-container {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        border-left: 5px solid #1f6feb;
        margin: 10px 0 25px 0;
    }
    .kpi-title { color: #555; font-size: 14px; font-weight: 600; text-transform: uppercase; }
    .kpi-value { font-size: 28px; font-weight: bold; color: #1f6feb; }
    
    /* Bandeau de succès */
    .status-box {
        padding: 15px;
        border-radius: 5px;
        background-color: #d4edda;
        color: #155724;
        border: 1px solid #c3e6cb;
        font-weight: bold;
        margin-bottom: 20px;
    }
    </style>
    """, unsafe_allow_html=True)

# --- FONCTION DE NETTOYAGE DES COLONNES ---
def clean_columns(df):
    if df is not None:
        df.columns = [str(c).strip() for c in df.columns]
    return df

# --- LOGIQUE TECHNIQUE : RAPPORT D'EXTRACTION ---
def extraire_donnees_ext(file_content):
    contenu = file_content.decode('latin-1', errors='ignore')
    
    # Capture du début de l'extraction (04)
    motif_debut = r"(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}) \(04\) .*? Rapatriement de fichier (.*?) depuis"
    debuts = {}
    for match in re.finditer(motif_debut, contenu):
        ts_str, nom_fichier = match.groups()
        debuts[nom_fichier.strip()] = datetime.strptime(ts_str, "%d/%m/%Y %H:%M:%S")

    # Capture de la fin de l'extraction (05)
    motif_fin = r"(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}) \(05\) (.*?) (\S+) Téléchargement terminé \((.*?)\) - ([\d\.]+) Mo"
    extractions = []
    for match in re.finditer(motif_fin, contenu):
        ts_str, site_brut, ident, nom_fichier, taille = match.groups()
        fin_ts = datetime.strptime(ts_str, "%d/%m/%Y %H:%M:%S")
        
        # Nettoyage nom du site (Code - Nom)
        site_clean = site_brut.strip()
        reg_match = re.search(r"(\d{6}\s*-\s*.*)", site_clean)
        nom_final = reg_match.group(1).strip() if reg_match else site_clean
        
        extractions.append({
            "Site": nom_final,
            "Traite_par": ident,
            "Taille_Mo": float(taille),
            "End": fin_ts
        })
    
    if not extractions: return None
    
    df = pd.DataFrame(extractions)
    # Agrégation par site et technicien
    res = []
    for (site, ident), gp in df.groupby(["Site", "Traite_par"]):
        res.append({
            "Site (Code - Nom)": site, 
            "Traité par": ident, 
            "Date": gp['End'].max().strftime("%d/%m/%Y"),
            "Nb d'Extractions": int(len(gp)), 
            "Taille (Mo)": round(gp['Taille_Mo'].sum(), 2)
        })
    return pd.DataFrame(res)

# --- LOGIQUE TECHNIQUE : ANALYSE V10 ---
def analyser_v10_logic(df_v10, df_plume):
    df_v10 = clean_columns(df_v10)
    df_plume = clean_columns(df_plume)
    
    # Identification dynamique des colonnes de date
    date_col = [c for c in df_v10.columns if 'Date' in c and 'cr' in c][0]
    time_col = [c for c in df_v10.columns if 'Heure' in c and 'cr' in c][0]
    
    df_v10['dt'] = pd.to_datetime(df_v10[date_col].astype(str) + ' ' + df_v10[time_col].astype(str), dayfirst=True, errors='coerce')
    df_v10 = df_v10.dropna(subset=['dt']).sort_values('dt')
    
    states, maintenant = {}, datetime.now()
    inc_pat = r'(INC\d+)'
    
    for _, row in df_v10.iterrows():
        site = str(row['Produit'])
        if site == 'nan': continue
        comm = str(row.get('Commentaire', ''))
        ack = str(row.get("Heure d'acquittement", ''))
        text = f"{comm} {ack}"
        
        l_m_en = bool(re.search(r"Mettre en maintenance", text, re.IGNORECASE)) or bool(re.search(inc_pat, text, re.IGNORECASE))
        l_m_ex = bool(re.search(r"Sortir de maintenance", text, re.IGNORECASE))
        l_t_en = bool(re.search(r"Mettre en travaux|En Travaux", text, re.IGNORECASE))
        l_t_ex = bool(re.search(r"Sortir de travaux", text, re.IGNORECASE))
        
        if site not in states:
            states[site] = {'maint': False, 'travaux': False, 'inc': None, 'reason': '', 'date_trav': None}
        
        # Exclusivité : Le dernier état prend le dessus
        if l_m_en:
            states[site].update({'maint': True, 'travaux': False, 'date_trav': None})
            found = re.search(inc_pat, text, re.IGNORECASE)
            if found: states[site]['inc'] = found.group(1).upper()
        if l_t_en:
            states[site].update({'travaux': True, 'maint': False, 'inc': None, 'reason': ack if "travaux" in ack.lower() else comm})
            if states[site]['date_trav'] is None: states[site]['date_trav'] = row['dt']
        
        if l_m_ex: states[site]['maint'] = False
        if l_t_ex: states[site].update({'travaux': False, 'date_trav': None})
            
    anomalies, travaux_list = [], []
    # Croisement avec Plume pour la maintenance
    if df_plume is not None:
        m_list = [{'Site': s, 'INC_V10': v['inc']} for s, v in states.items() if v['maint'] and v['inc']]
        if m_list:
            c_inc = 'Numéro' if 'Numéro' in df_plume.columns else df_plume.columns[0]
            date_resol_col = [c for c in df_plume.columns if 'Ouvert' in c or 'jour' in c][0] 
            merged = pd.merge(pd.DataFrame(m_list), df_plume, left_on='INC_V10', right_on=c_inc, how='inner')
            anom_df = merged[merged['État'].isin(['Résolu', 'Fermé'])].copy()
            
            for _, r in anom_df.iterrows():
                jours_clos = (maintenant - pd.to_datetime(r[date_resol_col])).days
                anomalies.append({
                    "Code et Nom du Site": r['Site'], "N° INC": r['INC_V10'], "Statut Plume": r['État'],
                    "Affecté à": r.get('Affecté à', 'N/A'), "_alerte": jours_clos >= 10, "_jours": jours_clos
                })
    
    # Liste des sites en travaux
    for s, v in states.items():
        if v['travaux'] and v['date_trav']:
            diff = (maintenant - v['date_trav']).days
            travaux_list.append({
                "Code et Nom du Site": s, "Mise en Travaux": v['date_trav'].strftime('%d/%m/%Y'), 
                "Depuis (Jours)": int(diff), "Raison (V10)": v['reason'], "_jours_t": int(diff)
            })
            
    return pd.DataFrame(anomalies), pd.DataFrame(travaux_list)

# --- INTERFACE UTILISATEUR ---
st.title("🛡️ Outils Prynvision")
tab_v10, tab_ext = st.tabs(["📊 Analyse V10 / Plume", "📹 Rapport d'Extraction"])

# --- ONGLET 1 : V10 ---
with tab_v10:
    st.header("Analyse Maintenance & Travaux")
    col1, col2 = st.columns(2)
    f_v10 = col1.file_uploader("1. Historique V10 (CSV)", type="csv")
    f_plume = col2.file_uploader("2. Historique Plume (Excel/CSV)", type=["csv", "xlsx"])
    
    if st.button("LANCER L'ANALYSE V10", type="primary"):
        if f_v10:
            try:
                df_v10_raw = pd.read_csv(f_v10, sep=';', encoding='latin-1')
                df_p_raw = None
                if f_plume:
                    df_p_raw = pd.read_excel(f_plume) if f_plume.name.endswith('xlsx') else pd.read_csv(f_plume)
                
                df_a, df_t = analyser_v10_logic(df_v10_raw, df_p_raw)
                st.session_state['res_a'], st.session_state['res_t'] = df_a, df_t
                st.session_state['msg_v10'] = f"Analyse terminée : {len(df_a)} anomalies de maintenance, {len(df_t)} sites en travaux."
            except Exception as e:
                st.error(f"Erreur : {e}")
        else:
            st.error("Le fichier V10 est obligatoire.")

    if 'msg_v10' in st.session_state:
        st.markdown(f'<div class="status-box">{st.session_state["msg_v10"]}</div>', unsafe_allow_html=True)
        
        search = st.text_input("🔍 Filtrer les résultats...")
        t1, t2 = st.tabs(["🔧 Maintenance", "🏗️ Travaux"])
        
        with t1:
            df = st.session_state['res_a'].copy()
            if search: df = df[df.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)]
            if not df.empty:
                st.dataframe(df.style.apply(lambda r: ['color: red' if r['_alerte'] else '' for _ in r], axis=1), 
                             use_container_width=True, hide_index=True, column_order=("Code et Nom du Site", "N° INC", "Statut Plume", "Affecté à"))
                st.download_button("📥 Export Maintenance", df.drop(columns=['_alerte', '_jours']).to_csv(index=False, sep=';').encode('utf-8-sig'), "maint.csv")

        with t2:
            df = st.session_state['res_t'].copy()
            if search: df = df[df.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)]
            if not df.empty:
                st.dataframe(df.drop(columns=['_jours_t']), use_container_width=True, hide_index=True)
                st.download_button("📥 Export Travaux", df.drop(columns=['_jours_t']).to_csv(index=False, sep=';').encode('utf-8-sig'), "trav.csv")

# --- ONGLET 2 : EXTRACTION ---
with tab_ext:
    st.header("Rapport d'Extraction")
    f_ext = st.file_uploader("Importer le fichier log Rapatriement (.txt)", type="txt")
    
    if f_ext:
        df_ext = extraire_donnees_ext(f_ext.getvalue())
        if df_ext is not None:
            # --- CALCUL DU VOLUME TOTAL ---
            total_mo = df_ext["Taille (Mo)"].sum()
            total_go = total_mo / 1024
            
            # --- AFFICHAGE DU BLOC KPI ---
            st.markdown(f"""
                <div class="kpi-container">
                    <div class="kpi-title">Volume Total Extrait</div>
                    <div class="kpi-value">{total_go:.2f} Go</div>
                    <div style="color: #666; font-size: 12px; margin-top:5px;">(Basé sur {total_mo:,.0f} Mo)</div>
                </div>
                """, unsafe_allow_html=True)

            search_ext = st.text_input("🔍 Rechercher un site ou un technicien...")
            df_f = df_ext.copy()
            if search_ext:
                df_f = df_f[df_f.apply(lambda r: r.astype(str).str.contains(search_ext, case=False).any(), axis=1)]
            
            st.dataframe(df_f.sort_values("Taille (Mo)", ascending=False), use_container_width=True, hide_index=True)
            st.download_button("📥 Télécharger Rapport CSV", df_f.to_csv(index=False, sep=';').encode('utf-8-sig'), "Extractions.csv")
        else:
            st.warning("Aucune donnée trouvée dans le fichier texte.")
