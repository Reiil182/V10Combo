import streamlit as st
import pandas as pd
import re
from datetime import datetime

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(page_title="Outils Prynvision", layout="wide")

# --- STYLE PERSONNALISÉ ---
st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre;
        border-radius: 4px 4px 0px 0px;
        padding: 10px 20px;
    }
    .stTabs [aria-selected="true"] {
        color: #1F6FEB !important;
        border-bottom-color: #1F6FEB !important;
        font-weight: bold;
    }
    div.stDownloadButton > button {
        background-color: #3498db !important;
        color: white !important;
        width: 100%;
        border: none;
        padding: 10px;
        margin-top: 10px;
    }
    .status-box {
        padding: 15px;
        border-radius: 5px;
        margin-top: 10px;
        margin-bottom: 10px;
        background-color: #d4edda;
        color: #155724;
        border: 1px solid #c3e6cb;
    }
    </style>
    """, unsafe_allow_html=True)

# --- LOGIQUE TECHNIQUE : RAPPORT D'EXTRACTION ---
def extraire_donnees_ext(file_content):
    contenu = file_content.decode('latin-1', errors='ignore')
    motif_debut = r"(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}) \(04\) .*? Rapatriement de fichier (.*?) depuis"
    debuts = {}
    for match in re.finditer(motif_debut, contenu):
        ts_str, nom_fichier = match.groups()
        debuts[nom_fichier.strip()] = datetime.strptime(ts_str, "%d/%m/%Y %H:%M:%S")

    motif_fin = r"(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}) \(05\) (.*?) (\S+) Téléchargement terminé \((.*?)\) - ([\d\.]+) Mo"
    extractions = []
    for match in re.finditer(motif_fin, contenu):
        ts_str, site_brut, ident, nom_fichier, taille = match.groups()
        fin_ts = datetime.strptime(ts_str, "%d/%m/%Y %H:%M:%S")
        site_clean = site_brut.strip()
        reg_match = re.search(r"(\d{6}\s*-\s*.*)", site_clean)
        nom_final = reg_match.group(1).strip() if reg_match else site_clean
        extractions.append({
            "Site (Code - Nom)": nom_final,
            "Transmis": fin_ts,
            "Traité part": ident,
            "Taille_Mo": float(taille),
            "Start": debuts.get(nom_fichier.strip()),
            "End": fin_ts
        })
    
    if not extractions: return None
    df = pd.DataFrame(extractions)
    resultats = []
    for (site, ident), groupe in df.groupby(["Site (Code - Nom)", "Traité part"]):
        v_start, v_end = groupe['Start'].dropna(), groupe['End'].dropna()
        if not v_start.empty and not v_end.empty:
            duree_sec = (v_end.max() - v_start.min()).total_seconds()
            m, s = divmod(int(duree_sec), 60)
            temps_str = f"{m} min {s} s" if m > 0 else f"{s} s"
        else: temps_str = "N/A"
        resultats.append({
            "Site (Code - Nom)": site, 
            "Traité par": ident, 
            "Date": groupe['End'].max().strftime("%d/%m/%Y"),
            "Nb d'Extractions": int(len(groupe)), 
            "Taille (Mo)": round(groupe['Taille_Mo'].sum(), 2),
            "Temps estimé": temps_str
        })
    return pd.DataFrame(resultats)

# --- LOGIQUE TECHNIQUE : ANALYSE V10 ---
def analyser_v10_logic(df_v10, df_plume):
    df_v10.columns = [c.strip() for c in df_v10.columns]
    date_col = [c for c in df_v10.columns if 'Date' in c and 'cr' in c][0]
    time_col = [c for c in df_v10.columns if 'Heure' in c and 'cr' in c][0]
    
    # SECURITÉ : Gestion des erreurs de date pour éviter le crash
    df_v10['dt'] = pd.to_datetime(
        df_v10[date_col].astype(str) + ' ' + df_v10[time_col].astype(str), 
        dayfirst=True, 
        errors='coerce'
    )
    df_v10 = df_v10.dropna(subset=['dt']).sort_values('dt')
    
    states, maintenant = {}, datetime.now()
    inc_pat = r'(INC\d+)'
    
    for _, row in df_v10.iterrows():
        site, comm, ack = str(row['Produit']), str(row.get('Commentaire', '')), str(row.get("Heure d'acquittement", ''))
        text = f"{comm} {ack}"
        l_m_en = bool(re.search(r"Mettre en maintenance", text, re.IGNORECASE))
        l_m_ex = bool(re.search(r"Sortir de maintenance", text, re.IGNORECASE))
        l_t_en = bool(re.search(r"Mettre en travaux|En Travaux", text, re.IGNORECASE))
        l_t_ex = bool(re.search(r"Sortir de travaux", text, re.IGNORECASE))
        
        if not any([l_m_en, l_m_ex, l_t_en, l_t_ex]) and re.search(inc_pat, text, re.IGNORECASE):
            l_m_en = True

        if site not in states:
            states[site] = {'maint': False, 'travaux': False, 'inc': None, 'reason': '', 'date_trav': None}
        
        if l_m_en:
            states[site].update({'maint': True, 'travaux': False, 'date_trav': None})
            found = re.search(inc_pat, text, re.IGNORECASE)
            if found: states[site]['inc'] = found.group(1).upper()
        if l_t_en:
            states[site].update({'travaux': True, 'maint': False, 'inc': None, 'reason': ack if "travaux" in ack.lower() else comm})
            if states[site]['date_trav'] is None: states[site]['date_trav'] = row['dt']
        if l_m_ex: states[site]['maint'] = False
        if l_t_ex: states[site].update({'travaux': False, 'date_trav': None})
            
    anomalies, travaux = [], []
    if df_plume is not None:
        df_plume.columns = [c.strip() for c in df_plume.columns]
        m_list = [{'Site': s, 'INC_V10': v['inc']} for s, v in states.items() if v['maint'] and v['inc']]
        if m_list:
            c_inc = 'Numéro' if 'Numéro' in df_plume.columns else df_plume.columns[0]
            date_resol_col = [c for c in df_plume.columns if 'Ouvert' in c or 'jour' in c][0] 
            merged = pd.merge(pd.DataFrame(m_list), df_plume, left_on='INC_V10', right_on=c_inc, how='inner')
            anom_df = merged[merged['État'].isin(['Résolu', 'Fermé'])].copy()
            
            for _, r in anom_df.iterrows():
                date_cloture = pd.to_datetime(r[date_resol_col])
                jours_clos = (maintenant - date_cloture).days
                anomalies.append({
                    "Code et Nom du Site": r['Site'], 
                    "N° INC": r['INC_V10'], 
                    "Statut Plume": r['État'],
                    "Statut Prynvision": "En maintenance",
                    "Affecté à": r.get('Affecté à', 'N/A'),
                    "_alerte": jours_clos >= 10,
                    "_jours": jours_clos
                })
    
    # Nettoyage des sites supprimés (nan)
    df_anom_final = pd.DataFrame(anomalies)
    if not df_anom_final.empty:
        df_anom_final = df_anom_final[df_anom_final["Code et Nom du Site"].astype(str) != 'nan']
    
    for s, v in states.items():
        if v['travaux'] and v['date_trav']:
            diff = (maintenant - v['date_trav']).days
            travaux.append({
                "Code et Nom du Site": s, 
                "Mise en Travaux": v['date_trav'].strftime('%d/%m/%Y'), 
                "Depuis (Jours)": int(diff), 
                "Statut Prynvision": "En Travaux",
                "Raison (V10)": v['reason'],
                "_jours_t": int(diff)
            })
            
    return df_anom_final, pd.DataFrame(travaux)

# --- INTERFACE ---
st.title("🛡️ Outils Prynvision")
tab_v10, tab_ext = st.tabs(["📊 Analyse V10 / Plume", "📹 Rapport d'Extraction"])

with tab_v10:
    st.header("Analyse Maintenance & Travaux")
    c1, c2 = st.columns(2)
    file_v10 = c1.file_uploader("1. Importer Historique V10 (CSV)", type="csv")
    file_plume = c2.file_uploader("2. Importer Historique Plume (Excel/CSV)", type=["csv", "xlsx"])
    
    if st.button("LANCER L'ANALYSE V10", type="primary"):
        if file_v10:
            df_v10_raw = pd.read_csv(file_v10, sep=';', encoding='latin-1')
            df_p_raw = None
            if file_plume:
                df_p_raw = pd.read_excel(file_plume) if file_plume.name.endswith('xlsx') else pd.read_csv(file_plume)
            
            df_anom, df_trav = analyser_v10_logic(df_v10_raw, df_p_raw)
            st.session_state['df_anom'], st.session_state['df_trav'] = df_anom, df_trav
            st.session_state['v10_msg'] = f"Analyse terminée avec succès ! ({len(df_anom)} cas en maintenance, {len(df_trav)} sites en travaux)"
        else: st.error("Le fichier V10 est requis.")

    if 'v10_msg' in st.session_state:
        st.markdown(f'<div class="status-box">{st.session_state["v10_msg"]}</div>', unsafe_allow_html=True)

    if 'df_anom' in st.session_state:
        search_v10 = st.text_input("🔍 Filtrer...", key="search_v10")
        t_maint, t_trav = st.tabs(["🔧 Anomalies Maintenance", "🏗️ Sites en Travaux"])
        
        with t_maint:
            df_f = st.session_state['df_anom'].copy()
            if search_v10: df_f = df_f[df_f.apply(lambda r: r.astype(str).str.contains(search_v10, case=False).any(), axis=1)]
            
            def colorier_texte(row):
                return ['color: red' if row['_alerte'] else '' for _ in row]

            if not df_f.empty:
                df_f = df_f.sort_values('_jours', ascending=False)
                st.dataframe(df_f.style.apply(colorier_texte, axis=1), use_container_width=True, 
                             column_order=("Code et Nom du Site", "N° INC", "Statut Plume", "Statut Prynvision", "Affecté à"), hide_index=True)
                
                csv_m = df_f.drop(columns=['_alerte', '_jours']).to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig')
                st.download_button("📥 Exporter Maintenance (CSV)", csv_m, "Maintenance_Anomalies.csv", "text/csv")
            else: st.info("Aucune anomalie détectée.")

        with t_trav:
            df_f_t = st.session_state['df_trav'].copy()
            if search_v10: df_f_t = df_f_t[df_f_t.apply(lambda r: r.astype(str).str.contains(search_v10, case=False).any(), axis=1)]
            if not df_f_t.empty:
                df_f_t = df_f_t.sort_values('_jours_t', ascending=False)
                st.dataframe(df_f_t.drop(columns=['_jours_t']), use_container_width=True, hide_index=True)
                
                csv_t = df_f_t.drop(columns=['_jours_t']).to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig')
                st.download_button("📥 Exporter Travaux (CSV)", csv_t, "Sites_En_Travaux.csv", "text/csv")

with tab_ext:
    st.header("Rapport d'Extraction")
    file_ext = st.file_uploader("Importer fichier Rapatriement (.txt)", type="txt")
    if file_ext:
        df_ext = extraire_donnees_ext(file_ext.getvalue())
        if df_ext is not None:
            search_ext = st.text_input("🔍 Filtrer...", key="search_ext")
            df_f = df_ext[df_ext.apply(lambda r: r.astype(str).str.contains(search_ext, case=False).any(), axis=1)] if search_ext else df_ext
            st.dataframe(df_f.sort_values("Taille (Mo)", ascending=False), use_container_width=True, hide_index=True)
            st.download_button("📥 Exporter le Rapport (CSV)", df_f.to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig'), "Rapport_Extractions.csv", "text/csv")
