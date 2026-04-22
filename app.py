import streamlit as st
import pandas as pd
import re
from datetime import datetime, timedelta

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

# --- LOGIQUE TECHNIQUE : RAPPORT D'EXTRACTION (Inchangée) ---
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
            "Transmis": fin_ts.strftime("%d/%m/%Y"),
            "Traité part": ident,
            "Taille_Mo": float(taille),
            "Start": debuts.get(nom_fichier.strip()),
            "End": fin_ts
        })
    
    if not extractions: return None
    df = pd.DataFrame(extractions)
    resultats = []
    for (site, ident, date), groupe in df.groupby(["Site (Code - Nom)"], ["Traité part"], ["Transmis"]):
        v_start, v_end = groupe['Start'].dropna(), groupe['End'].dropna()
        if not v_start.empty and not v_end.empty:
            duree_sec = (v_end.max() - v_start.min()).total_seconds()
            m, s = divmod(int(duree_sec), 60)
            temps_str = f"{m} min {s} s" if m > 0 else f"{s} s"
        else: temps_str = "N/A"
        resultats.append({
            "Site (Code - Nom)": site, "Traité par": ident, "Date": date,
            "Nb d'Extractions": len(groupe), "Taille": f"{groupe['Taille_Mo'].sum():.2f} Mo",
            "Temps estimé": temps_str
        })
    return pd.DataFrame(resultats)

# --- LOGIQUE TECHNIQUE : ANALYSE V10 AVEC ALERTE ROUGE ---
def analyser_v10_logic(df_v10, df_plume):
    df_v10.columns = [c.strip() for c in df_v10.columns]
    date_col = [c for c in df_v10.columns if 'Date' in c and 'cr' in c][0]
    time_col = [c for c in df_v10.columns if 'Heure' in c and 'cr' in c][0]
    df_v10['dt'] = pd.to_datetime(df_v10[date_col] + ' ' + df_v10[time_col], dayfirst=True)
    df_v10 = df_v10.sort_values('dt')
    
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
            # On cherche la colonne qui contient la date de résolution/fermeture
            date_resol_col = [c for c in df_plume.columns if 'Ouvert' in c or 'jour' in c][0] 
            
            merged = pd.merge(pd.DataFrame(m_list), df_plume, left_on='INC_V10', right_on=c_inc, how='inner')
            anom_df = merged[merged['État'].isin(['Résolu', 'Fermé'])].copy()
            
            for _, r in anom_df.iterrows():
                # Calcul de la différence de jours
                date_cloture = pd.to_datetime(r[date_resol_col])
                jours_clos = (maintenant - date_cloture).days
                
                anomalies.append({
                    "Code et Nom du Site": r['Site'], 
                    "N° INC": r['INC_V10'], 
                    "Statut Plume": r['État'],
                    "Clos depuis": f"{jours_clos} jours",
                    "Affecté à": r.get('Affecté à', 'N/A'),
                    "_alerte": jours_clos > 3 # Champ caché pour le style
                })
    
    for s, v in states.items():
        if v['travaux'] and v['date_trav']:
            travaux.append({
                "Code et Nom du Site": s, "Mise en Travaux": v['date_trav'].strftime('%d/%m/%Y'), 
                "Depuis (Jours)": f"{(maintenant - v['date_trav']).days} jours", 
                "Raison (V10)": v['reason']
            })
            
    return pd.DataFrame(anomalies), pd.DataFrame(travaux)

# --- INTERFACE ---
st.title("🛡️ Outils Prynvision")
tab_v10, tab_ext = st.tabs(["📊 Analyse V10 / Plume", "📹 Rapport d'Extraction"])

with tab_v10:
    st.header("Analyse Maintenance & Travaux")
    c1, c2 = st.columns(2)
    file_v10 = c1.file_uploader("1. Importer Historique V10 (CSV)", type="csv")
    file_plume = c2.file_uploader("2. Importer Historique Plume (Excel/CSV)", type=["csv", "xlsx"])
    
    if st.button("LANCER L'ANALYSE V10", type="primary"):
        if file_v10 and file_plume:
            df_v10_raw = pd.read_csv(file_v10, sep=';', encoding='latin-1')
            df_p_raw = pd.read_excel(file_plume) if file_plume.name.endswith('xlsx') else pd.read_csv(file_plume)
            df_anom, df_trav = analyser_v10_logic(df_v10_raw, df_p_raw)
            st.session_state['df_anom'], st.session_state['df_trav'] = df_anom, df_trav
            st.session_state['v10_msg'] = f"Analyse terminée : {len(df_anom)} anomalies maintenance, {len(df_trav)} sites en travaux."
        else: st.error("Les DEUX fichiers (V10 et Plume) sont nécessaires pour cette option.")

    if 'v10_msg' in st.session_state:
        st.markdown(f'<div class="status-box">{st.session_state["v10_msg"]}</div>', unsafe_allow_html=True)

    if 'df_anom' in st.session_state:
        search_v10 = st.text_input("🔍 Filtrer...", key="search_v10")
        t_maint, t_trav = st.tabs(["🔧 Anomalies Maintenance", "🏗️ Sites en Travaux"])
        
        with t_maint:
            df_f = st.session_state['df_anom']
            if search_v10: df_f = df_f[df_f.apply(lambda r: r.astype(str).str.contains(search_v10, case=False).any(), axis=1)]
            
            # --- APPLICATION DE LA COULEUR ROUGE ---
            def colorier_ligne(row):
                return ['background-color: #ffcccc' if row['_alerte'] else '' for _ in row]

            if not df_f.empty:
                # On affiche tout sauf la colonne technique '_alerte'
                st.dataframe(df_f.style.apply(colorier_ligne, axis=1), use_container_width=True, column_order=("Code et Nom du Site", "N° INC", "Statut Plume", "Clos depuis", "Affecté à"))
            else: st.info("Aucune anomalie détectée.")

        with t_trav:
            df_f_t = st.session_state['df_trav']
            if search_v10: df_f_t = df_f_t[df_f_t.apply(lambda r: r.astype(str).str.contains(search_v10, case=False).any(), axis=1)]
            st.dataframe(df_f_t, use_container_width=True)
