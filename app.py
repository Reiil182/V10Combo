import streamlit as st
import pandas as pd
import re
from datetime import datetime
from io import BytesIO

# Configuration de la page
st.set_page_config(page_title="Prynvision Toolsuite Web", layout="wide")

# --- LOGIQUE TECHNIQUE : RAPPORT D'EXTRACTION (Issu de Extraction Video.py) ---
def extraire_donnees_ext(file_content):
    # Lecture du contenu brut
    contenu = file_content.decode('latin-1', errors='ignore')
    
    # 1. Capture des DEBUTS (04)
    motif_debut = r"(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}) \(04\) .*? Rapatriement de fichier (.*?) depuis"
    debuts = {}
    for match in re.finditer(motif_debut, contenu):
        ts_str, nom_fichier = match.groups()
        debuts[nom_fichier.strip()] = datetime.strptime(ts_str, "%d/%m/%Y %H:%M:%S")

    # 2. Capture des FINS (05)
    motif_fin = r"(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}) \(05\) (.*?) (\S+) Téléchargement terminé \((.*?)\) - ([\d\.]+) Mo"
    extractions = []
    
    for match in re.finditer(motif_fin, contenu):
        ts_str, site_brut, ident, nom_fichier, taille = match.groups()
        fin_ts = datetime.strptime(ts_str, "%d/%m/%Y %H:%M:%S")
        
        # Nettoyage du site : code 6 chiffres et tout ce qui suit
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
    
    # Groupement et calcul selon vos règles
    for (site, ident, date), groupe in df.groupby(["Site (Code - Nom)", "Traité part", "Transmis"]):
        v_start, v_end = groupe['Start'].dropna(), groupe['End'].dropna()
        
        if not v_start.empty and not v_end.empty:
            duree_sec = (v_end.max() - v_start.min()).total_seconds()
            m, s = divmod(int(duree_sec), 60)
            temps_str = f"{m} min {s} s" if m > 0 else f"{s} s"
        else:
            temps_str = "N/A"
        
        resultats.append({
            "Site (Code - Nom)": site,
            "Traité par": ident,
            "Date": date,
            "Nb d'Extractions": len(groupe),
            "Taille": f"{groupe['Taille_Mo'].sum():.2f} Mo",
            "Temps estimé": temps_str
        })
    return pd.DataFrame(resultats)

# --- LOGIQUE TECHNIQUE : ANALYSE V10 (Issu de Programme.py) ---
def analyser_v10_logic(df_v10, df_plume):
    df_v10.columns = [c.strip() for c in df_v10.columns]
    df_v10['dt'] = pd.to_datetime(df_v10['Date de création'] + ' ' + df_v10['Heure de création'], dayfirst=True)
    df_v10 = df_v10.sort_values('dt')
    
    states, maintenant = {}, datetime.now()
    inc_pat = r'(INC\d+)'
    
    for _, row in df_v10.iterrows():
        site, comm, ack = str(row['Produit']), str(row.get('Commentaire', '')), str(row.get("Heure d'acquittement", ''))
        text = f"{comm} {ack}"
        
        is_m_entry = "Mettre en maintenance" in text or re.search(inc_pat, text, re.IGNORECASE)
        is_m_exit = "Sortir de maintenance" in text
        is_t_entry = "Mettre en travaux" in text or "En Travaux" in text
        is_t_exit = "Sortir de travaux" in text
        
        if site not in states: states[site] = {'maint': False, 'travaux': False, 'inc': None, 'reason': '', 'date_trav': None}
        
        if is_m_exit: states[site]['maint'] = False
        elif is_m_entry:
            states[site]['maint'] = True
            found = re.search(inc_pat, text, re.IGNORECASE)
            if found: states[site]['inc'] = found.group(1).upper()

        if is_t_exit: 
            states[site]['travaux'] = False
            states[site]['date_trav'] = None
        elif is_t_entry:
            states[site]['travaux'] = True
            states[site]['reason'] = ack if "travaux" in ack.lower() else comm
            if states[site]['date_trav'] is None: states[site]['date_trav'] = row['dt']
            
    # Construction des tableaux de sortie
    anomalies, travaux = [], []
    
    if df_plume is not None:
        df_plume.columns = [c.strip() for c in df_plume.columns]
        m_list = [{'Site': s, 'INC_V10': v['inc']} for s, v in states.items() if v['maint'] and v['inc']]
        if m_list:
            c_inc = 'Numéro' if 'Numéro' in df_plume.columns else df_plume.columns[0]
            merged = pd.merge(pd.DataFrame(m_list), df_plume, left_on='INC_V10', right_on=c_inc, how='inner')
            anom_df = merged[merged['État'].isin(['Résolu', 'Fermé'])]
            for _, r in anom_df.iterrows():
                anomalies.append({
                    "Code et Nom du Site": r['Site'], "N° INC": r['INC_V10'], 
                    "Statut Plume": r['État'], "Statut Prynvision": "En maintenance", 
                    "Affecté à": r.get('Affecté à', 'N/A')
                })
    
    for s, v in states.items():
        if v['travaux'] and v['date_trav']:
            diff = (maintenant - v['date_trav']).days
            travaux.append({
                "Code et Nom du Site": s, "Mise en Travaux": v['date_trav'].strftime('%d/%m/%Y'), 
                "Depuis (Jours)": f"{diff} jours", "Statut Prynvision": "En Travaux", 
                "Raison (V10)": v['reason']
            })
            
    return pd.DataFrame(anomalies), pd.DataFrame(travaux)

# --- INTERFACE STREAMLIT ---
st.title("🛡️ Prynvision Toolsuite Web")

tab1, tab2 = st.tabs(["📊 Analyse V10 / Plume", "📹 Rapport d'Extraction"])

with tab1:
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
            
            st.session_state['df_anom'] = df_anom
            st.session_state['df_trav'] = df_trav
            st.toast(f"Analyse terminée : {len(df_anom)} anomalies, {len(df_trav)} travaux")

    if 'df_anom' in st.session_state:
        search = st.text_input("🔍 Filtrer les résultats (Site, INC, Agent...)", key="search_v10")
        
        # Fonction de filtre dynamique
        def filter_df(df, q):
            return df[df.apply(lambda row: row.astype(str).str.contains(q, case=False).any(), axis=1)] if q else df

        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader("Anomalies Maintenance")
            st.dataframe(filter_df(st.session_state['df_anom'], search), use_container_width=True)
        with col_b:
            st.subheader("Sites en Travaux")
            st.dataframe(filter_df(st.session_state['df_trav'], search), use_container_width=True)

with tab2:
    st.header("Rapport d'Extraction")
    file_ext = st.file_uploader("Importer fichier Rapatriement (.txt)", type="txt")
    if file_ext:
        df_ext = extraire_donnees_ext(file_ext.getvalue())
        if df_ext is not None:
            search_ext = st.text_input("🔍 Filtrer les extractions...", key="search_ext")
            df_f = df_ext[df_ext.apply(lambda r: r.astype(str).str.contains(search_ext, case=False).any(), axis=1)] if search_ext else df_ext
            
            st.dataframe(df_f, use_container_width=True)
            
            csv = df_f.to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig')
            st.download_button("📥 Télécharger ce filtrage (CSV)", csv, "Rapport_Filtre.csv", "text/csv")
