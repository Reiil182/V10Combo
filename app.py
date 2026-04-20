import streamlit as st
import pandas as pd
import re
import os
from datetime import datetime
from io import BytesIO

# Configuration de la page
st.set_page_config(page_title="Prynvision Toolsuite", layout="wide")

# --- LOGIQUE TECHNIQUE COMMUNE ---
def extraire_donnees_ext(file_content):
    debuts, extractions = {}, []
    lignes = file_content.decode('latin-1', errors='ignore').splitlines()
    for ligne in lignes:
        m_deb = re.search(r"(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}) \(04\) .*? Rapatriement de fichier (.*?) depuis", ligne)
        if m_deb:
            ts_str, nom_f = m_deb.groups()
            debuts[nom_f.strip()] = datetime.strptime(ts_str, "%d/%m/%Y %H:%M:%S")

        if "(05)" in ligne and "Téléchargement terminé" in ligne:
            m_fin = re.search(r"(\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}) \(05\) (.*?) (\S+) Téléchargement terminé \((.*?)\) - ([\d\.]+) Mo", ligne)
            if m_fin:
                ts_str, site_brut, ident, nom_f, taille = m_fin.groups()
                fin_ts = datetime.strptime(ts_str, "%d/%m/%Y %H:%M:%S")
                site_clean = re.sub(r"\s+", " ", site_brut).strip()
                reg_match = re.search(r"(\d{6}\s*-\s*[^ ]+)", site_clean)
                nom_final = reg_match.group(1) if reg_match else site_clean
                extractions.append({
                    "Site": nom_final, "Transmis": fin_ts.strftime("%d/%m/%Y"), 
                    "Agent": ident, "Mo": float(taille), "Start": debuts.get(nom_f.strip()), "End": fin_ts
                })
    if not extractions: return None
    df = pd.DataFrame(extractions)
    res = []
    for (site, ident, date), grp in df.groupby(["Site", "Agent", "Transmis"]):
        duree = f"{int((grp['End'].max() - grp['Start'].dropna().min()).total_seconds()//60)} min" if not grp['Start'].dropna().empty else "N/A"
        res.append({"Site (Code - Nom)": site, "Traité par": ident, "Date": date, "Nb Extractions": len(grp), "Taille": f"{grp['Mo'].sum():.2f} Mo", "Temps": duree})
    return pd.DataFrame(res)

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
        if is_t_exit: states[site]['travaux'] = False; states[site]['date_trav'] = None
        elif is_t_entry:
            states[site]['travaux'] = True; states[site]['reason'] = ack if "travaux" in ack.lower() else comm
            if states[site]['date_trav'] is None: states[site]['date_trav'] = row['dt']
            
    # Construction Maintenance
    anomalies = []
    if df_plume is not None:
        df_plume.columns = [c.strip() for c in df_plume.columns]
        m_list = [{'Site': s, 'INC_V10': v['inc']} for s, v in states.items() if v['maint'] and v['inc']]
        if m_list:
            c_inc = 'Numéro' if 'Numéro' in df_plume.columns else df_plume.columns[0]
            merged = pd.merge(pd.DataFrame(m_list), df_plume, left_on='INC_V10', right_on=c_inc, how='inner')
            anom_df = merged[merged['État'].isin(['Résolu', 'Fermé'])]
            for _, r in anom_df.iterrows():
                anomalies.append({"Code et Nom du Site": r['Site'], "N° INC": r['INC_V10'], "Statut Plume": r['État'], "Statut Prynvision": "En maintenance", "Affecté à": r.get('Affecté à', 'N/A')})
    
    # Construction Travaux
    travaux = []
    for s, v in states.items():
        if v['travaux'] and v['date_trav']:
            diff = (maintenant - v['date_trav']).days
            travaux.append({"Code et Nom du Site": s, "Mise en Travaux": v['date_trav'].strftime('%d/%m/%Y'), "Depuis (Jours)": f"{diff} jours", "Statut Prynvision": "En Travaux", "Raison (V10)": v['reason']})
            
    return pd.DataFrame(anomalies), pd.DataFrame(travaux)

# --- INTERFACE ---
st.title("🛡️ Prynvision Toolsuite Web")

tab1, tab2 = st.tabs(["📊 Analyse V10 / Plume", "📹 Rapport d'Extraction"])

with tab1:
    st.subheader("Analyse Maintenance & Travaux")
    c1, c2 = st.columns(2)
    file_v10 = c1.file_uploader("1. Historique V10 (CSV)", type="csv")
    file_plume = c2.file_uploader("2. Historique Plume (Excel/CSV)", type=["csv", "xlsx"])
    
    if st.button("LANCER L'ANALYSE V10", type="primary"):
        if file_v10:
            df_v10_raw = pd.read_csv(file_v10, sep=';', encoding='latin-1')
            df_p_raw = None
            if file_plume:
                df_p_raw = pd.read_excel(file_plume) if file_plume.name.endswith('xlsx') else pd.read_csv(file_plume)
            
            df_anom, df_trav = analyser_v10_logic(df_v10_raw, df_p_raw)
            
            st.toast(f"Analyse terminée : {len(df_anom)} anomalies, {len(df_trav)} travaux")
            
            sub1, sub2 = st.tabs(["Anomalies Maintenance", "Sites en Travaux"])
            with sub1:
                st.dataframe(df_anom, use_container_width=True)
                if not df_anom.empty:
                    st.download_button("Export Maintenance", df_anom.to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig'), "Maint.csv", "text/csv")
            with sub2:
                # Note: Streamlit ne permet pas facilement de colorer seulement 2 colonnes par ligne
                # On affiche donc le tableau propre, triable et filtrable nativement
                st.dataframe(df_trav, use_container_width=True)
                if not df_trav.empty:
                    st.download_button("Export Travaux", df_trav.to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig'), "Travaux.csv", "text/csv")
        else:
            st.error("Importez au moins le fichier V10.")

with tab2:
    st.subheader("Rapport d'Extraction")
    file_ext = st.file_uploader("Importer fichier Rapatriement (.txt)", type="txt")
    if file_ext:
        df_ext = extraire_donnees_ext(file_ext.getvalue())
        if df_ext is not None:
            st.success(f"Fichier analysé avec succès.")
            st.dataframe(df_ext, use_container_width=True)
            st.download_button("📥 Exporter le Rapport (CSV)", df_ext.to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig'), "Extractions.csv", "text/csv")
