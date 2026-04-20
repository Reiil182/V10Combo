import streamlit as st
import pandas as pd
import re
import os
from datetime import datetime
from io import BytesIO

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(page_title="Prynvision Toolsuite", layout="wide")

# --- LOGIQUE TECHNIQUE (EXTRACTION VIDÉO) ---
def extraire_donnees_ext(file_content):
    debuts, extractions = {}, []
    lignes = file_content.decode('latin-1').splitlines()
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
                extractions.append({"Site": nom_final, "Transmis": fin_ts.strftime("%d/%m/%Y"), "Agent": ident, "Mo": float(taille), "Start": debuts.get(nom_f.strip()), "End": fin_ts})
    
    if not extractions: return None
    df = pd.DataFrame(extractions)
    res = []
    for (site, ident, date), grp in df.groupby(["Site", "Agent", "Transmis"]):
        duree = f"{int((grp['End'].max() - grp['Start'].dropna().min()).total_seconds()//60)} min" if not grp['Start'].dropna().empty else "N/A"
        res.append({"Site": site, "Agent": ident, "Date": date, "Nb": len(grp), "Taille": f"{grp['Mo'].sum():.2f} Mo", "Temps": duree})
    return pd.DataFrame(res)

# --- INTERFACE STREAMLIT ---
st.title("🚀 Prynvision Toolsuite Web")

tab1, tab2 = st.tabs(["📊 Analyse V10 / Plume", "📹 Rapport d'Extraction"])

with tab1:
    st.header("Analyse Maintenance & Travaux")
    col1, col2 = st.columns(2)
    file_v10 = col1.file_uploader("Importer Historique V10 (CSV)", type="csv")
    file_plume = col2.file_uploader("Importer Historique Plume (Excel/CSV)", type=["csv", "xlsx"])
    
    if st.button("Lancer l'Analyse V10", type="primary"):
        if file_v10:
            # Note: Ici on simplifie la logique pour l'exemple Web
            df_v10 = pd.read_csv(file_v10, sep=';', encoding='latin-1')
            st.success(f"Fichier V10 chargé : {len(df_v10)} lignes")
            # La logique de traitement complète (states) peut être insérée ici
        else:
            st.error("Le fichier V10 est obligatoire.")

with tab2:
    st.header("Rapport d'Extraction")
    file_ext = st.file_uploader("Importer fichier Rapatriement (.txt)", type="txt")
    
    if file_ext:
        df_res = extraire_donnees_ext(file_ext.getvalue())
        if df_res is not None:
            st.dataframe(df_res, use_container_width=True)
            
            # Export CSV
            csv = df_res.to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig')
            st.download_button("📥 Télécharger le Rapport (CSV)", data=csv, file_name="Rapport_Extractions.csv", mime="text/csv")
        else:
            st.warning("Aucune donnée trouvée dans le fichier.")