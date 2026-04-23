import streamlit as st
import pandas as pd
import re
from datetime import datetime

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(page_title="Outils Prynvision", layout="wide")

# --- STYLE PERSONNALISÉ (CSS) ---
st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] { height: 50px; padding: 10px 20px; }
    .stTabs [aria-selected="true"] { color: #1F6FEB !important; font-weight: bold; }
    /* Style du bloc KPI pour le total Go */
    .kpi-box {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        border-left: 5px solid #1f6feb;
        margin-bottom: 20px;
    }
    .kpi-value { font-size: 24px; font-weight: bold; color: #1f6feb; }
    .status-box { padding: 15px; border-radius: 5px; margin: 10px 0; background-color: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
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
            "Site": nom_final,
            "Taille_Mo": float(taille),
            "Start": debuts.get(nom_fichier.strip()),
            "End": fin_ts,
            "Traite_par": ident
        })
    
    if not extractions: return None
    df = pd.DataFrame(extractions)
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

# --- LOGIQUE TECHNIQUE : ANALYSE V10 --- (Identique à la version précédente)
def analyser_v10_logic(df_v10, df_plume):
    # Nettoyage et logique simplifiée pour l'exemple
    anomalies, travaux = [], []
    # ... (Le code de traitement V10 reste ici)
    return pd.DataFrame(anomalies), pd.DataFrame(travaux)

# --- INTERFACE UTILISATEUR ---
st.title("🛡️ Outils Prynvision")
tab_v10, tab_ext = st.tabs(["📊 Analyse V10 / Plume", "📹 Rapport d'Extraction"])

with tab_v10:
    st.info("Utilisez cette section pour croiser V10 et Plume.")
    # (Le contenu de l'onglet V10 reste le même)

with tab_ext:
    st.header("Analyse des Rapatriements")
    file_ext = st.file_uploader("Importer le fichier log (.txt)", type="txt")
    
    if file_ext:
        df_ext = extraire_donnees_ext(file_ext.getvalue())
        
        if df_ext is not None:
            # --- CALCUL DU TOTAL EN GO ---
            total_mo = df_ext["Taille (Mo)"].sum()
            total_go = total_mo / 1024
            
            # --- AFFICHAGE DE LA CASE INFO (KPI) ---
            st.markdown(f"""
                <div class="kpi-box">
                    <div style="color: #555; font-size: 14px;">Volume Total Extrait</div>
                    <div class="kpi-value">{total_go:.2f} Go</div>
                    <div style="color: #888; font-size: 12px;">soit {total_mo:,.0f} Mo</div>
                </div>
                """, unsafe_allow_html=True)

            # Filtre et Tableau
            search_ext = st.text_input("🔍 Filtrer les sites ou techniciens...")
            df_display = df_ext.copy()
            if search_ext:
                df_display = df_display[df_display.apply(lambda r: r.astype(str).str.contains(search_ext, case=False).any(), axis=1)]
            
            st.dataframe(df_display.sort_values("Taille (Mo)", ascending=False), use_container_width=True, hide_index=True)
            
            csv = df_display.to_csv(index=False, sep=';', encoding='utf-8-sig').encode('utf-8-sig')
            st.download_button("📥 Télécharger le rapport CSV", csv, "Rapport_Extractions.csv")
        else:
            st.warning("Aucune donnée d'extraction trouvée dans ce fichier.")
