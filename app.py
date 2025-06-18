# Contenuto per il file principale app.py

import streamlit as st
import pandas as pd

# Imposta la configurazione della pagina
st.set_page_config(
    layout="wide",
    page_title="Configurazione Iniziale",
    page_icon="⚙️"
)

# Inizializza lo stato di sessione se non esiste
if 'tipo_struttura' not in st.session_state:
    st.session_state.tipo_struttura = 'Ditta'

# --- Contenuto della Pagina Principale ---

st.title("Configurazione Iniziale del Processo di Migrazione")
st.markdown("---")

st.subheader("1. Seleziona la modalità di lavoro")
st.info(
    "Questa scelta determinerà su quali dati opererà l'intero wizard. "
    "Potrai sempre tornare su questa pagina per cambiare la modalità."
)

# Il selettore della modalità vive solo e soltanto qui.
selected_mode = st.radio(
    "Su quale tipo di anagrafica vuoi lavorare?",
    ['Ditta', 'Dipendente'],
    index=['Ditta', 'Dipendente'].index(st.session_state.get('tipo_struttura', 'Ditta')),
    horizontal=True,
)

# Aggiorna lo stato di sessione in modo esplicito
if st.session_state.tipo_struttura != selected_mode:
    st.session_state.tipo_struttura = selected_mode
    st.rerun()

st.markdown("---")
st.subheader("2. Procedi con l'elaborazione")
st.success(f"Modalità di lavoro attualmente impostata su: **{st.session_state.tipo_struttura}**.")

# Link alla pagina del wizard
st.page_link(
    "pages/1_Wizard_Dati.py",
    label="Vai al Wizard di Elaborazione Dati",
    icon="➡️"
)