import os
import pandas as pd
import io
import zipfile
import json
import openpyxl

import streamlit as st
from sqlalchemy import create_engine, text, inspect

# --- BLOCCO DI INIZIALIZZAZIONE DELLO STATO ---
# questo blocco rende la pagina autosufficiente
if 'wizard_step' not in st.session_state:
    st.session_state['wizard_step'] = 0
if 'tipo_struttura' not in st.session_state:
    st.session_state.tipo_struttura = 'Ditta'
# --- FINE BLOCCO ---

# --- CONFIGURAZIONE E HELPER ---
BASE_DIR = os.getcwd() 
DATA_DIR = os.path.join(BASE_DIR, 'data')
DB_PATH = os.path.join(BASE_DIR, 'db', 'imported_data.sqlite')
MAPPING_BASE_DIR = os.path.join(BASE_DIR, 'mapping')
EXPORT_BASE_DIR = os.path.join(BASE_DIR, 'export')

try:
    engine = create_engine(f'sqlite:///{DB_PATH}')
except Exception as e:
    st.error(f"Errore critico nel motore del database: {e}"); st.stop()

# In pages/1_Wizard_Dati.py, nel blocco delle FUNZIONI HELPER

# In pages/1_Wizard_Dati.py, nel blocco delle FUNZIONI HELPER

# In pages/1_Wizard_Dati.py, nel blocco delle FUNZIONI HELPER
def load_template_callback(config):
    """
    Callback per caricare i dati di un template selezionato nello stato della sessione.
    """
    # Prende il nome del template selezionato dal widget
    template_name = st.session_state[f"template_loader_{config['mode']}"]
    st.session_state.loaded_template_name = None # Resetta il nome del template caricato

    if not template_name or template_name == "-- Non caricare nulla --":
        # Se l'utente deseleziona, puliamo lo stato del template caricato
        if 'loaded_template_data' in st.session_state:
            del st.session_state['loaded_template_data']
        # E anche quello dei singoli widget di mappatura
        keys_to_delete = [k for k in st.session_state.keys() if k.startswith("map_global_")]
        for k in keys_to_delete:
            del st.session_state[k]
        return

    # Costruisce il percorso del file del template
    templates_dir = os.path.join(config["mapping_dir"], "templates")
    safe_filename = "".join(c for c in template_name if c.isalnum() or c in (' ', '_')).rstrip()
    template_path = os.path.join(templates_dir, f"{safe_filename.replace(' ', '_')}.json")

    if os.path.exists(template_path):
        # Se il file esiste, lo legge e salva il suo contenuto nello stato della sessione
        with open(template_path, 'r', encoding='utf-8') as f:
            st.session_state.loaded_template_data = json.load(f)
            # Salva il nome del template attualmente caricato
            st.session_state.loaded_template_name = template_name 
        
        # --- INIZIO BLOCCO DI CORREZIONE ---
        # Pulisci lo stato dei singoli widget di mappatura per FORZARE l'aggiornamento
        # al prossimo ricaricamento della pagina.
        keys_to_delete = [k for k in st.session_state.keys() if k.startswith("map_global_")]
        for k in keys_to_delete:
            del st.session_state[k]
        # --- FINE BLOCCO DI CORREZIONE ---
            
    else:
        st.error(f"File del template '{template_name}' non trovato.")
        if 'loaded_template_data' in st.session_state:
            del st.session_state['loaded_template_data']

def on_mode_change():
    """Pulisce lo stato della sessione che dipende dalla modalità quando questa viene cambiata."""
    keys_to_clear = [k for k in st.session_state.keys() if '_ms_' in k or 'tabella_in_modifica' in k or 'mass_edits' in k or 'exported_file_paths' in k]
    for key in keys_to_clear:
        if key in st.session_state: del st.session_state[key]

def delete_file(directory, filename):
    """Funzione helper per cancellare un file in modo sicuro."""
    try:
        filepath = os.path.join(directory, filename)
        if os.path.exists(filepath): os.remove(filepath)
    except Exception as e:
        st.error(f"Errore durante l'eliminazione del file {filename}: {e}")

def get_current_config():
    """Genera la configurazione dinamica basata sulla modalità selezionata."""
    mode = st.session_state.get('tipo_struttura', 'Ditta').lower()
    config = {
        "mode": mode, "struttura_dir": os.path.join(DATA_DIR, mode, 'struttura'),
        "appoggio_dir": os.path.join(DATA_DIR, mode, 'appoggio'),
        "mapping_dir": os.path.join(MAPPING_BASE_DIR, mode),
        "export_dir": os.path.join(EXPORT_BASE_DIR, mode),
        "db_struttura_prefix": f"struttura_{mode}_", "db_appoggio_suffix": f"_appoggio_{mode}"
    }
    for key, path in config.items():
        if key.endswith("_dir"): os.makedirs(path, exist_ok=True)
    return config

# In pages/1_Wizard_Dati.py, nel blocco delle FUNZIONI HELPER
import unicodedata

def sanitize_column_name(col_name):
    """
    Pulisce aggressivamente il nome di una colonna:
    - Rimuove accenti e caratteri speciali.
    - Converte in minuscolo.
    - Sostituisce spazi e punteggiatura con un singolo trattino basso.
    """
    # Normalizza in NFD (es. 'è' -> 'e' + '`') e rimuove i diacritici (accenti)
    s = ''.join(c for c in unicodedata.normalize('NFD', str(col_name)) if unicodedata.category(c) != 'Mn')
    # Converte in minuscolo e sostituisce caratteri non alfanumerici con spazio
    s = ''.join(c if c.isalnum() else ' ' for c in s.lower())
    # Sostituisce spazi multipli con un singolo trattino basso
    return '_'.join(s.split())

# --- FUNZIONI DEGLI STEP DEL WIZARD ---

def step_0_impostazioni(config):
    mode_name = config['mode'].capitalize()
    st.header('Step 1: Impostazioni Iniziali')
    st.info(f"Il wizard è in esecuzione in modalità: **{mode_name}**.")
    st.warning("Per cambiare modalità, torna alla pagina di Configurazione Iniziale.")
    
    def save_codice_studio_callback():
        st.session_state.codice_studio_valore_sicuro = st.session_state.codice_studio_input_widget
    st.text_input('Codice Studio (3 caratteri)', value=st.session_state.get('codice_studio_valore_sicuro', ''), max_chars=3, key='codice_studio_input_widget', on_change=save_codice_studio_callback)
    
    if st.button('🗑️ Svuota Database e Resetta', key='svuota_db_top'):
        try:
            engine.dispose()
            with engine.connect() as conn:
                inspector = inspect(engine)
                for table_name in inspector.get_table_names(): conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
            for key in list(st.session_state.keys()):
                if key != 'tipo_struttura': del st.session_state[key]
            st.success('Database e stato resettati. Ricarica la pagina (F5).'); st.rerun()
        except Exception as e: st.error(f"Errore: {e}")

def step_1_upload_struttura(config):
    mode_name = config['mode'].capitalize()
    st.header(f"Step 2: Carica File Struttura ({mode_name})")
    with st.form(key=f"upload_form_struttura_{mode_name}", clear_on_submit=True):
        st.write(f"Scegli i file struttura per {mode_name}:")
        uploaded_files = st.file_uploader("Carica file struttura", type=['xlsx'], accept_multiple_files=True, label_visibility="collapsed")
        submitted = st.form_submit_button("Carica i file selezionati")
        if submitted and uploaded_files:
            for f in uploaded_files:
                with open(os.path.join(config["struttura_dir"], f.name), 'wb') as file: file.write(f.getbuffer())
            st.success(f"{len(uploaded_files)} file caricati con successo.")
    st.markdown("---")
    st.write("File attualmente presenti:")
    try:
        files = [f for f in os.listdir(config["struttura_dir"]) if f.endswith('.xlsx')]
        if not files: st.info("Nessun file presente.")
        for i, filename in enumerate(files):
            c1, c2 = st.columns([4, 1])
            c1.info(filename)
            c2.button("🗑️ Rimuovi", key=f"remove_struttura_{mode_name}_{i}", on_click=delete_file, args=(config["struttura_dir"], filename))
    except FileNotFoundError: st.warning("Cartella non ancora creata.")

# In pages/1_Wizard_Dati.py

# In pages/1_Wizard_Dati.py
def step_2_import_struttura(config):
    mode_name = config['mode'].capitalize()
    st.header(f"Step 3: Importa Struttura ({mode_name})")
    st.info(f"I file vengono letti da: `{config['struttura_dir']}`")
    try:
        files = [f for f in os.listdir(config["struttura_dir"]) if f.endswith('.xlsx')]
    except FileNotFoundError: 
        st.error("Cartella struttura non trovata."); return
    if not files: 
        st.warning('Nessun file .xlsx trovato.'); return
    
    # --> MODIFICA QUI: 'default=files' preseleziona tutti i file trovati.
    selected_files = st.multiselect(
        'Seleziona file da importare', 
        files, 
        default=files, 
        key=f'struttura_ms_{mode_name}'
    )
    
    numeric_header_row = st.number_input("Riga intestazioni NUMERICHE", min_value=1, value=2, key=f'struttura_numeric_header_{mode_name}')
    desc_header_row = st.number_input("Riga intestazioni DESCRITTIVE", min_value=1, value=3, key=f'struttura_desc_header_{mode_name}')
    
    if st.button('Importa Struttura', key=f'importa_struttura_btn_{mode_name}'):
        with st.spinner("Importazione..."):
            for file_name in selected_files:
                try:
                    # ... (la logica interna rimane invariata)
                    workbook = openpyxl.load_workbook(os.path.join(config["struttura_dir"], file_name), read_only=True)
                    sheet = workbook.active
                    numeric_values = [cell.value for cell in sheet[numeric_header_row]]
                    descriptive_values = [cell.value for cell in sheet[desc_header_row]]
                    if descriptive_values and str(descriptive_values[0]).strip().lower() == 'non modificare questa riga':
                        numeric_values.pop(0); descriptive_values.pop(0)
                    
                    header_map = {}; final_headers = []; pretty_name_map = {}
                    for desc, num in zip(descriptive_values, numeric_values):
                        if desc and str(desc).strip():
                            original_desc = ' '.join(str(desc).strip().split())
                            clean_desc = sanitize_column_name(original_desc)
                            final_headers.append(clean_desc)
                            header_map[clean_desc] = num
                            pretty_name_map[clean_desc] = original_desc
                    
                    df_structure = pd.DataFrame(columns=final_headers)
                    table_name = f'{config["db_struttura_prefix"]}{os.path.splitext(file_name)[0]}'
                    df_structure.to_sql(table_name, engine, if_exists='replace', index=False)
                    st.success(f"Struttura '{table_name}' importata con colonne sanificate.")
                    
                    header_map_path = os.path.join(config["mapping_dir"], f"{table_name}_headers.json")
                    with open(header_map_path, 'w', encoding='utf-8') as f: json.dump(header_map, f, indent=4)
                    
                    pretty_name_map_path = os.path.join(config["mapping_dir"], f"{table_name}_prettynames.json")
                    with open(pretty_name_map_path, 'w', encoding='utf-8') as f: json.dump(pretty_name_map, f, indent=4)
                    
                except Exception as e: st.error(f"Errore importando {file_name}: {e}")

def step_3_upload_appoggio(config):
    mode_name = config['mode'].capitalize()
    st.header(f'Step 4: Carica File Appoggio ({mode_name})')
    with st.form(key=f"upload_form_appoggio_{mode_name}", clear_on_submit=True):
        st.write(f"Scegli i file di appoggio per {mode_name}:")
        uploaded_files = st.file_uploader("Carica file di appoggio", type=['xlsx'], accept_multiple_files=True, label_visibility="collapsed")
        submitted = st.form_submit_button("Carica i file selezionati")
        if submitted and uploaded_files:
            for f in uploaded_files:
                with open(os.path.join(config["appoggio_dir"], f.name), 'wb') as file: file.write(f.getbuffer())
            st.success(f"{len(uploaded_files)} file caricati.")
    st.markdown("---")
    st.write("File attualmente presenti:")
    try:
        files = [f for f in os.listdir(config["appoggio_dir"]) if f.endswith('.xlsx')]
        if not files: st.info("Nessun file presente.")
        for i, filename in enumerate(files):
            c1, c2 = st.columns([4, 1])
            c1.info(filename)
            c2.button("🗑️ Rimuovi", key=f"remove_appoggio_{mode_name}_{i}", on_click=delete_file, args=(config["appoggio_dir"], filename))
    except FileNotFoundError: st.warning("Cartella non ancora creata.")

# In pages/1_Wizard_Dati.py
def step_4_import_appoggio(config):
    mode_name = config['mode'].capitalize()
    st.header(f"Step 5: Importa Dati di Appoggio ({mode_name})")
    try:
        files = [f for f in os.listdir(config["appoggio_dir"]) if f.endswith('.xlsx')]
    except FileNotFoundError: 
        st.error("Cartella di appoggio non trovata."); return
    if not files: 
        st.warning('Nessun file trovato.'); return
    
    # --> MODIFICA QUI: 'default=files' preseleziona tutti i file trovati.
    selected_files = st.multiselect(
        'Seleziona file', 
        files, 
        default=files, 
        key=f'appoggio_ms_{mode_name}'
    )
    
    header_row = st.number_input("Riga intestazioni", min_value=1, value=1, key=f'appoggio_header_{mode_name}')
    
    if st.button('Importa Dati', key=f'importa_appoggio_btn_{mode_name}'):
        with st.spinner("Importazione..."):
            for file_name in selected_files:
                try:
                    # ... (la logica interna rimane invariata)
                    df = pd.read_excel(os.path.join(config["appoggio_dir"], file_name), header=header_row - 1, dtype=str).fillna('')
                    
                    pretty_name_map = {sanitize_column_name(col): str(col).strip() for col in df.columns}
                    df.columns = [sanitize_column_name(col) for col in df.columns]

                    table_name = f'{os.path.splitext(file_name)[0]}{config["db_appoggio_suffix"]}'
                    df.to_sql(table_name, engine, if_exists='replace', index=False)
                    st.success(f"Dati '{table_name}' importati con colonne sanificate.")

                    pretty_name_map_path = os.path.join(config["mapping_dir"], f"{table_name}_prettynames.json")
                    with open(pretty_name_map_path, 'w', encoding='utf-8') as f: json.dump(pretty_name_map, f, indent=4)
                    
                except Exception as e: st.error(f"Errore importando {file_name}: {e}")

# In pages/1_Wizard_Dati.py
def step_5_mappatura_globale(config):
    mode_name = config['mode'].capitalize()
    st.header(f'Step 6: Mappatura Globale ({mode_name})')
    try:
        # Caricamento tabelle e opzioni
        inspector = inspect(engine)
        all_tables = inspector.get_table_names()
        struttura_tables = sorted([t for t in all_tables if t.startswith(config["db_struttura_prefix"])])
        appoggio_tables = sorted([t for t in all_tables if t.endswith(config["db_appoggio_suffix"])])
        if not struttura_tables or not appoggio_tables: 
            st.error(f'Importa tabelle Struttura e Appoggio per la modalità {mode_name}.'); return
        
        # Caricamento mappe dei nomi leggibili
        master_pretty_name_map = {}
        all_relevant_tables = struttura_tables + appoggio_tables
        for table_name in all_relevant_tables:
            pretty_name_path = os.path.join(config["mapping_dir"], f"{table_name}_prettynames.json")
            if os.path.exists(pretty_name_path):
                with open(pretty_name_path, 'r', encoding='utf-8') as f:
                    master_pretty_name_map.update(json.load(f))

        def get_pretty_name(sanitized_name):
            return master_pretty_name_map.get(sanitized_name, sanitized_name)

        unique_dest_cols = set()
        for table in struttura_tables:
            cols = pd.read_sql(f'SELECT * FROM "{table}" LIMIT 0', engine).columns
            unique_dest_cols.update(cols)
        sorted_unique_cols = sorted(list(unique_dest_cols))
        dest_options = ["-- Non mappare --"] + sorted_unique_cols

        # Sezione Gestione Template
        st.subheader("Gestione Template di Mappatura")
        templates_dir = os.path.join(config["mapping_dir"], "templates")
        os.makedirs(templates_dir, exist_ok=True)
        saved_templates = ["-- Non caricare nulla --"] + sorted([os.path.splitext(f)[0].replace('_', ' ') for f in os.listdir(templates_dir) if f.endswith('.json')])
        
        loaded_template_name = st.session_state.get('loaded_template_name')
        index = saved_templates.index(loaded_template_name) if loaded_template_name in saved_templates else 0

        st.selectbox("Carica un Template:", options=saved_templates, index=index, key=f"template_loader_{config['mode']}", on_change=load_template_callback, args=(config,))
        st.markdown("---")
        
        # Prende i dati dal template caricato o dalla sessione
        loaded_mapping_data = st.session_state.get('loaded_template_data', {})
        mapping_path = os.path.join(config["mapping_dir"], "global_mapping.json")
        if not loaded_mapping_data and os.path.exists(mapping_path):
             with open(mapping_path, 'r', encoding='utf-8') as f: loaded_mapping_data.setdefault("column_mappings", json.load(f))

        current_mapping = loaded_mapping_data.get('column_mappings', {})
        
        # Interfaccia Mappatura Colonne Dati
        st.subheader("Mappatura Colonne Dati")
        new_mapping = {}
        for appoggio_table in appoggio_tables:
            st.write(f"**Sorgente: {appoggio_table}**")
            appoggio_cols_sanitized = pd.read_sql(f'SELECT * FROM "{appoggio_table}" LIMIT 0', engine).columns
            for col_source_sanitized in appoggio_cols_sanitized:
                source_key = f"{appoggio_table}.{col_source_sanitized}"
                default_selection = current_mapping.get(source_key, dest_options[0])
                default_index = dest_options.index(default_selection) if default_selection in dest_options else 0
                
                scelta = st.selectbox(
                    f"`{get_pretty_name(col_source_sanitized)}`",
                    options=dest_options,
                    index=default_index,
                    key=f"map_global_{source_key}_{mode_name}",
                    format_func=get_pretty_name
                )
                if scelta != "-- Non mappare --":
                    new_mapping[source_key] = scelta
        
        st.markdown("---")
        
        # Impostazioni Aggiuntive
        st.subheader("Impostazioni Aggiuntive")
        default_date_cols = loaded_mapping_data.get('date_format_columns', [])
        valid_default_dates = [col for col in default_date_cols if col in sorted_unique_cols]
        selected_date_cols = st.multiselect("Colonne da formattare come Data:", options=sorted_unique_cols, default=valid_default_dates, key=f'date_cols_selector_{mode_name}', format_func=get_pretty_name)
        
        default_studio_col = loaded_mapping_data.get('studio_code_column', "-- Non applicare --")
        studio_dest_options = ["-- Non applicare --"] + sorted_unique_cols
        studio_col_default_index = studio_dest_options.index(default_studio_col) if default_studio_col in studio_dest_options else 0
        selected_studio_col = st.selectbox("Colonna per Codice Studio:", options=studio_dest_options, index=studio_col_default_index, key=f'studio_col_selector_{mode_name}', format_func=get_pretty_name)
        
        st.markdown("---")

        # Sezione Azioni sui Template
        st.subheader("Azioni sui Template")
        col1, col2 = st.columns(2)
        with col1:
            template_name_input = st.text_input("Nome per nuovo template:", key=f"template_name_input_{mode_name}", placeholder="Es. Mappatura Standard Ditte")
            if st.button("Salva come Nuovo Template", key=f"save_as_template_btn_{mode_name}", use_container_width=True):
                if template_name_input and template_name_input.strip():
                    safe_filename = "".join(c for c in template_name_input if c.isalnum() or c in (' ', '_')).rstrip()
                    template_path = os.path.join(templates_dir, f"{safe_filename.replace(' ', '_')}.json")
                    if os.path.exists(template_path): st.warning("Un template con questo nome esiste già.")
                    else:
                        template_data = {"column_mappings": new_mapping, "date_format_columns": selected_date_cols, "studio_code_column": selected_studio_col if selected_studio_col != "-- Non applicare --" else ""}
                        with open(template_path, 'w', encoding='utf-8') as f: json.dump(template_data, f, indent=4)
                        st.success(f"Template '{template_name_input}' salvato!"); st.rerun()
                else: st.error("Inserisci un nome per il template.")
        with col2:
            st.write(""); st.write("")
            if st.button("Aggiorna Template Selezionato", key=f"update_template_btn_{mode_name}", disabled=(not loaded_template_name), use_container_width=True):
                template_data = {"column_mappings": new_mapping, "date_format_columns": selected_date_cols, "studio_code_column": selected_studio_col if selected_studio_col != "-- Non applicare --" else ""}
                safe_filename = "".join(c for c in loaded_template_name if c.isalnum() or c in (' ', '_')).rstrip()
                template_path = os.path.join(templates_dir, f"{safe_filename.replace(' ', '_')}.json")
                with open(template_path, 'w', encoding='utf-8') as f: json.dump(template_data, f, indent=4)
                st.success(f"Template '{loaded_template_name}' aggiornato!")
        
        st.markdown("---")
        
        st.subheader("Salvataggio per la Sessione Corrente")
        if st.button("Salva Impostazioni per Questa Sessione", type="primary", key=f'salva_mappature_btn_{mode_name}', use_container_width=True):
            with open(mapping_path, 'w', encoding='utf-8') as f: json.dump(new_mapping, f, indent=4)
            st.success("Mappatura di sessione salvata!")
            date_format_path = os.path.join(config["mapping_dir"], "date_columns.json")
            with open(date_format_path, 'w', encoding='utf-8') as f: json.dump({"date_columns": selected_date_cols}, f, indent=4)
            st.success("Impostazione colonne data salvata!")
            studio_mapping_path = os.path.join(config["mapping_dir"], "studio_mapping.json")
            studio_map_data = {"codice_studio_column": selected_studio_col if selected_studio_col != "-- Non applicare --" else ""}
            with open(studio_mapping_path, 'w', encoding='utf-8') as f: json.dump(studio_map_data, f, indent=4)
            st.success("Impostazione Codice Studio salvata!")
            
    except Exception as e: 
        st.error(f"Errore durante la mappatura: {e}"); st.exception(e)

# In pages/1_Wizard_Dati.py
def step_6_popola_dati(config):
    mode_name = config['mode'].capitalize()
    st.header(f"Step 7: Popola Dati ({mode_name})")

    if st.button("APPLICA MAPPATURA E POPOLA", key=f'popola_btn_{mode_name}'):
        with st.spinner("Popolamento in corso..."):
            try:
                # 1. Caricamento Globale (una sola volta)
                mapping_path = os.path.join(config["mapping_dir"], "global_mapping.json")
                if not os.path.exists(mapping_path): st.error(f"'global_mapping.json' non trovato."); return
                with open(mapping_path, 'r', encoding='utf-8') as f: global_mapping = json.load(f)
                
                studio_mapping_path = os.path.join(config["mapping_dir"], "studio_mapping.json")
                studio_target_col = ""
                if os.path.exists(studio_mapping_path):
                    with open(studio_mapping_path, 'r', encoding='utf-8') as f: studio_target_col = json.load(f).get("codice_studio_column", "")
                codice_studio_value = st.session_state.get('codice_studio_valore_sicuro', '').upper()

                inspector = inspect(engine)
                appoggio_tables = [t for t in inspector.get_table_names() if t.endswith(config["db_appoggio_suffix"])]
                struttura_tables = [t for t in inspector.get_table_names() if t.startswith(config["db_struttura_prefix"])]
                
                if not appoggio_tables: st.warning("Dati di appoggio non trovati."); return
                df_appoggio = pd.read_sql_table(appoggio_tables[0], engine).astype(str)

                # 2. Inizio Ciclo: un'elaborazione per ogni tabella di destinazione
                for struttura_table in struttura_tables:
                    st.write(f"--- Elaborazione per `{struttura_table}` ---")
                    
                    # 2a. Filtra la mappatura per usare solo le colonne di questa specifica tabella
                    dest_cols_for_this_table = pd.read_sql(f'SELECT * FROM "{struttura_table}" LIMIT 0', engine).columns.tolist()
                    current_mapping = {
                        source: dest for source, dest in global_mapping.items() 
                        if dest in dest_cols_for_this_table
                    }
                    if not current_mapping:
                        st.warning(f"Nessuna mappatura valida trovata per le colonne in '{struttura_table}'. Tabella saltata.")
                        continue

                    # 2b. Esegui la logica di trasformazione usando SOLO la mappatura filtrata
                    df_popolato = pd.DataFrame()
                    if config['mode'] == 'ditta':
                        dest_to_sources = {dest: [s for s, d in current_mapping.items() if d == dest] for dest in set(current_mapping.values())}
                        unpivot_destinations = {dest: sources for dest, sources in dest_to_sources.items() if len(sources) > 1}
                        context_mapping = {source: dest for source, dest in current_mapping.items() if dest not in unpivot_destinations}
                        
                        final_rows = []
                        for _, source_row in df_appoggio.iterrows():
                            base_row_data = {dest: source_row.get(source.split('.')[1]) for source, dest in context_mapping.items()}
                            rows_generated = 0

                            if unpivot_destinations:
                                for dest_col, source_keys in unpivot_destinations.items():
                                    for source_key in source_keys:
                                        value = source_row.get(source_key.split('.')[1])
                                        if pd.notna(value) and str(value).strip() != '':
                                            new_row = base_row_data.copy()
                                            new_row[dest_col] = value
                                            final_rows.append(new_row)
                                            rows_generated += 1
                            
                            if rows_generated == 0:
                                final_rows.append(base_row_data)
                        df_popolato = pd.DataFrame(final_rows)
                    else: # Logica Standard per Dipendente
                        map_short = {s.split('.')[1]: d for s, d in current_mapping.items()}
                        df_popolato = df_appoggio.rename(columns=map_short)

                    # 2c. Salva il risultato specifico per questa tabella
                    if studio_target_col and codice_studio_value:
                        df_popolato[studio_target_col] = codice_studio_value
                    
                    df_popolato = df_popolato.reindex(columns=dest_cols_for_this_table).fillna('')
                    df_popolato.to_sql(struttura_table, engine, if_exists='replace', index=False)
                    st.success(f"Tabella '{struttura_table}' popolata con successo con {len(df_popolato)} righe.")

            except Exception as e: 
                st.error(f"Errore durante il popolamento: {e}"); st.exception(e)

def step_7_modifica_massiva(config):
    mode_name = config['mode'].capitalize()
    st.header(f"Step 8: Modifica Massiva ({mode_name})")
    try:
        inspector = inspect(engine)
        struttura_tables = sorted([t for t in inspector.get_table_names() if t.startswith(config["db_struttura_prefix"])])
        if not struttura_tables: st.warning("Nessuna tabella dati da modificare."); return

        session_key_table = f'tabella_in_modifica_{mode_name}'; session_key_edits = f'mass_edits_{mode_name}'
        if session_key_table not in st.session_state: st.session_state[session_key_table] = ""
        
        selected_table = st.selectbox(f"1. Seleziona tabella", [""] + struttura_tables, key=f"modifica_tabella_selector_{mode_name}")
        if selected_table and selected_table != st.session_state.get(session_key_table):
            if st.button(f"Prepara Modifiche per '{selected_table}'", key=f"prepare_edit_btn_{mode_name}"):
                st.session_state[session_key_table] = selected_table
                st.session_state[session_key_edits] = [{"col": "", "val": ""}]
                st.rerun()
        elif not selected_table and st.session_state.get(session_key_table):
            st.session_state[session_key_table] = ""; st.session_state[session_key_edits] = []; st.rerun()
        
        active_table = st.session_state.get(session_key_table)
        if active_table:
            if session_key_edits not in st.session_state: st.session_state[session_key_edits] = []
            st.markdown("---"); st.subheader(f"2. Imposta le modifiche per: `{active_table}`")
            table_cols = [""] + list(pd.read_sql(f'SELECT * FROM "{active_table}" LIMIT 0', engine).columns)

            for i in range(len(st.session_state.get(session_key_edits, []))):
                with st.container(border=True):
                    c1, c2, c3 = st.columns([4, 4, 1])
                    edit = st.session_state[session_key_edits][i]
                    default_col_idx = table_cols.index(edit["col"]) if "col" in edit and edit["col"] in table_cols else 0
                    edit["col"] = c1.selectbox("Colonna", table_cols, index=default_col_idx, key=f"edit_col_{mode_name}_{i}")
                    edit["val"] = c2.text_input("Nuovo Valore", value=edit.get("val", ""), key=f"edit_val_{mode_name}_{i}")
                    if c3.button("🗑️", key=f"remove_edit_{mode_name}_{i}", help="Rimuovi"):
                        st.session_state[session_key_edits].pop(i); st.rerun()
            
            c_btn1, c_btn2, _ = st.columns([2, 2, 8])
            if c_btn1.button("➕ Aggiungi modifica", key=f"add_edit_btn_{mode_name}"):
                st.session_state[session_key_edits].append({"col": "", "val": ""}); st.rerun()
            if c_btn2.button("✅ Applica modifiche", type="primary", key=f"apply_all_edits_btn_{mode_name}"):
                with st.spinner("Applicazione..."):
                    valid_edits = [e for e in st.session_state[session_key_edits] if e.get("col")]
                    if not valid_edits: st.warning("Nessuna modifica valida."); st.stop()
                    df_to_modify = pd.read_sql_table(active_table, engine)
                    for single_edit in valid_edits: df_to_modify[single_edit["col"]] = single_edit["val"]
                    df_to_modify.to_sql(active_table, engine, if_exists='replace', index=False)
                    st.success(f"Tabella '{active_table}' aggiornata!"); st.rerun()
            
            st.markdown("---"); st.write(f"Anteprima di **{active_table}**:")
            st.dataframe(pd.read_sql_table(active_table, engine))
    except Exception as e: st.error(f"Errore: {e}"); st.exception(e)

# In pages/1_Wizard_Dati.py
def step_8_export_globale(config):
    mode_name = config['mode'].capitalize()
    st.header(f'Step 9: Export Globale Finale ({mode_name})')
    
    export_state_key = f'exported_file_paths_{mode_name}'
    if export_state_key not in st.session_state: 
        st.session_state[export_state_key] = None

    if st.session_state.get(export_state_key):
        st.success(f"Export completato. {len(st.session_state[export_state_key])} file sono pronti.")
        for f_path in st.session_state[export_state_key]:
            file_name = os.path.basename(f_path)
            if os.path.exists(f_path):
                with open(f_path, 'rb') as f:
                    st.download_button(f"⬇️ Scarica {file_name}", f.read(), file_name, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=f"dl_{file_name}_{mode_name}")
            else:
                st.error(f"File di export '{file_name}' non trovato. Riprova l'export.")
        
        if len(st.session_state[export_state_key]) > 1:
            st.markdown("---")
            with st.spinner("Creazione ZIP..."):
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, "w") as zipf:
                    for f_path in st.session_state[export_state_key]:
                        if os.path.exists(f_path): zipf.write(f_path, arcname=os.path.basename(f_path))
                st.download_button("📦 Scarica tutto (ZIP)", zip_buffer.getvalue(), f"export_{mode_name}.zip", "application/zip", key=f"dl_zip_btn_{mode_name}")
        st.markdown("---")
        if st.button("Esegui un nuovo export", key=f"clear_export_btn_{mode_name}"):
            st.session_state[export_state_key] = None; st.rerun()
    else:
        st.info("Premi il bottone per generare i file di export finali.")
        if st.button("AVVIA EXPORT FINALE", key=f'start_final_export_btn_{mode_name}', type="primary"):
            with st.spinner("Creazione file..."):
                try:
                    date_format_path = os.path.join(config["mapping_dir"], "date_columns.json"); colonne_data = []
                    if os.path.exists(date_format_path):
                        with open(date_format_path, 'r', encoding='utf-8') as f: colonne_data = json.load(f).get("date_columns", [])
                    
                    inspector = inspect(engine)
                    struttura_tables = [t for t in inspector.get_table_names() if t.startswith(config["db_struttura_prefix"])]
                    if not struttura_tables: st.warning("Nessuna tabella dati da esportare trovata."); st.stop()

                    generated_paths = []
                    for struttura_table in struttura_tables:
                        header_map_path = os.path.join(config["mapping_dir"], f"{struttura_table}_headers.json"); header_map = {}
                        if os.path.exists(header_map_path):
                             with open(header_map_path, 'r', encoding='utf-8') as f: header_map = json.load(f)
                        else:
                            st.error(f"Mappa intestazioni per {struttura_table} non trovata."); continue

                        df_to_export = pd.read_sql_table(struttura_table, engine)
                        
                        if df_to_export.empty:
                            st.warning(f"La tabella '{struttura_table}' è vuota, l'export per questo file sarà vuoto.")
                        
                        for col in colonne_data:
                            if col in df_to_export.columns:
                                df_to_export[col] = pd.to_datetime(df_to_export[col], errors='coerce').dt.strftime('%d/%m/%Y').fillna('')
                        
                        dest_cols = list(df_to_export.columns)
                        numeric_headers_row = [header_map.get(col, '') for col in dest_cols]
                        wb_export = openpyxl.Workbook()
                        ws_export = wb_export.active
                        base_name = struttura_table.replace(config["db_struttura_prefix"], '')
                        ws_export.append(["Non modificare questa riga", base_name.upper()])
                        ws_export.append(["Non modificare questa riga"] + numeric_headers_row)
                        ws_export.append(["Non modificare questa riga"] + dest_cols)
                        
                        for row_data_tuple in df_to_export.itertuples(index=False, name=None):
                            ws_export.append([""] + list(row_data_tuple))

                        export_file_name = f"{base_name}_Export.xlsx"
                        export_file_path = os.path.join(config["export_dir"], export_file_name)
                        wb_export.save(export_file_path)
                        generated_paths.append(export_file_path)

                    st.session_state[export_state_key] = generated_paths
                    st.rerun()
                except Exception as e:
                    st.error(f"Errore durante l'export: {e}"); st.exception(e)

# --- GESTIONE WIZARD E UI ---
st.title('Importazione e Mappatura Dati')
st.sidebar.title("Navigazione")

step_labels = ['Impostazioni', 'Carica Struttura', 'Importa Struttura', 'Carica Dati', 'Importa Dati', 'Mappatura', 'Popola Dati', 'Modifica Massiva', 'Export Globale']
step_functions = [
    step_0_impostazioni, step_1_upload_struttura, step_2_import_struttura,
    step_3_upload_appoggio, step_4_import_appoggio, step_5_mappatura_globale,
    step_6_popola_dati, step_7_modifica_massiva, step_8_export_globale
]

current_step_idx = st.session_state.get('wizard_step', 0)
if 'wizard_nav_radio' not in st.session_state: st.session_state.wizard_nav_radio = current_step_idx

def update_step():
    st.session_state.wizard_step = st.session_state.wizard_nav_radio
st.sidebar.radio(
    "Passaggi:", range(len(step_labels)), 
    format_func=lambda x: f"Step {x+1}: {step_labels[x]}", 
    index=current_step_idx, key="wizard_nav_radio", on_change=update_step
)

config = get_current_config()
# NUOVO BLOCCO PIÙ SICURO
try:
    # Tentiamo di eseguire lo step corrente
    step_idx = st.session_state.get('wizard_step', 0)
    step_functions[step_idx](config)

except Exception as e:
    # Se si verifica un errore, lo mostriamo in modo sicuro senza causare altri errori.
    # Usiamo .get() per accedere alla chiave in modo sicuro.
    step_for_error_msg = st.session_state.get('wizard_step', -1) + 1
    st.error(f"Si è verificato un errore primario nello Step {step_for_error_msg}.")
    
    # Questa linea è la più importante: stamperà il VERO errore e il suo traceback.
    st.exception(e)

st.markdown("---")
c1, c2, _ = st.columns([2, 2, 8])
if st.session_state['wizard_step'] > 0:
    if c1.button('◀️ Indietro', key='nav_indietro', use_container_width=True): 
        st.session_state['wizard_step'] -= 1; st.rerun()
if st.session_state['wizard_step'] < len(step_functions) - 1:
    if c2.button('Avanti ▶️', key='nav_avanti', use_container_width=True): 
        st.session_state['wizard_step'] += 1; st.rerun()