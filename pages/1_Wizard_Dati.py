import os
import pandas as pd
import io
import zipfile
import json
import openpyxl
import numpy as np
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

# VERSIONE FINALE DELLA FUNZIONE HELPER - Supporta la logica "ibrida"
def crea_righe_multiple(df: pd.DataFrame, key_cols_map: dict, context_cols_map: dict, unpivot_map: dict) -> pd.DataFrame:
    """
    Crea righe multiple con logica ibrida: la prima riga è completa, le successive sono sparse.
    - key_cols_map: Colonne chiave da ripetere su OGNI riga. {dest: source}
    - context_cols_map: Colonne di contesto da mostrare SOLO sulla prima riga. {dest: source}
    - unpivot_map: Colonne da trasformare. {dest: [source_1, source_2, ...]}
    """
    final_rows = []
    num_groups = max(len(v) for v in unpivot_map.values()) if unpivot_map else 0
    if num_groups == 0: return pd.DataFrame()

    for _, source_row in df.iterrows():
        is_first_row_for_this_company = True
        
        # Prepara i dati chiave che si ripeteranno sempre
        key_data = {dest_col: source_row.get(source_col) for dest_col, source_col in key_cols_map.items()}
        # Prepara i dati di contesto che appariranno solo una volta
        context_data = {dest_col: source_row.get(source_col) for dest_col, source_col in context_cols_map.items()}

        for i in range(num_groups):
            new_row_segment = {}
            is_valid_row = False
            
            for dest_col, source_cols_list in unpivot_map.items():
                try:
                    value = source_row.get(source_cols_list[i], '')
                    new_row_segment[dest_col] = value
                    if str(value).strip():
                        is_valid_row = True
                except IndexError:
                    new_row_segment[dest_col] = ''
            
            if is_valid_row:
                if is_first_row_for_this_company:
                    # Per la prima riga, unisci tutto: chiavi + contesto + dati trasformati
                    full_row = {**key_data, **context_data, **new_row_segment}
                    is_first_row_for_this_company = False
                else:
                    # Per le righe successive, unisci solo: chiavi + dati trasformati
                    full_row = {**key_data, **new_row_segment}
                
                final_rows.append(full_row)
                
    return pd.DataFrame(final_rows)

try:
    engine = create_engine(f'sqlite:///{DB_PATH}')
except Exception as e:
    st.error(f"Errore critico nel motore del database: {e}"); st.stop()

# Sostituisci questa funzione
def load_template_callback(config):
    """
    Callback per caricare i dati di un template selezionato nello stato della sessione.
    """
    mode = config['mode']
    template_name = st.session_state.get(f"template_loader_{mode}")
    
    # Chiave per il nostro stato di mappatura centrale
    live_mapping_state_key = f"live_mapping_state_{mode}"

    # Reset dei nomi dei template
    st.session_state.loaded_template_name = None
    if 'loaded_template_data' in st.session_state:
        del st.session_state['loaded_template_data']

    # Pulisce lo stato dei widget e lo stato centrale
    keys_to_delete = [k for k in st.session_state.keys() if k.startswith(f"map_global_") or k == live_mapping_state_key]
    for k in keys_to_delete:
        del st.session_state[k]

    if not template_name or template_name == "-- Non caricare nulla --":
        return

    # Costruisce il percorso e carica il file
    templates_dir = os.path.join(config["mapping_dir"], "templates")
    safe_filename = "".join(c for c in template_name if c.isalnum() or c in (' ', '_')).rstrip()
    template_path = os.path.join(templates_dir, f"{safe_filename.replace(' ', '_')}.json")

    if os.path.exists(template_path):
        with open(template_path, 'r', encoding='utf-8') as f:
            # Carica TUTTI i dati del template in uno stato dedicato
            st.session_state.loaded_template_data = json.load(f)
            st.session_state.loaded_template_name = template_name 
            # Non facciamo altro qui. L'inizializzazione dello stato avverrà in step_5
    else:
        st.error(f"File del template '{template_name}' non trovato.")

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

def step_0_impostazioni(config, engine):
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

def step_1_upload_struttura(config, engine):
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
def step_2_import_struttura(config, engine):
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

def step_3_upload_appoggio(config, engine):
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

# SOSTITUISCI IL TUO step_4 CON QUESTA VERSIONE POTENZIATA
def step_4_import_appoggio(config, engine):
    mode_name = config['mode'].capitalize()
    st.header(f"Step 5: Importa Dati di Appoggio ({mode_name})")
    try:
        files = [f for f in os.listdir(config["appoggio_dir"]) if f.endswith('.xlsx')]
    except FileNotFoundError: 
        st.error("Cartella di appoggio non trovata."); return
    if not files: 
        st.warning('Nessun file trovato.'); return
    
    selected_files = st.multiselect(
        'Seleziona file da importare', 
        files, 
        default=files, 
        key=f'appoggio_ms_{mode_name}'
    )
    
    header_row = st.number_input("Riga intestazioni", min_value=1, value=1, key=f'appoggio_header_{mode_name}')
    
    if st.button('Importa Dati', key=f'importa_appoggio_btn_{mode_name}'):
        with st.spinner("Importazione..."):
            for file_name in selected_files:
                try:
                    file_path = os.path.join(config["appoggio_dir"], file_name)
                    
                    # --- BLOCCO AGGIUNTO: ESTRAZIONE COMMENTI CON OPENPYXL ---
                    st.write(f"Estrazione commenti da `{file_name}`...")
                    workbook = openpyxl.load_workbook(file_path)
                    sheet = workbook.active
                    
                    comments_map = {}
                    # Itera sulle celle della riga di intestazione specificata
                    for cell in sheet[header_row]:
                        if cell.comment and cell.value:
                            # Pulisce il nome della colonna e il testo del commento
                            sanitized_header = sanitize_column_name(cell.value)
                            # --- INIZIO BLOCCO DI PULIZIA COMMENTO ---
                            raw_text = cell.comment.text
                            
                            # Cerca la posizione dei primi due punti ":"
                            colon_position = raw_text.find(':')
                            
                            # Se li trova, prende solo il testo che viene DOPO. Altrimenti, prende tutto.
                            if colon_position != -1:
                                comment_text = raw_text[colon_position + 1:].strip()
                            else:
                                comment_text = raw_text.strip()
                            
                            comments_map[sanitized_header] = comment_text
                            # --- FINE BLOCCO DI PULIZIA COMMENTO ---
                                                
                    # Salva i commenti in un file JSON dedicato
                    comments_path = os.path.join(config["mapping_dir"], "appoggio_comments.json")
                    with open(comments_path, 'w', encoding='utf-8') as f:
                        json.dump(comments_map, f, indent=4)
                    
                    if comments_map:
                        st.success(f"Trovati e salvati {len(comments_map)} commenti.")
                    # --- FINE BLOCCO AGGIUNTO ---

                    # La logica originale per leggere i dati e salvarli nel DB rimane invariata
                    df = pd.read_excel(file_path, header=header_row - 1, dtype=str).fillna('')
                    pretty_name_map = {sanitize_column_name(col): str(col).strip() for col in df.columns}
                    df.columns = [sanitize_column_name(col) for col in df.columns]

                    table_name = f'{os.path.splitext(file_name)[0]}{config["db_appoggio_suffix"]}'
                    df.to_sql(table_name, engine, if_exists='replace', index=False)
                    st.success(f"Dati '{table_name}' importati con colonne sanificate.")

                    pretty_name_map_path = os.path.join(config["mapping_dir"], f"{table_name}_prettynames.json")
                    with open(pretty_name_map_path, 'w', encoding='utf-8') as f: json.dump(pretty_name_map, f, indent=4)
                    
                except Exception as e: st.error(f"Errore importando {file_name}: {e}")

# SOSTITUISCI INTERAMENTE LA TUA FUNZIONE CON QUESTA
def step_5_mappatura_globale(config, engine):
    mode_name = config['mode'].capitalize()
    mode = config['mode']
    st.header(f'Step 6: Mappatura Globale ({mode_name})')

    try:
        # --- 1. CARICAMENTO DATI E SETUP ---
        inspector = inspect(engine)
        all_tables = inspector.get_table_names()
        struttura_tables = sorted([t for t in all_tables if t.startswith(config["db_struttura_prefix"])])
        appoggio_tables = sorted([t for t in all_tables if t.endswith(config["db_appoggio_suffix"])])
        if not (struttura_tables and appoggio_tables):
            st.error(f'Importa tabelle Struttura e Appoggio per la modalità {mode_name}.'); return
        
        appoggio_table_name = appoggio_tables[0]
        source_cols_sanitized = pd.read_sql_table(appoggio_table_name, engine).columns.tolist()

        master_pretty_name_map = {}
        for table_name in (struttura_tables + appoggio_tables):
            path = os.path.join(config["mapping_dir"], f"{table_name}_prettynames.json")
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f: master_pretty_name_map.update(json.load(f))
        
        def get_pretty_name(s_name): return master_pretty_name_map.get(s_name, s_name)
        
        unique_dest_cols = set()
        for table in struttura_tables:
            cols = pd.read_sql(f'SELECT * FROM "{table}" LIMIT 0', engine).columns
            unique_dest_cols.update(cols)
        sorted_unique_cols = sorted(list(unique_dest_cols))
        dest_options = ["-- Non mappare --", "Nascondi"] + sorted_unique_cols

        # --- BLOCCO AGGIUNTO: CARICAMENTO COMMENTI ---
        comments_path = os.path.join(config["mapping_dir"], "appoggio_comments.json")
        comments_map = {}
        if os.path.exists(comments_path):
            with open(comments_path, 'r', encoding='utf-8') as f:
                comments_map = json.load(f)
        # --- FINE BLOCCO AGGIUNTO ---


        # --- 2. GESTIONE STATO CENTRALE ---
        live_mapping_state_key = f"live_mapping_state_{mode}"

        def update_live_mapping(source_key):
            widget_key = f"map_global_{source_key}_{mode}"
            if widget_key in st.session_state:
                st.session_state[live_mapping_state_key][source_key] = st.session_state[widget_key]

        if live_mapping_state_key not in st.session_state:
            st.session_state[live_mapping_state_key] = {}
            
            loaded_mapping_data = st.session_state.get('loaded_template_data', {})
            if not loaded_mapping_data:
                mapping_path = os.path.join(config["mapping_dir"], "global_mapping.json")
                if os.path.exists(mapping_path):
                    with open(mapping_path, 'r', encoding='utf-8') as f:
                        loaded_mapping_data.setdefault("column_mappings", json.load(f))

            current_mapping = loaded_mapping_data.get('column_mappings', {})
            for col in source_cols_sanitized:
                source_key = f"{appoggio_table_name}.{col}"
                st.session_state[live_mapping_state_key][source_key] = current_mapping.get(source_key, dest_options[0])

        # --- 3. GESTIONE TEMPLATE (UI) ---
        st.subheader("Gestione Template di Mappatura")
        templates_dir = os.path.join(config["mapping_dir"], "templates")
        os.makedirs(templates_dir, exist_ok=True)
        saved_templates = ["-- Non caricare nulla --"] + sorted([os.path.splitext(f)[0].replace('_', ' ') for f in os.listdir(templates_dir) if f.endswith('.json')])
        
        # Determina l'indice del template caricato per il selectbox
        loaded_name_for_display = st.session_state.get('loaded_template_name')
        index = saved_templates.index(loaded_name_for_display) if loaded_name_for_display in saved_templates else 0
        st.selectbox("Carica un Template:", options=saved_templates, index=index, key=f"template_loader_{mode}", on_change=load_template_callback, args=(config,))
        st.markdown("---")

        # --- 4. INTERFACCIA DI MAPPATURA ---
        st.subheader("Mappatura Colonne Dati")
        # Inizializzazione stati per i filtri
        if f'hide_unmapped_{mode}' not in st.session_state: st.session_state[f'hide_unmapped_{mode}'] = False
        if f'hide_hidden_{mode}' not in st.session_state: st.session_state[f'hide_hidden_{mode}'] = False 
        
        st.checkbox("Nascondi colonne non mappate", key=f'hide_unmapped_{mode}')
        st.checkbox("Nascondi colonne impostate su 'Nascondi'", key=f'hide_hidden_{mode}')
        st.markdown("---")

        cols_to_display = source_cols_sanitized
        if st.session_state[f'hide_unmapped_{mode}']:
            cols_to_display = [c for c in cols_to_display if st.session_state[live_mapping_state_key].get(f"{appoggio_table_name}.{c}") != '-- Non mappare --']
        if st.session_state[f'hide_hidden_{mode}']:
            cols_to_display = [c for c in cols_to_display if st.session_state[live_mapping_state_key].get(f"{appoggio_table_name}.{c}") != 'Nascondi']

        # Logica di paginazione
        page_key = f'mapping_page_{mode}'
        if page_key not in st.session_state: st.session_state[page_key] = 0
        items_per_page = 15
        total_pages = (len(cols_to_display) + items_per_page - 1) // items_per_page
        st.session_state[page_key] = min(st.session_state[page_key], max(0, total_pages - 1))
        start_index, end_index = st.session_state[page_key] * items_per_page, (st.session_state[page_key] + 1) * items_per_page
        paginated_cols = cols_to_display[start_index:end_index]

        st.write(f"**Sorgente: {get_pretty_name(appoggio_table_name)}**")
        
        for col in paginated_cols: # Rendering dei widget
            source_key = f"{appoggio_table_name}.{col}"
            default_sel = st.session_state[live_mapping_state_key].get(source_key, dest_options[0])
            default_idx = dest_options.index(default_sel) if default_sel in dest_options else 0

            # --- INIZIO BLOCCO COMMENTI INTEGRATO ---
            pretty_label = get_pretty_name(col)
            comment_text = comments_map.get(col) # Cerca il commento per la colonna sanificata

            # Aggiungi un'icona 💬 se il commento esiste
            label_to_show = f"`{pretty_label}` → 💬" if comment_text else f"`{pretty_label}` →"

            # Crea il widget selectbox usando l'etichetta e l'aiuto a comparsa
            scelta = st.selectbox(
                label_to_show,
                options=dest_options,
                index=default_idx,
                key=f"map_global_{source_key}_{mode}", # Manteniamo la tua chiave originale
                format_func=get_pretty_name,
                on_change=update_live_mapping, # Manteniamo la tua callback originale
                args=(source_key,),            # Manteniamo i tuoi args originali
                help=comment_text # <-- Mostra il commento completo al passaggio del mouse
            )
            # La logica per 'new_mapping' non è nel tuo snippet, ma andrebbe qui
            # if scelta != "-- Non mappare --":
            #     new_mapping[source_key] = scelta
            # --- FINE BLOCCO COMMENTI INTEGRATO ---

        if total_pages > 1: # Navigazione di pagina
            st.markdown("---")
            c1,c2,c3 = st.columns([2,3,2]); prev_page = lambda: st.session_state.__setitem__(page_key, st.session_state[page_key] - 1); next_page = lambda: st.session_state.__setitem__(page_key, st.session_state[page_key] + 1)
            if st.session_state[page_key] > 0: c1.button("⬅️ Prec.", on_click=prev_page)
            c2.write(f"<div style='text-align: center;'>Pagina {st.session_state[page_key] + 1} di {total_pages}</div>", unsafe_allow_html=True)
            if st.session_state[page_key] < total_pages - 1: c3.button("Succ. ➡️", on_click=next_page)
        
        st.markdown("---")

        # --- 5. IMPOSTAZIONI AGGIUNTIVE (REINTEGRATE) ---
        st.subheader("Impostazioni Aggiuntive")
        loaded_data = st.session_state.get('loaded_template_data', {})
        
        # Colonne Data
        default_dates = loaded_data.get('date_format_columns', [])
        valid_default_dates = [c for c in default_dates if c in sorted_unique_cols]
        selected_date_cols = st.multiselect("Colonne da formattare come Data (gg/mm/aaaa):", 
                                            options=sorted_unique_cols, default=valid_default_dates, 
                                            key=f'date_cols_selector_{mode}', format_func=get_pretty_name)
        # Colonna Codice Studio
        studio_opts = ["-- Non applicare --"] + sorted_unique_cols
        default_studio = loaded_data.get('studio_code_column', studio_opts[0])
        default_studio_idx = studio_opts.index(default_studio) if default_studio in studio_opts else 0
        selected_studio_col = st.selectbox("Colonna per Codice Studio:", options=studio_opts, index=default_studio_idx,
                                           key=f'studio_col_selector_{mode}', format_func=get_pretty_name)
        
        st.markdown("---")



        # --- 6. AZIONI SUL TEMPLATE E SALVATAGGIO (REINTEGRATE) ---
        
        # Raccoglie tutti i dati correnti pronti per essere salvati
        current_full_mapping_data = {
            "column_mappings": {k: v for k, v in st.session_state[live_mapping_state_key].items() if v not in ["-- Non mappare --", "Nascondi"]},
            "date_format_columns": selected_date_cols,
            "studio_code_column": selected_studio_col if selected_studio_col != "-- Non applicare --" else ""
        }

        with st.expander("Azioni su Template e Salvataggio Sessione"):
            st.write("Usa queste opzioni per salvare le tue impostazioni per un uso futuro o per la sessione corrente.")
            
            # Salva come Nuovo Template
            template_name_input = st.text_input("Nome per nuovo template:", key=f"template_name_input_{mode}")
            if st.button("Salva come Nuovo Template", key=f"save_as_template_btn_{mode}"):
                if template_name_input.strip():
                    safe_filename = "".join(c for c in template_name_input if c.isalnum() or c in (' ', '_')).rstrip().replace(' ', '_')
                    template_path = os.path.join(templates_dir, f"{safe_filename}.json")
                    with open(template_path, 'w', encoding='utf-8') as f:
                        json.dump(current_full_mapping_data, f, indent=4)
                    st.success(f"Template '{template_name_input}' salvato!"); st.rerun()
                else: 
                    st.error("Inserisci un nome per il template.")

            # Aggiorna Template Esistente
            if st.button("Aggiorna Template Caricato", key=f"update_template_btn_{mode}", disabled=(not loaded_name_for_display)):
                safe_filename = "".join(c for c in loaded_name_for_display if c.isalnum() or c in (' ', '_')).rstrip().replace(' ', '_')
                template_path = os.path.join(templates_dir, f"{safe_filename}.json")
                with open(template_path, 'w', encoding='utf-8') as f:
                    json.dump(current_full_mapping_data, f, indent=4)
                st.success(f"Template '{loaded_name_for_display}' aggiornato!")

            st.markdown("---")
            # Salva per la sessione corrente
            if st.button("Salva Impostazioni e Procedi", type="primary", key=f'salva_sessione_btn_{mode}'):
                # Salva i file separati usati dagli step successivi
                with open(os.path.join(config["mapping_dir"], "global_mapping.json"), 'w', encoding='utf-8') as f:
                    json.dump(current_full_mapping_data['column_mappings'], f, indent=4)
                
                with open(os.path.join(config["mapping_dir"], "date_columns.json"), 'w', encoding='utf-8') as f:
                    json.dump({"date_columns": current_full_mapping_data['date_format_columns']}, f, indent=4)

                with open(os.path.join(config["mapping_dir"], "studio_mapping.json"), 'w', encoding='utf-8') as f:
                    json.dump({"codice_studio_column": current_full_mapping_data['studio_code_column']}, f, indent=4)
                
                st.success("Impostazioni di sessione salvate! Puoi procedere allo step successivo.")
                # Pulisce lo stato per assicurare un ricaricamento pulito se si torna indietro
                if live_mapping_state_key in st.session_state: del st.session_state[live_mapping_state_key]

    except Exception as e: 
        st.error(f"Errore critico durante la mappatura: {e}"); st.exception(e)

# SOSTITUISCI IL TUO STEP 5B CON QUESTA VERSIONE INTERATTIVA
def step_5b_verifica_trasformazioni(config, engine):
    st.header("Step 6b: Configura Colonne Chiave per Trasformazioni")
    st.info("Questo step analizza la mappatura. Se rileva una trasformazione '1 a molti', ti permette di scegliere quali colonne chiave ripetere su ogni riga.")

    try:
        # Carica la mappatura creata nello step precedente
        mapping_path = os.path.join(config["mapping_dir"], "global_mapping.json")
        if not os.path.exists(mapping_path):
            st.warning("Esegui prima la mappatura allo Step 6 e salva le impostazioni.")
            return
        with open(mapping_path, 'r', encoding='utf-8') as f:
            global_mapping = json.load(f)

        # Carica i nomi leggibili per un output più chiaro
        inspector = inspect(engine)
        all_tables = inspector.get_table_names()
        master_pretty_name_map = {t:t for t in all_tables}
        for table_name in all_tables:
            path = os.path.join(config["mapping_dir"], f"{table_name}_prettynames.json")
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f: master_pretty_name_map.update(json.load(f))
        def get_pretty_name(s_name): return master_pretty_name_map.get(s_name, s_name)

        # Analizza le mappature per trovare le tabelle che necessitano di configurazione
        struttura_tables = sorted([t for t in all_tables if t.startswith(config["db_struttura_prefix"])])
        unpivot_tables_info = {}
        for table_name in struttura_tables:
            table_dest_cols = pd.read_sql(f'SELECT * FROM "{table_name}" LIMIT 0', engine).columns.tolist()
            current_mapping = {k: v for k, v in global_mapping.items() if v in table_dest_cols}
            if not current_mapping: continue
            
            dest_to_sources = {dest: [s for s, d in current_mapping.items() if d == dest] for dest in set(current_mapping.values())}
            if any(len(sources) > 1 for sources in dest_to_sources.values()):
                one_to_one_cols = [k for k,v in dest_to_sources.items() if len(v) == 1]
                unpivot_tables_info[table_name] = {"triggers": {k:v for k,v in dest_to_sources.items() if len(v)>1}, "key_options": one_to_one_cols}

        st.markdown("---")
        
        if not unpivot_tables_info:
            st.success("✅ Nessuna trasformazione 'molti-a-uno' rilevata. Puoi procedere allo step successivo.")
            return

        st.warning(f"⚠️ Rilevata una trasformazione 'molti-a-uno' per {len(unpivot_tables_info)} tabella/e.")
        
        # Carica/Inizializza la configurazione delle chiavi
        unpivot_keys_path = os.path.join(config["mapping_dir"], "unpivot_keys_config.json")
        if 'unpivot_keys_config' not in st.session_state:
            st.session_state.unpivot_keys_config = json.load(open(unpivot_keys_path)) if os.path.exists(unpivot_keys_path) else {}

        # Mostra l'interfaccia di configurazione
        for table_name, info in unpivot_tables_info.items():
            with st.expander(f"**Configura le chiavi per: `{get_pretty_name(table_name)}`**"):
                st.write("Causa della trasformazione:")
                for dest_col, source_keys in info["triggers"].items():
                     st.markdown(f"- La colonna **`{get_pretty_name(dest_col)}`** è mappata da {len(source_keys)} sorgenti.")

                st.markdown("---")
                st.write("**Azione richiesta:** Scegli quali colonne (tra quelle mappate 1-a-1) vuoi ripetere su ogni riga creata.")
                st.caption("Se non selezioni nulla, verranno usate tutte le colonne mappate 1-a-1 (comportamento di default).")

                key_options = info["key_options"]
                if not key_options:
                    st.warning("Questa tabella non ha colonne con mappatura 1-a-1 da usare come chiave.")
                    st.session_state.unpivot_keys_config[table_name] = []
                    continue

                default_keys = st.session_state.unpivot_keys_config.get(table_name, [])
                valid_defaults = [k for k in default_keys if k in key_options]

                selected_keys = st.multiselect(
                    "Colonne chiave da ripetere:",
                    options=key_options,
                    default=valid_defaults,
                    key=f"unpivot_keys_{table_name}",
                    format_func=get_pretty_name
                )
                st.session_state.unpivot_keys_config[table_name] = selected_keys

        if st.button("Salva Configurazione Colonne Chiave"):
            with open(unpivot_keys_path, 'w', encoding='utf-8') as f:
                json.dump(st.session_state.unpivot_keys_config, f, indent=4)
            st.success("Configurazione salvata!")

    except Exception as e:
        st.error(f"Errore: {e}")

# SOSTITUISCI IL TUO STEP 6 CON QUESTA VERSIONE FINALE
# VERSIONE FINALE DI STEP 6 - Implementa la logica a tre livelli
def step_6_popola_dati(config, engine):
    mode_name = config['mode'].capitalize()
    st.header(f"Step 7: Popola Dati ({mode_name})")

    if st.button("APPLICA MAPPATURA E POPOLA", key=f'popola_btn_{mode_name}'):
        with st.spinner("Popolamento in corso..."):
            try:
                # 1. Caricamento Globale
                mapping_path = os.path.join(config["mapping_dir"], "global_mapping.json")
                if not os.path.exists(mapping_path): st.error(f"'global_mapping.json' non trovato."); return
                with open(mapping_path, 'r', encoding='utf-8') as f: global_mapping = json.load(f)
                
                unpivot_keys_path = os.path.join(config["mapping_dir"], "unpivot_keys_config.json")
                unpivot_keys_config = json.load(open(unpivot_keys_path)) if os.path.exists(unpivot_keys_path) else {}

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

                # 2. Ciclo di Esecuzione
                for struttura_table in struttura_tables:
                    st.write(f"--- Elaborazione per `{struttura_table}` ---")
                    
                    dest_cols_for_this_table = pd.read_sql(f'SELECT * FROM "{struttura_table}" LIMIT 0', engine).columns.tolist()
                    current_mapping = {k: v for k, v in global_mapping.items() if v in dest_cols_for_this_table}
                    df_popolato = pd.DataFrame()

                    if not current_mapping:
                        st.warning(f"Nessuna mappatura trovata per '{struttura_table}'.")
                    else:
                        dest_to_sources = {dest: [s.split('.')[1] for s, d in current_mapping.items() if d == dest] for dest in set(current_mapping.values())}
                        is_unpivot = any(len(sources) > 1 for sources in dest_to_sources.values())
                        
                        if is_unpivot:
                            st.info(f"Logica Rilevata: Trasformazione Wide-to-Long")
                            all_one_to_one_map = {dest: sources[0] for dest, sources in dest_to_sources.items() if len(sources) == 1}
                            unpivot_map = {dest: sources for dest, sources in dest_to_sources.items() if len(sources) > 1}
                            
                            # --- LOGICA IBRIDA FINALE ---
                            user_defined_keys = unpivot_keys_config.get(struttura_table, [])
                            
                            if user_defined_keys:
                                st.success(f"Trovata configurazione personalizzata: uso {len(user_defined_keys)} colonne chiave.")
                                key_cols_map = {k: v for k, v in all_one_to_one_map.items() if k in user_defined_keys}
                                context_cols_map = {k: v for k, v in all_one_to_one_map.items() if k not in user_defined_keys}
                            else:
                                st.warning("Nessuna chiave personalizzata definita. Uso tutte le mappature 1-a-1 come chiavi (comportamento di default).")
                                key_cols_map = all_one_to_one_map
                                context_cols_map = {}
                            
                            df_popolato = crea_righe_multiple(df_appoggio, key_cols_map, context_cols_map, unpivot_map)
                            # --- FINE LOGICA IBRIDA ---
                        else:
                            st.info("Logica Rilevata: Mappatura Semplice (1-a-1)")
                            rename_map = {v[0]: k for k, v in dest_to_sources.items() if v}
                            df_popolato = df_appoggio.rename(columns=rename_map)
                    
                    # Logica Codice Studio
                    if studio_target_col and codice_studio_value and studio_target_col in dest_cols_for_this_table:
                        if df_popolato.empty and len(df_appoggio) > 0:
                            df_popolato = pd.DataFrame(index=range(len(df_appoggio)))
                        df_popolato[studio_target_col] = codice_studio_value
                    
                    # 3. Salvataggio
                    if not df_popolato.empty:
                        df_popolato = df_popolato.reindex(columns=dest_cols_for_this_table).fillna('')
                        df_popolato.to_sql(struttura_table, engine, if_exists='replace', index=False)
                        st.success(f"Tabella `{struttura_table}` popolata con successo con {len(df_popolato)} righe.")
                    else:
                        st.warning(f"Nessun dato generato per `{struttura_table}`.")

            except Exception as e: st.error(f"Errore: {e}"); st.exception(e)

def step_7_modifica_massiva(config, engine):
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

# SOSTITUISCI LA VECCHIA FUNZIONE CON QUESTA VERSIONE CORRETTA
def step_8_export_globale(config, engine):
    mode_name = config['mode'].capitalize()
    mode = config['mode']
    st.header(f'Step 9: Export Globale Finale ({mode_name})')
    
    export_state_key = f'exported_file_paths_{mode}'
    if export_state_key not in st.session_state: 
        st.session_state[export_state_key] = None

    # --- MODIFICA 1: Sposta il checkbox qui in alto ---
    # In questo modo viene sempre visualizzato, permettendo all'utente di impostarlo
    # prima di avviare l'export o tra un export e l'altro.
    st.info("Imposta le opzioni desiderate prima di avviare l'export.")
    st.checkbox(
        "Rimuovi colonne vuote dall'export", 
        value=True, 
        key=f"export_remove_empty_cols_{mode}",
        help="Se selezionato, le colonne che non contengono alcun dato (oltre alle intestazioni) non verranno incluse nel file Excel finale."
    )
    st.markdown("---")

    # Ora gestiamo la visualizzazione dei download o del bottone di avvio
    if st.session_state.get(export_state_key):
        # --- BLOCCO VISUALIZZAZIONE DOWNLOAD (invariato) ---
        st.success(f"Export completato con successo. {len(st.session_state[export_state_key])} file sono pronti.")
        for f_path in st.session_state[export_state_key]:
            file_name = os.path.basename(f_path)
            if os.path.exists(f_path):
                with open(f_path, 'rb') as f:
                    st.download_button(
                        f"⬇️ Scarica {file_name}", 
                        f.read(), 
                        file_name, 
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
                        key=f"dl_{file_name}_{mode}"
                    )
            else:
                st.error(f"File di export '{file_name}' non trovato. Riprova l'export.")
        
        if len(st.session_state[export_state_key]) > 1:
            st.markdown("---")
            with st.spinner("Creazione Archivio ZIP..."):
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, "w") as zipf:
                    for f_path in st.session_state[export_state_key]:
                        if os.path.exists(f_path): 
                            zipf.write(f_path, arcname=os.path.basename(f_path))
                st.download_button("📦 Scarica tutto (ZIP)", zip_buffer.getvalue(), f"export_{mode}.zip", "application/zip", key=f"dl_zip_btn_{mode}")
                
        st.markdown("---")
        if st.button("Esegui un nuovo export", key=f"clear_export_btn_{mode}"):
            st.session_state[export_state_key] = None
            st.rerun()
            
    else:
        # --- BLOCCO AVVIO EXPORT ---
        # Il checkbox è già stato disegnato sopra, qui mettiamo solo il bottone
        if st.button("AVVIA EXPORT FINALE", key=f'start_final_export_btn_{mode}', type="primary"):
            with st.spinner("Creazione file in corso..."):
                try:
                    # --- La logica interna rimane la stessa ---
                    date_format_path = os.path.join(config["mapping_dir"], "date_columns.json"); colonne_data = []
                    if os.path.exists(date_format_path):
                        with open(date_format_path, 'r', encoding='utf-8') as f: 
                            colonne_data = json.load(f).get("date_columns", [])
                    
                    inspector = inspect(engine)
                    struttura_tables = [t for t in inspector.get_table_names() if t.startswith(config["db_struttura_prefix"])]
                    if not struttura_tables: 
                        st.warning("Nessuna tabella dati da esportare trovata.")
                        st.stop()

                    generated_paths = []
                    for struttura_table in struttura_tables:
                        base_name = struttura_table.replace(config["db_struttura_prefix"], '')
                        st.write(f"Elaborazione di `{base_name}`...")

                        header_map_path = os.path.join(config["mapping_dir"], f"{struttura_table}_headers.json"); header_map = {}
                        if os.path.exists(header_map_path):
                            with open(header_map_path, 'r', encoding='utf-8') as f: header_map = json.load(f)
                        else:
                            st.error(f"Mappa intestazioni per {struttura_table} non trovata."); continue

                        df_to_export = pd.read_sql_table(struttura_table, engine)
                        
                        df_final_for_export = df_to_export.copy()
                        if st.session_state.get(f"export_remove_empty_cols_{mode}", False):
                            if not df_to_export.empty:
                                cols_to_drop = [
                                    col for col in df_to_export.columns 
                                    if df_to_export[col].astype(str).str.strip().eq('').all()
                                ]
                                
                                if cols_to_drop:
                                    df_final_for_export = df_to_export.drop(columns=cols_to_drop)
                                    st.info(f"In '{base_name}', rimosse {len(cols_to_drop)} colonne vuote.")
                            else:
                                st.warning(f"La tabella '{base_name}' è vuota, l'export per questo file sarà vuoto.")
                        
                        for col in colonne_data:
                            if col in df_final_for_export.columns:
                                df_final_for_export[col] = pd.to_datetime(df_final_for_export[col], errors='coerce').dt.strftime('%d/%m/%Y').fillna('')
                        
                        dest_cols = list(df_final_for_export.columns)
                        numeric_headers_row = [header_map.get(col, '') for col in dest_cols]
                        
                        wb_export = openpyxl.Workbook()
                        ws_export = wb_export.active
                        
                        ws_export.append(["Non modificare questa riga", base_name.upper()])
                        ws_export.append(["Non modificare questa riga"] + numeric_headers_row)
                        ws_export.append(["Non modificare questa riga"] + dest_cols)
                        
                        for row_data_tuple in df_final_for_export.itertuples(index=False, name=None):
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

step_labels = ['Impostazioni', 'Carica Struttura', 'Importa Struttura', 'Carica Dati', 'Importa Dati', 'Mappatura', 'Verifica Trasformazioni','Popola Dati', 'Modifica Massiva', 'Export Globale']
step_functions = [
    step_0_impostazioni, step_1_upload_struttura, step_2_import_struttura,
    step_3_upload_appoggio, step_4_import_appoggio, step_5_mappatura_globale,
    step_5b_verifica_trasformazioni, # <-- NUOVO STEP INSERITO
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
    step_functions[step_idx](config, engine)

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