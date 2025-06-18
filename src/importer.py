import os
import pandas as pd
from sqlalchemy import create_engine, text
import json

# Percorsi cartelle
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', 'imported_data.sqlite')

# Funzione per importare un file (XLSX o CSV) e creare una tabella nel DB
def import_file_to_db(file_name, table_name=None, header_row=0, data_start_row=None):
    file_path = os.path.join(DATA_DIR, file_name)
    if file_name.endswith('.xlsx'):
        # header_row: indice della riga delle intestazioni (0-based)
        # data_start_row: indice della prima riga di dati (0-based)
        if data_start_row is not None and data_start_row > header_row + 1:
            skiprows = list(range(header_row + 1, data_start_row))
        else:
            skiprows = None
        df = pd.read_excel(file_path, header=header_row, skiprows=skiprows)
    elif file_name.endswith('.csv'):
        if data_start_row is not None and data_start_row > header_row + 1:
            skiprows = list(range(header_row + 1, data_start_row))
        else:
            skiprows = None
        df = pd.read_csv(file_path, header=header_row, skiprows=skiprows)
    else:
        raise ValueError('Formato file non supportato')

    if table_name is None:
        table_name = os.path.splitext(file_name)[0]

    engine = create_engine(f'sqlite:///{DB_PATH}')
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Tabella '{table_name}' creata/importata nel database con {len(df)} righe e {len(df.columns)} colonne.")
    print(f"Colonne: {list(df.columns)}\n")
    return table_name, list(df.columns)

def import_all_files_in_data():
    """Importa automaticamente tutti i file XLSX e CSV presenti nella cartella data/."""
    for file_name in os.listdir(DATA_DIR):
        if file_name.endswith('.xlsx') or file_name.endswith('.csv'):
            try:
                import_file_to_db(file_name)
            except Exception as e:
                print(f"Errore nell'importazione di {file_name}: {e}")

def import_selected_files():
    """Chiede all'utente i nomi dei file da importare (separati da virgola) e li importa."""
    print("File disponibili nella cartella data/:\n")
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.xlsx') or f.endswith('.csv')]
    for f in files:
        print(f"- {f}")
    selected = input("\nInserisci i nomi dei file da importare (separati da virgola): ")
    selected_files = [s.strip() for s in selected.split(',') if s.strip() in files]
    if not selected_files:
        print("Nessun file valido selezionato.")
        return []
    imported = []
    for file_name in selected_files:
        try:
            print(f"\nPer il file '{file_name}':")
            header_row = int(input("Numero riga intestazioni (1=prima riga): ")) - 1
            data_start_row = int(input("Numero riga inizio dati (1=prima riga): ")) - 1
            table_name, columns = import_file_to_db(file_name, header_row=header_row, data_start_row=data_start_row)
            imported.append((table_name, columns))
        except Exception as e:
            print(f"Errore nell'importazione di {file_name}: {e}")
    return imported

def list_tables_in_db():
    """Stampa l'elenco delle tabelle presenti nel database."""
    engine = create_engine(f'sqlite:///{DB_PATH}')
    with engine.connect() as conn:
        result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table';"))
        tables = [row[0] for row in result]
    print("Tabelle presenti nel database:")
    for t in tables:
        print(f"- {t}")
    return tables

def show_table_preview(table_name, n=5):
    """Mostra le prime n righe di una tabella del database."""
    engine = create_engine(f'sqlite:///{DB_PATH}')
    try:
        df = pd.read_sql_table(table_name, engine)
        print(f"\nAnteprima della tabella '{table_name}':")
        print(df.head(n))
    except Exception as e:
        print(f"Errore nella lettura della tabella {table_name}: {e}")

def show_table_columns(table_name):
    """Mostra le intestazioni delle colonne di una tabella del database."""
    engine = create_engine(f'sqlite:///{DB_PATH}')
    try:
        df = pd.read_sql_table(table_name, engine)
        print(f"\nColonne della tabella '{table_name}':")
        for col in df.columns:
            print(f"- {col}")
    except Exception as e:
        print(f"Errore nella lettura della tabella {table_name}: {e}")

def find_common_columns():
    """Trova e mostra le colonne comuni tra le tabelle del database (potenziali chiavi di collegamento)."""
    engine = create_engine(f'sqlite:///{DB_PATH}')
    tables = list_tables_in_db()
    columns_by_table = {}
    for t in tables:
        try:
            df = pd.read_sql_table(t, engine)
            columns_by_table[t] = set(df.columns)
        except Exception as e:
            print(f"Errore nella lettura della tabella {t}: {e}")
    # Trova colonne comuni
    all_columns = {}
    for table, cols in columns_by_table.items():
        for col in cols:
            all_columns.setdefault(col, set()).add(table)
    print("\nColonne presenti in più tabelle (potenziali chiavi di collegamento):")
    found = False
    for col, tables in all_columns.items():
        if len(tables) > 1:
            found = True
            print(f"- '{col}' presente in: {', '.join(tables)}")
    if not found:
        print("Nessuna colonna comune trovata tra le tabelle.")
    return all_columns

def select_and_save_relationships():
    """Permette all'utente di selezionare colonne chiave tra le tabelle e salva le relazioni in mapping/relazioni.json."""
    all_columns = find_common_columns()
    # Filtra solo colonne presenti in più tabelle
    candidate_keys = {col: list(tabs) for col, tabs in all_columns.items() if len(tabs) > 1}
    if not candidate_keys:
        print("\nNessuna colonna comune tra tabelle da collegare.")
        return
    print("\nSeleziona le relazioni tra le tabelle (chiavi di collegamento):")
    relationships = []
    for idx, (col, tables) in enumerate(candidate_keys.items(), 1):
        print(f"{idx}. Colonna '{col}' tra le tabelle: {', '.join(tables)}")
    print("\nPer ogni relazione che vuoi salvare, inserisci il numero corrispondente (separati da virgola). Premi invio per saltare.")
    selected = input("Numeri delle relazioni da salvare: ")
    selected_idx = [int(s.strip()) for s in selected.split(',') if s.strip().isdigit()]
    for i, (col, tables) in enumerate(candidate_keys.items(), 1):
        if i in selected_idx:
            relationships.append({"colonna": col, "tabelle": tables})
    if relationships:
        os.makedirs(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'mapping'), exist_ok=True)
        mapping_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'mapping', 'relazioni.json')
        with open(mapping_path, 'w', encoding='utf-8') as f:
            json.dump(relationships, f, ensure_ascii=False, indent=2)
        print(f"\nRelazioni salvate in {mapping_path}")
    else:
        print("\nNessuna relazione selezionata.")

def join_tables_on_key(mapping_path=None):
    """Esegue un esempio di join tra le tabelle collegate tramite la relazione salvata."""
    if mapping_path is None:
        mapping_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'mapping', 'relazioni.json')
    if not os.path.exists(mapping_path):
        print("Nessun file di relazioni trovato.")
        return
    with open(mapping_path, 'r', encoding='utf-8') as f:
        rels = json.load(f)
    if not rels:
        print("Nessuna relazione salvata.")
        return
    engine = create_engine(f'sqlite:///{DB_PATH}')
    for rel in rels:
        col = rel['colonna']
        tables = rel['tabelle']
        if len(tables) < 2:
            continue
        print(f"\nEsempio JOIN tra '{tables[0]}' e '{tables[1]}' sulla colonna '{col}':")
        try:
            df1 = pd.read_sql_table(tables[0], engine)
            df2 = pd.read_sql_table(tables[1], engine)
            if col in df1.columns and col in df2.columns:
                join_df = pd.merge(df1, df2, on=col, suffixes=(f'_{tables[0]}', f'_{tables[1]}'))
                print(join_df.head(10))
            else:
                print(f"Colonna '{col}' non trovata in entrambe le tabelle.")
        except Exception as e:
            print(f"Errore nel join: {e}")

# Esempio d'uso (decommenta per testare)
# import_file_to_db('esempio_ditta.xlsx')
# import_file_to_db('esempio_dipendente.csv')

if __name__ == "__main__":
    print("IMPORTAZIONE TABELLE AZIENDA (PRIMA FASE)")
    azienda_tables = import_selected_files()
    print("\nTabelle azienda importate:")
    for tname, cols in azienda_tables:
        print(f"- {tname}: {cols}")
    print("\nIMPORTAZIONE TABELLE DIPENDENTI (SECONDA FASE)")
    dipendenti_tables = import_selected_files()
    print("\nTabelle dipendenti importate:")
    for tname, cols in dipendenti_tables:
        print(f"- {tname}: {cols}")
    print()
    tables = list_tables_in_db()
    if tables:
        for t in tables:
            show_table_preview(t)
            show_table_columns(t)
        find_common_columns()
        select_and_save_relationships()
        join_tables_on_key()
