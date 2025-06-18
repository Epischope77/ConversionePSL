<!-- NOTA IMPORTANTE: -->
> **Per il corretto funzionamento, estrai lo ZIP mantenendo la struttura delle cartelle** (ad es. le cartelle `db`, `data`, `mapping`, `export`, `backup` devono trovarsi nella stessa posizione del file `app.py` e degli script di avvio). Se il database non viene trovato o la struttura delle cartelle è alterata, l'app mostrerà un messaggio d'errore.
> 
> **Avvia sempre l'applicazione tramite `run_app.bat` o `setup.bat` dalla cartella principale del progetto.**

# Migrazione Azienda/Dipendenti - Streamlit

## Requisiti
- Python 3.10+
- Consigliato: ambiente virtuale (venv)

## Installazione semplificata (Windows)

1. Scarica ed estrai lo ZIP su una cartella a piacere.
2. Fai doppio clic su `setup.bat` e segui le istruzioni a schermo.
   - Lo script controllerà la presenza di Python, installerà i requisiti e avvierà l'applicazione.
3. Se Python non è installato, lo script ti avviserà e ti indicherà dove scaricarlo.

## Installazione manuale (avanzata)

1. Installa Python 3.10+ dal sito ufficiale: https://www.python.org/downloads/
2. Apri il prompt dei comandi nella cartella del progetto.
3. Esegui:
   ```
   pip install -r requirements.txt
   run_app.bat
   ```

## Avvio su Linux/Mac

1. Installa Python 3.10+ e pip.
2. Apri il terminale nella cartella del progetto.
3. Esegui:
   ```
   pip install -r requirements.txt
   ./run_app.sh
   ```

L'applicazione sarà accessibile da browser all'indirizzo che verrà mostrato a schermo (di solito http://localhost:8501).

Per problemi o domande, consulta la sezione FAQ o contatta lo sviluppatore.

## Struttura cartelle
- `src/` : codice sorgente
- `data/` : file di input (struttura, appoggio)
- `db/` : database sqlite
- `export/` : file esportati
- `mapping/` : file di mappatura
- `backup/` : backup automatici

## Note
- Per problemi con le dipendenze, assicurati di avere installato anche i pacchetti di sistema necessari per `pandas`, `sqlalchemy`, `openpyxl`.
- Per assistenza: contatta lo sviluppatore.

## Backup automatico del progetto

Per creare un backup completo e portabile del progetto (escludendo file temporanei, venv, zip, pyc, ecc.):

1. Apri una finestra di PowerShell nella cartella principale del progetto.
2. Esegui:
   ```powershell
   powershell -ExecutionPolicy Bypass -File backup/crea_backup_progetto.ps1
   ```
3. Al termine troverai un file ZIP di backup nella cartella `backup/` (es: `progetto_backup_YYYYMMDD_HHMMSS.zip`).

Il backup include solo i file e le cartelle utili per la migrazione e può essere usato per ripristinare o trasferire il progetto su un altro PC.
