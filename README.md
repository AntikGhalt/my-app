# istatsync

Scarica dalle API SDMX dell'ISTAT le serie che servono per il **conto delle
famiglie** (reddito, consumi, risparmio, prezzi, popolazione), sia trimestrali
sia annuali, e le pubblica come CSV ed Excel su una cartella locale o su
Google Drive. A ogni nuova edizione ISTAT i file vengono aggiornati e la
versione precedente finisce in archivio.

```
istatsync run            # scarica tutto quello che è cambiato
istatsync run --frequency Q
istatsync list           # cosa c'è nel catalogo
istatsync describe 163_1226_DF_DCCN_QNA1_3 --codes DATA_TYPE_AGGR
```

<!-- DATASETS -->

## Avvio rapido

Serve Python 3.11 o più recente.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
istatsync run
```

La prima esecuzione è lenta (circa 20 minuti): per ogni dataflow va scaricata
la struttura, che ISTAT serve in un minuto o più, e tra due richieste
passano almeno 13 secondi (vedi sotto). Le strutture restano in cache per 7
giorni, e le esecuzioni successive scaricano solo i dataset che hanno una
nuova edizione.

I file finiscono in `data/`:

```
data/
├── trimestrali/
│   ├── consumi_famiglie_trimestrali.xlsx
│   ├── consumi_famiglie_trimestrali.csv
│   └── ...
├── annuali/
├── mensili/
├── archivio/<dataset>/<dataset>_<data>_<edizione>.xlsx|csv
├── log/run_log.csv          # una riga per dataset per esecuzione
├── manifest.json            # cosa è pubblicato, edizione, hash dei dati
└── .cache/                  # strutture SDMX
```

Ogni dataset produce due file con lo stesso contenuto:

* **CSV, formato lungo**: una riga per osservazione, con codici ed etichette di
  ogni dimensione, `TIME_PERIOD`, `DATE`, `YEAR`, `QUARTER`/`MONTH`,
  `OBS_VALUE` e gli attributi (`UNIT_MEAS`, `UNIT_MULT`, `OBS_STATUS`...). È il
  formato da usare in Tableau, pandas, R.
* **Excel, formato largo**: periodi in riga e serie in colonna, un foglio per
  ogni valore di `sheets_by` (di solito grezzi / destagionalizzati), più un
  foglio `Info` (edizione, periodo coperto, URL della query) e un foglio
  `Serie` con i codici di ogni colonna.

Il nome dei file non cambia tra un aggiornamento e l'altro, quindi le
connessioni di Tableau e i collegamenti in Drive restano validi.

## Configurazione

Tutto si imposta con variabili d'ambiente (o con un file `.env`, vedi
`.env.example`). Nel codice non ci sono id di cartelle, progetti o account.

| Variabile | Default | |
|---|---|---|
| `ISTATSYNC_OUTPUT_DIR` | `data` | cartella locale di output |
| `ISTATSYNC_DRIVE_FOLDER_ID` | – | se impostata, i file vanno su questa cartella Drive invece che in locale |
| `ISTATSYNC_CATALOG` | `datasets.toml` | catalogo dei dataset |
| `ISTATSYNC_MIN_REQUEST_INTERVAL` | `13` | secondi tra due richieste a ISTAT |
| `ISTATSYNC_REQUEST_TIMEOUT` | `300` | timeout di una richiesta, in secondi |
| `ISTATSYNC_MAX_RETRIES` | `3` | tentativi su errori 5xx o di rete |
| `ISTATSYNC_STRUCTURE_TTL_DAYS` | `7` | durata della cache delle strutture |
| `ISTATSYNC_BASE_URL` | `https://esploradati.istat.it/SDMXWS/rest` | endpoint SDMX |
| `ISTATSYNC_TIMEZONE` | `Europe/Rome` | fuso per date di log e archivio |

## Aggiungere un dataset

I dataset sono descritti in [`datasets.toml`](datasets.toml). Per aggiungerne
uno:

1. trova il dataflow su [esploradati.istat.it](https://esploradati.istat.it/)
   (l'id è nell'URL, tipo `163_1226_DF_DCCN_QNA1_3`);
2. guarda dimensioni e codici:
   `istatsync describe 163_1226_DF_DCCN_QNA1_3 --codes ADJUSTMENT,VALUATION`;
3. aggiungi un blocco al catalogo:

```toml
[[dataset]]
id = "consumi_famiglie_trimestrali"      # nome dei file
title = "Spesa per consumi finali delle famiglie"
dataflow = "163_1226_DF_DCCN_QNA1_3"
frequency = "Q"                          # A, Q o M: decide la cartella
start = "1995"                           # opzionale
filter = { REF_AREA = "IT", VALUATION = ["V", "L_2020"] }
sheets_by = "ADJUSTMENT"                 # opzionale: un foglio Excel per valore
negate = ["*_D_*"]                       # opzionale: impieghi col segno meno (solo Excel)
edition = "2025M10"                      # opzionale: fissa un'edizione
```

Le dimensioni non elencate in `filter` vengono scaricate tutte. `FREQ` si
ricava da `frequency`. Se l'edizione non è indicata viene scelta in automatico
(vedi sotto). Ogni modifica a un blocco forza il riscaricamento di quel
dataset alla prossima esecuzione.

## Come funziona l'aggiornamento

Per ogni dataset:

1. **struttura**: dimensioni ed etichette dei codici, dalla cache se recente;
2. **edizione** (solo per i dataflow che hanno la dimensione `EDITION`, cioè i
   conti nazionali): una richiesta sugli ultimi tre anni lasciando `EDITION`
   vuota restituisce tutte le edizioni pubblicate; si sceglie la più recente
   *completa*. Se è la stessa già pubblicata ci si ferma qui, con una sola
   richiesta;
3. **dati**: una richiesta per tutta la serie storica dell'edizione scelta;
4. **confronto**: si calcola un hash di codici, periodi e valori. Se è uguale a
   quello pubblicato non si scrive niente. Per i dataflow senza edizione (i
   prezzi) è questo il controllo che coglie sia il nuovo mese sia le revisioni;
5. **pubblicazione**: la versione precedente viene copiata in `archivio/`, poi
   Excel e CSV vengono sovrascritti e il `manifest.json` aggiornato.

Un dataset che fallisce non blocca gli altri. Il processo esce con codice 1 se
almeno un dataset è fallito, così Cloud Run o la CI lo segnalano.

## Insidie dell'API ISTAT

Cose imparate a spese nostre, tutte gestite nel codice.

**Limite di 5 richieste al minuto.** Oltre questo limite ISTAT blocca l'IP per
uno o due giorni ([fonte](https://www.istat.it/classificazioni-e-strumenti/web-services-sdmx/)).
Il blocco si presenta come HTTP 429 o 403, oppure come connessioni che restano
appese. Il client tiene almeno 13 secondi tra l'inizio di due richieste e, al
primo 403/429, interrompe tutta l'esecuzione: riprovare peggiora le cose. Non
abbassare `ISTATSYNC_MIN_REQUEST_INTERVAL` sotto 12 e non lanciare due
esecuzioni in parallelo dallo stesso IP.

**Cercare l'edizione mese per mese fa scattare il blocco.** La versione
precedente provava `2025M11`, `2025M10`, `2025M9`... con una richiesta per
tentativo: fino a 25 richieste per dataset. Lasciando vuota la dimensione
`EDITION` ISTAT restituisce tutte le edizioni in una volta sola.

**I mesi delle edizioni non hanno lo zero davanti.** `2025M9` viene dopo
`2025M10` in ordine alfabetico, quindi le edizioni vanno confrontate come
(anno, mese) e non come stringhe.

**Edizioni parziali.** A volte un'edizione nuova contiene solo una parte delle
serie (stima flash, tavola aggiornata prima delle altre). Prenderla alla cieca
fa sparire colonne dai file. Si sceglie la più recente che abbia almeno l'80%
delle serie dell'edizione più completa, e nel log compare un avviso.

**URL troppo lunghi.** Oltre circa 350 caratteri l'API risponde HTTP 400. Se la
chiave è troppo lunga il client lascia vuota la dimensione con più codici
(cioè "tutti") e filtra le righe dopo il download. Per questo nel catalogo
conviene filtrare poco: una richiesta più grande costa meno di molte piccole.

**"Nessun dato" è un 404.** Una query senza risultati non restituisce un
dataset vuoto ma HTTP 404 con `NoRecordsFound`.

**Le strutture sono lente e pesanti.** `dataflow/.../?references=all` per i
conti nazionali impiega più di un minuto (la codelist `REF_AREA` ha oltre
12.000 codici). Per questo vanno in cache nella stessa destinazione dei file,
anche su Drive: un job Cloud Run parte ogni volta da un disco vuoto.

**Cambi di base dei prezzi.** Da gennaio 2026 il NIC è pubblicato in base 2025
con classificazione ECOICOP 2, in dataflow nuovi (`167_745_...`). I vecchi
dataflow in base 2015 (`167_744_...`) rispondono ancora ma si fermano a
dicembre 2025: una pipeline che punta lì "funziona" senza ricevere più dati
nuovi. Lo stesso succede ai conti nazionali con i dataflow `..._B19_...`
("edizioni ottobre 2019 – luglio 2024"), che sono archivi chiusi. Quando ISTAT
cambia base bisogna cambiare il `dataflow` nel catalogo.

**Formato.** Si chiede SDMX-CSV
(`Accept: application/vnd.sdmx.data+csv;version=1.0.0`) invece dell'XML
generico: è più leggero da scaricare e non richiede un parser fatto a mano. I
codici restano testo (`"001"` non diventa `1`), le colonne `NOTE_*` vengono
scartate. `UNIT_MULT` indica la potenza di dieci (6 = milioni di euro): i valori
non vengono riscalati.

**Endpoint.** `esploradati.istat.it/SDMXWS/rest` è quello che risponde in modo
stabile; `sdmx.istat.it` reindirizza. Se ISTAT lo cambia basta impostare
`ISTATSYNC_BASE_URL`.

## Pubblicazione su Google Drive

Con `ISTATSYNC_DRIVE_FOLDER_ID` impostata i file vengono scritti nella cartella
indicata (funziona anche con i Drive condivisi), con la stessa struttura di
sottocartelle. Le sottocartelle mancanti vengono create. I file esistenti
vengono aggiornati sul posto, quindi mantengono lo stesso id e lo stesso link.

Le credenziali sono le Application Default Credentials: in locale
`gcloud auth application-default login`, su Cloud Run il service account del
job. L'account deve avere il ruolo *Gestore contenuti* sulla cartella.

`istatsync run --local` scrive comunque in locale, utile per provare senza
toccare Drive.

## Deploy su Cloud Run

Il processo è un batch: gira, scrive, termina. Per questo è un **Cloud Run
Job** e non un servizio web. Rispetto alla versione precedente (servizio Flask
pubblico chiamato via HTTP):

* non c'è un URL pubblico che chiunque può chiamare, facendo scattare il
  blocco ISTAT;
* non c'è il timeout di 120 secondi di gunicorn, che interrompeva le pipeline
  lunghe a metà;
* ogni esecuzione ha log, durata ed esito (successo/errore) nella console.

Sostituisci i valori tra `<>` con quelli del tuo progetto:

```bash
gcloud run jobs deploy istatsync \
  --source . \
  --region <regione> \
  --task-timeout 3600 \
  --max-retries 0 \
  --memory 2Gi \
  --service-account <service-account> \
  --set-env-vars ISTATSYNC_DRIVE_FOLDER_ID=<id-cartella>
```

`--max-retries 0` è voluto: se ISTAT ci sta bloccando, riprovare subito
allunga il blocco.

Esecuzione giornaliera alle 7 con Cloud Scheduler:

```bash
gcloud scheduler jobs create http istatsync-daily \
  --location <regione> \
  --schedule "0 7 * * *" \
  --time-zone "Europe/Rome" \
  --http-method POST \
  --uri "https://run.googleapis.com/v2/projects/<progetto>/locations/<regione>/jobs/istatsync:run" \
  --oauth-service-account-email <service-account>
```

Esecuzione manuale e log:

```bash
gcloud run jobs execute istatsync --region <regione> --wait
gcloud run jobs executions list --job istatsync --region <regione>
```

Con il servizio precedente si eliminano servizio e vecchio job dello scheduler:
`gcloud run services delete istat-pipeline` e
`gcloud scheduler jobs delete istat-daily`.

## Sviluppo

```bash
pip install -e ".[dev]"
pytest            # nessuna chiamata di rete: ISTAT e Drive sono simulati
ruff check . && ruff format --check .
```

La CI (GitHub Actions) esegue lint e test su Python 3.11 e 3.13 a ogni push.

```
src/istatsync/
├── sdmx.py        client ISTAT: ritmo delle richieste, retry, chiavi, parsing
├── periods.py     edizioni e periodi
├── catalog.py     lettura e validazione di datasets.toml
├── transform.py   tabella lunga, tabella larga, CSV ed Excel
├── runner.py      scelta edizione, confronto, archivio, manifest, log
├── sinks/         destinazioni: cartella locale, Google Drive
├── settings.py    variabili d'ambiente
└── cli.py         comandi run / list / describe
```
