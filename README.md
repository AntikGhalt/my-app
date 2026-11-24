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

## Dati scaricati

Copertura alla prima esecuzione completa (24 novembre 2026).

| File | Contenuto | Dataflow ISTAT | Periodo | Serie |
|---|---|---|---|---|
| `trimestrali/famiglie_conto_trimestrale` | Famiglie consumatrici: sequenza dei conti (trimestrale) | `162_1064_DF_DCCN_ISTITUZ_QNA1_1` | 1999-Q1 – 2026-Q2 | 44 |
| `trimestrali/famiglie_destagionalizzati_trimestrali` | Famiglie consumatrici: aggregati destagionalizzati (trimestrale) | `162_1064_DF_DCCN_ISTITUZ_QNA1_3` | 1999-Q1 – 2026-Q2 | 16 |
| `trimestrali/famiglie_indicatori_trimestrali` | Famiglie consumatrici: propensione al risparmio, potere d'acquisto e altri indicatori (trimestrale) | `162_1064_DF_DCCN_ISTITUZ_QNA1_2` | 1999-Q1 – 2026-Q2 | 6 |
| `trimestrali/consumi_famiglie_trimestrali` | Spesa per consumi finali delle famiglie (trimestrale) | `163_1226_DF_DCCN_QNA1_3` | 1995-Q1 – 2026-Q2 | 198 |
| `trimestrali/consumi_coicop_trimestrali` | Spesa delle famiglie per voce di spesa (COICOP 2 cifre) e durata (trimestrale) | `163_1226_DF_DCCN_QNA1_4` | 1995-Q1 – 2026-Q2 | 171 |
| `trimestrali/popolazione_trimestrale` | Popolazione media del periodo (trimestrale) | `163_88_DF_DCCN_OCCQSEC2010_2` | 1995-Q1 – 2026-Q2 | 1 |
| `annuali/famiglie_conto_annuale` | Famiglie consumatrici: sequenza dei conti (annuale) | `94_1063_DF_DCCN_ISTITUZ_ANA1_1` | 1995 – 2025 | 181 |
| `annuali/famiglie_indicatori_annuali` | Famiglie consumatrici: propensione al risparmio, potere d'acquisto e altri indicatori (annuale) | `94_1063_DF_DCCN_ISTITUZ_ANA1_2` | 1995 – 2025 | 3 |
| `annuali/consumi_coicop_annuali` | Spesa delle famiglie per voce di spesa (COICOP 3 cifre) e durata (annuale) | `92_1225_DF_DCCN_ANA1_6` | 1995 – 2025 | 276 |
| `annuali/valori_pro_capite_annuali` | PIL, reddito e consumi per abitante (annuale) | `92_1225_DF_DCCN_ANA1_7` | 1995 – 2025 | 29 |
| `annuali/popolazione_annuale` | Popolazione media annua | `92_507_DF_DCCN_OCCNSEC2010_3` | 1995 – 2025 | 1 |
| `mensili/nic_tipologie_mensili` | NIC per tipologia di prodotto, Italia e ripartizioni (mensile, base 2025) | `167_745_DF_DCSP_NIC1B2025_2` | 2026-01 – 2026-10 | 57 |
| `mensili/nic_tipologie_mensili_base2015` | NIC per tipologia di prodotto, Italia e ripartizioni (mensile 2016-2025, base 2015) | `167_744_DF_DCSP_NIC1B2015_2` | 2016-01 – 2025-12 | 285 |
| `mensili/nic_ecoicop_mensili` | NIC per voce ECOICOP 2, numeri indici (mensile, tutte le basi) | `167_745_DF_DCSP_NIC1B2025_6` | 1996-01 – 2026-10 | 2413 |
| `annuali/nic_medie_annue` | NIC medie annue per voce ECOICOP 2 (1996-2025) | `167_747_DCSP_NIC2B2025_1` | 1996 – 2025 | 2938 |

Per tutti i conti nazionali si usa l'ultima edizione completa (a novembre 2026: 2026M10 per i trimestrali e i conti annuali per settore, 2026M9 per gli altri annuali). Il settore è sempre S14A, *famiglie consumatrici*.

## Avvio rapido

Serve Python 3.11 o più recente.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
istatsync run
```

La prima esecuzione è lenta, circa 40 minuti: per ogni dataflow va scaricata
la struttura, che ISTAT serve in uno o due minuti, e tra due richieste passano
almeno 13 secondi (vedi sotto). Le strutture restano in cache per 7 giorni. Le
esecuzioni successive richiedono 5-10 minuti (24 richieste per l'intero
catalogo) e scaricano per intero solo i dataset che hanno una nuova
edizione.

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

**Le basi del NIC non sono raccordate.** Il dataflow "tutte le basi"
(`167_745_DF_DCSP_NIC1B2025_6`) non contiene una serie unica ma quattro serie
distinte, una per base (1995, 2010, 2015, 2025), distinte dal codice
`DATA_TYPE` e con etichette identiche fino al trentesimo carattere. Nell'Excel
sono su fogli separati; per una serie lunga vanno raccordate con i
coefficienti ISTAT.

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

## Dalla versione 1

| File v1 | File v2 | Cosa cambia |
|---|---|---|
| `ISTAT_Reddito_disponibile_famiglie_LATEST.xlsx` | `trimestrali/famiglie_conto_trimestrale` | tutta la sequenza dei conti S14A (44 voci invece di 14); destagionalizzati in `famiglie_destagionalizzati_trimestrali` |
| `ISTAT_Consumi_famiglie_LATEST.xlsx` | `trimestrali/consumi_famiglie_trimestrali` | anche le voci COICOP e i dati corretti per i giorni lavorativi |
| `ISTAT_NIC_ECOICOP_LATEST.xlsx` | `mensili/nic_ecoicop_mensili` | ECOICOP 2, base 2025, storia dal 1996; periodi in riga invece che in colonna |
| `ISTAT_NIC_Tipologia_LATEST.xlsx` | `mensili/nic_tipologie_mensili` (+ `_base2015`) | base 2025 dal 2026; la base 2015 resta come serie chiusa |

Nel conto per settore istituzionale ISTAT usa la stessa etichetta per la
risorsa e per l'impiego (ad esempio "contributi sociali netti" compare due
volte). Il CSV ha una colonna `FLUSSO` (risorse / impieghi / saldi, ricavata dal
codice SEC: `_C_`, `_D_`, `_B_`) e nell'Excel le intestazioni iniziano con
"Risorse - ", "Impieghi - ", "Saldi - ". Se due serie hanno comunque la stessa
intestazione viene aggiunto il codice tra parentesi quadre.

I file Excel v1 non vanno spostati: il nuovo sistema non li legge. Il primo
giro crea i file nuovi accanto e da lì in poi tiene traccia delle versioni nel
`manifest.json`.

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
