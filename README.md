# istatsync

Scarica le serie per la tabella sul **reddito disponibile delle famiglie**:
reddito e componenti, consumi e deflatore, ULA e redditi da lavoro dipendente,
popolazione e prezzi delle abitazioni dall'ISTAT, ricchezza finanziaria dalla
Banca d'Italia. Trimestrali e annuali. I file escono in CSV ed Excel, in una
cartella locale o su Google Drive; a ogni nuova edizione vengono aggiornati e
la versione precedente va in archivio.

```
istatsync run            # scarica tutto quello che è cambiato
istatsync run --frequency Q
istatsync list           # cosa c'è nel catalogo
istatsync describe 163_1226_DF_DCCN_QNA1_3 --codes DATA_TYPE_AGGR
```

## Dati scaricati

Il catalogo è pensato per la tabella sul reddito disponibile delle famiglie
(semestrale, con ricchezza, consumi e ULA). Copertura verificata il 24 novembre
2026 sulle API ISTAT e BCE.

| File | Contenuto | Fonte | Periodo | Serie |
|---|---|---|---|---|
| `trimestrali/famiglie_conto_trimestrale` | Famiglie consumatrici: sequenza dei conti (trimestrale) | ISTAT, `162_1064_DF_DCCN_ISTITUZ_QNA1_1` | 1999-Q1 – 2026-Q2 | 44 |
| `trimestrali/famiglie_destagionalizzati_trimestrali` | Famiglie consumatrici: aggregati destagionalizzati (trimestrale) | ISTAT, `162_1064_DF_DCCN_ISTITUZ_QNA1_3` | 1999-Q1 – 2026-Q2 | 16 |
| `trimestrali/famiglie_indicatori_trimestrali` | Famiglie consumatrici: propensione al risparmio, potere d'acquisto e altri indicatori (trimestrale) | ISTAT, `162_1064_DF_DCCN_ISTITUZ_QNA1_2` | 1999-Q1 – 2026-Q2 | 6 |
| `trimestrali/consumi_famiglie_trimestrali` | Spesa per consumi finali delle famiglie (trimestrale) | ISTAT, `163_1226_DF_DCCN_QNA1_3` | 1995-Q1 – 2026-Q2 | 198 |
| `trimestrali/consumi_coicop_trimestrali` | Spesa delle famiglie per voce di spesa (COICOP 2 cifre) e durata (trimestrale) | ISTAT, `163_1226_DF_DCCN_QNA1_4` | 1995-Q1 – 2026-Q2 | 171 |
| `trimestrali/popolazione_trimestrale` | Popolazione media del periodo (trimestrale) | ISTAT, `163_88_DF_DCCN_OCCQSEC2010_2` | 1995-Q1 – 2026-Q2 | 1 |
| `trimestrali/ula_trimestrali` | Unità di lavoro, occupati e ore lavorate per branca, dipendenti e totale (trimestrale) | ISTAT, `163_88_DF_DCCN_OCCQSEC2010_1` | 1995-Q1 – 2026-Q2 | 483 |
| `trimestrali/redditi_lavoro_trimestrali` | Redditi da lavoro dipendente, retribuzioni lorde e contributi per branca (trimestrale) | ISTAT, `163_1226_DF_DCCN_QNA1_5` | 1995-Q1 – 2026-Q2 | 210 |
| `trimestrali/ipab_trimestrale` | Prezzi delle abitazioni (IPAB), base 2025 (trimestrale, dal 2010) | ISTAT, `143_497_DF_DCSP_IPAB_6` | 2010-Q1 – 2026-Q2 | 9 |
| `trimestrali/ricchezza_finanziaria_trimestrale` | Famiglie e ISP: attività e passività finanziarie, consistenze e transazioni (Banca d'Italia, trimestrale) | Banca d'Italia (via BCE), `QSA` | 1999-Q1 – 2026-Q2 | 66 |
| `annuali/ula_annuali` | Unità di lavoro, occupati e ore lavorate per branca, dipendenti e totale (annuale) | ISTAT, `92_507_DF_DCCN_OCCNSEC2010_1` | 1995 – 2025 | 846 |
| `annuali/ipab_annuale` | Prezzi delle abitazioni (IPAB), base 2025, medie annue (dal 2010) | ISTAT, `143_497_DF_DCSP_IPAB_7` | 2010 – 2025 | 6 |
| `annuali/famiglie_conto_annuale` | Famiglie consumatrici: sequenza dei conti (annuale) | ISTAT, `94_1063_DF_DCCN_ISTITUZ_ANA1_1` | 1995 – 2025 | 181 |
| `annuali/famiglie_indicatori_annuali` | Famiglie consumatrici: propensione al risparmio, potere d'acquisto e altri indicatori (annuale) | ISTAT, `94_1063_DF_DCCN_ISTITUZ_ANA1_2` | 1995 – 2025 | 3 |
| `annuali/consumi_coicop_annuali` | Spesa delle famiglie per voce di spesa (COICOP 3 cifre) e durata (annuale) | ISTAT, `92_1225_DF_DCCN_ANA1_6` | 1995 – 2025 | 276 |
| `annuali/valori_pro_capite_annuali` | PIL, reddito e consumi per abitante (annuale) | ISTAT, `92_1225_DF_DCCN_ANA1_7` | 1995 – 2025 | 29 |
| `annuali/popolazione_annuale` | Popolazione media annua | ISTAT, `92_507_DF_DCCN_OCCNSEC2010_3` | 1995 – 2025 | 1 |
| `mensili/nic_tipologie_mensili` *(spento)* | NIC per tipologia di prodotto, Italia e ripartizioni (mensile, base 2025) | ISTAT, `167_745_DF_DCSP_NIC1B2025_2` | 2026-01 – 2026-10 | 57 |
| `mensili/nic_tipologie_mensili_base2015` *(spento)* | NIC per tipologia di prodotto, Italia e ripartizioni (mensile 2016-2025, base 2015) | ISTAT, `167_744_DF_DCSP_NIC1B2015_2` | 2016-01 – 2025-12 | 285 |
| `mensili/nic_ecoicop_mensili` *(spento)* | NIC per voce ECOICOP 2, numeri indici (mensile, tutte le basi) | ISTAT, `167_745_DF_DCSP_NIC1B2025_6` | 1996-01 – 2026-10 | 2413 |
| `annuali/nic_medie_annue` *(spento)* | NIC medie annue per voce ECOICOP 2 (1996-2025) | ISTAT, `167_747_DCSP_NIC2B2025_1` | 1996 – 2025 | 2938 |

I conti nazionali usano l'ultima edizione completa (a novembre 2026: 2026M10 per i
trimestrali e i conti annuali per settore, 2026M9 per gli altri annuali). Il reddito
è sempre del settore S14A, *famiglie consumatrici*; la ricchezza finanziaria è di
*famiglie e ISP* (S1M), come nelle tavole della Banca d'Italia.

I prezzi al consumo (NIC) sono spenti: non servono alla tabella e sono i download
più pesanti. Restano nel catalogo e si scaricano con `istatsync run nic_ecoicop_mensili`
o togliendo `enabled = false`.

### Controllo sui dati della tabella 1° semestre 2026

I valori scaricati coincidono con quelli controllati a mano:

| Voce | Valore |
|---|---|
| Reddito disponibile lordo (B6G), 1° sem. 2007 / 2019 / 2025, mln € | 537.232 / 566.145 / 690.209 |
| ULA dipendenti, II trim. 2026, migliaia (grezzi) | 18.473 (occupati: 20.857) |
| Attività finanziarie famiglie e ISP, dic. 2024, mln € | 6.033.659 |
| Transazioni II trim. 2026: attività / passività, mln € | 23.925 / 17.110 |
| IPAB II trim. 2026: congiunturale / tendenziale | +1,7% / +4,0% |
| IPAB media 2025 su 2024; 1° sem. 2026 su 1° sem. 2025 | +4,0% / +4,55% |

## Tabelle di sintesi

Dopo ogni aggiornamento vengono ricostruiti due file Excel in `tabelle/`, solo
se è cambiato almeno uno dei dati che usano (la versione precedente va in
`archivio/tabelle/`):

* `tabella_reddito_trimestrale.xlsx`: trimestri o semestri, dai dati
  trimestrali;
* `tabella_reddito_annuale.xlsx`: anni, dai dati annuali.

Nel foglio **Tabella** si scelgono la frequenza (solo nel trimestrale) e i
quattro periodi da confrontare con i menu a tendina nelle celle gialle;
l'ultimo periodo è il riferimento per le variazioni e per i prezzi. Le righe:
componenti del reddito disponibile (risorse meno impieghi, rettifica D8
compresa), reddito disponibile lordo, consumi delle famiglie residenti,
ricchezza finanziaria netta; gli stessi valori a prezzi dell'ultimo periodo;
valori per abitante e per ULA dipendente; deflatore, propensione al consumo e
una riga di controllo che verifica che le componenti tornino al reddito.

Tutto è calcolato con formule a partire dai fogli **Dati** (serie grezze
dell'ultima edizione, non toccate) e **Serie** (somme, medie e valori di fine
periodo per ogni trimestre, semestre o anno). Il foglio **Note** riporta
edizioni e definizioni. Un periodo con dati incompleti risulta `n.d.`; una
variazione tra valori di segno opposto `n.s.`.

Sul primo semestre 2026 i risultati coincidono con la tabella costruita a mano
(a prezzi del 1° sem. 2026, variazioni su 2007 / 2019 / 2025): reddito
disponibile −5,9 / +4,1 / +0,5; ricchezza finanziaria netta +13,7 / +29,1 /
+4,8; redditi da lavoro dipendente per ULA −0,1 / −2,3 / +0,7.

I file vanno aperti in Excel (o LibreOffice): i valori vengono calcolati
all'apertura, quindi l'anteprima di Google Drive può mostrare celle vuote. La
ricchezza immobiliare non c'è, perché esce solo nel rapporto annuale
ISTAT-Banca d'Italia.

Per ricostruirli senza scaricare nulla: `istatsync tabelle`.

## Avvio rapido

Serve Python 3.11 o più recente.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
istatsync run
```

La prima esecuzione è lenta, circa 40 minuti: per ogni dataflow va scaricata
la struttura, che ISTAT serve in uno o due minuti, e tra due richieste passano
almeno 13 secondi (vedi sotto). Le strutture restano in cache per 30 giorni. Le
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
├── tabelle/                 # le due tabelle di sintesi
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
| `ISTATSYNC_MIN_REQUEST_INTERVAL` | `13` | secondi tra due richieste a ISTAT (la BCE usa 2) |
| `ISTATSYNC_REQUEST_TIMEOUT` | `300` | timeout di una richiesta, in secondi |
| `ISTATSYNC_MAX_RETRIES` | `3` | tentativi su errori 5xx o di rete |
| `ISTATSYNC_STRUCTURE_TTL_DAYS` | `30` | durata della cache delle strutture |
| `ISTATSYNC_BASE_URL` | `https://esploradati.istat.it/SDMXWS/rest` | endpoint SDMX |
| `ISTATSYNC_TIMEZONE` | `Europe/Rome` | fuso per date di log e archivio |

## Aggiungere un dataset

I dataset sono descritti in [`datasets.toml`](datasets.toml). Per aggiungerne
uno:

1. trova il dataflow su [esploradati.istat.it](https://esploradati.istat.it/)
   (l'id è nell'URL, tipo `163_1226_DF_DCCN_QNA1_3`);
2. guarda dimensioni e codici:
   `istatsync describe 163_1226_DF_DCCN_QNA1_3 --codes ADJUSTMENT,VALUATION`
   (per la BCE: `istatsync describe QSA --source ecb --codes STO`);
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
source = "ecb"                           # opzionale: "istat" (default) o "ecb"
enabled = false                          # opzionale: resta nel catalogo ma non si scarica
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
almeno un dataset è fallito, così il run su GitHub risulta in errore e arriva la mail di notifica.

## Insidie delle API ISTAT e Banca d'Italia

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
anche su Drive: un job automatico parte ogni volta da un disco vuoto.

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

**Ricchezza finanziaria: niente API dalla Banca d'Italia.** Il portale Infostat
(tavola TFAT0012) non ha un'API pubblica documentata: la pagina è
un'applicazione JavaScript e il download diretto viene rifiutato. La Banca
d'Italia trasmette però gli stessi conti finanziari alla BCE (dataset QSA, settore
S1M), che li pubblica con un'API SDMX stabile. I valori coincidono con quelli
della Banca, revisioni comprese (attività a dicembre 2024: 6.033.659 milioni in
entrambe le fonti). La BCE li riceve nella stessa settimana in cui la Banca
d'Italia li diffonde.

**Endpoint.** `esploradati.istat.it/SDMXWS/rest` è quello che risponde in modo
stabile; `sdmx.istat.it` reindirizza. Se ISTAT lo cambia basta impostare
`ISTATSYNC_BASE_URL`.

## Esecuzione automatica, senza costi

Il workflow [`.github/workflows/update.yml`](.github/workflows/update.yml) gira
su GitHub Actions dal lunedì al venerdì alle 7:23 (8:23 con l'ora legale) e si
può lanciare a mano da *Actions → aggiornamento dati → Run workflow*, anche solo
per alcuni dataset.

**Perché non Cloud Run.** Su GitHub Actions non c'è niente da pagare su Google
Cloud: niente Cloud Run, Cloud Build, Artifact Registry o Cloud Scheduler, e
nessun account di fatturazione da tenere attivo. Le Actions sono gratuite per i
repository pubblici; per quelli privati il piano gratuito dà 2.000 minuti al
mese e il limite di spesa predefinito è 0 €, quindi oltre la soglia il job si
ferma invece di addebitare. Un giro giornaliero senza novità dura 5-10 minuti
(circa 150-200 minuti al mese); una volta al mese, quando scade la cache delle
strutture, circa 30.

Il job non parte mai due volte in parallelo (gruppo di concorrenza), per non
superare il limite di richieste dell'ISTAT.

### Dove finiscono i file

**Senza configurare nulla** i file sono un *artifact* dell'esecuzione: si
scaricano come zip dalla pagina del run e restano 30 giorni. Il `manifest.json`
passa da un'esecuzione all'altra nella cache di GitHub, quindi i dataset non
cambiati non vengono riscaricati.

**Su Google Drive**, senza chiavi da conservare (Workload Identity Federation:
GitHub si presenta a Google con un token che dura pochi minuti):

1. apri [Cloud Shell](https://shell.cloud.google.com) (gratuito) su un progetto
   Google Cloud qualsiasi, anche senza fatturazione, e lancia

   ```bash
   curl -sO https://raw.githubusercontent.com/AntikGhalt/sdmx-pipeline/main/scripts/setup_google_drive.sh
   bash setup_google_drive.sh <id-progetto>
   ```

   (con il repository privato il link non funziona: copia
   `scripts/setup_google_drive.sh` nell'editor di Cloud Shell). Lo script abilita le API (Drive, IAM: tutte gratuite), crea il service
   account `istatsync-drive` e lo autorizza solo per questo repository. Si può
   rilanciare senza danni;
2. aggiungi il service account che lo script stampa al **Drive condiviso** come
   *Gestore contenuti*. Deve essere un Drive condiviso, non "Il mio Drive": un
   service account non ha spazio proprio e non può creare file nelle cartelle
   personali;
3. su GitHub, *Settings → Secrets and variables → Actions → Variables*, crea le
   tre variabili che lo script stampa: `GCP_WORKLOAD_IDENTITY_PROVIDER`,
   `GCP_SERVICE_ACCOUNT`, `DRIVE_FOLDER_ID` (l'ultima parte dell'URL della
   cartella).

In alternativa funziona anche una chiave JSON del service account nel secret
`GDRIVE_SERVICE_ACCOUNT` (al posto delle prime due variabili), se la policy
dell'organizzazione permette di crearla.

Se manca qualcosa il workflow torna da solo alla modalità artifact.

Su Drive i file esistenti vengono aggiornati sul posto, quindi mantengono lo
stesso id e lo stesso link, e le connessioni di Tableau restano valide. Le
sottocartelle mancanti vengono create.

### Avviso via e-mail

Quando un'esecuzione pubblica dati nuovi o rivisti parte un'e-mail (una sola
per esecuzione) con l'elenco dei dataset aggiornati, l'edizione, l'ultimo
periodo, le tabelle ricalcolate e il link alla cartella. Se non c'è niente di
nuovo non arriva nulla. Gli errori invece li segnala GitHub, con la sua e-mail
al proprietario del repository.

Il messaggio parte da un account Gmail con una *password per le app* (serve la
verifica in due passaggi attiva):

1. sull'account mittente: <https://myaccount.google.com/apppasswords>, crea una
   password per "istatsync" e copiala;
2. su GitHub, *Settings → Secrets and variables → Actions*:
   * secret `SMTP_PASSWORD`: la password per le app;
   * variabile `SMTP_USER`: l'indirizzo Gmail che invia;
   * variabile `NOTIFY_TO`: chi riceve (più indirizzi separati da virgola).

Gli indirizzi stanno nelle variabili di GitHub e non nel codice, perché il
repository è pubblico. Senza `NOTIFY_TO` il passaggio viene saltato.

### Cambiare nome al repository

Si può fare da *Settings → General → Repository name*; GitHub reindirizza i
vecchi link. L'accesso a Google Drive è legato all'id numerico del repository,
non al nome, a patto che la configurazione sia stata fatta (o rifatta) con la
versione attuale di `scripts/setup_google_drive.sh`: chi l'ha lanciata prima
deve rilanciarla una volta, prima di cambiare nome.

### In locale

```bash
istatsync run                # in ./data, oppure su Drive se è impostato ISTATSYNC_DRIVE_FOLDER_ID
istatsync run --local        # sempre in locale
```

Per Drive in locale: `gcloud auth application-default login` con un account che
ha accesso alla cartella, oppure `GOOGLE_APPLICATION_CREDENTIALS` che punta alla
chiave del service account.

### Se si vuole comunque Cloud Run

Il `Dockerfile` produce un'immagine per un Cloud Run Job (`istatsync run`).
Prima di usarlo: impostare un **budget con avviso** sul progetto, usare
`--max-retries 0` (un retry automatico durante un blocco ISTAT lo allunga) e
non esporre un servizio HTTP pubblico. Il costo vivo è basso, ma ogni deploy
passa da Cloud Build e Artifact Registry, che sono a pagamento oltre le soglie
gratuite.

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
