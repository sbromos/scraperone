# Scraperone - Configurazione upload immagini

Di default, le immagini vengono salvate localmente nel percorso `images/...`.

## Installazione dipendenze

### Ubuntu Server 24.04 / 26.04 LTS

Installa prima i pacchetti di sistema necessari (Python 3, pip, librerie SSL e header di sviluppo):

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv python3-dev \
    libssl-dev libffi-dev ca-certificates
```

Crea un virtualenv (consigliato) e installa le dipendenze Python:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Per eseguire lo scraper ricordati di attivare il virtualenv ogni volta:

```bash
source .venv/bin/activate
```

### macOS / altri sistemi

Da `scraperone/`:

```bash
pip install -r requirements.txt
```

## Configurazione `.env` per Cloudflare R2

Il progetto carica automaticamente un file `.env` all'avvio (se presente) tramite
`python-dotenv`, senza crash se il file non esiste.

- Posiziona il file `.env` nella cartella `scraperone/` (stesso livello di `scraperissimo.py`).
- Le variabili di ambiente del sistema operativo restano compatibili e hanno precedenza
  sui valori presenti nel `.env`.

Esempio completo:

```env
USE_R2_UPLOAD=true
R2_ACCOUNT_ID=your_account_id
R2_ACCESS_KEY_ID=your_access_key_id
R2_SECRET_ACCESS_KEY=your_secret_access_key
R2_BUCKET_NAME=your_bucket_name
R2_BUCKET_FOLDER=rrc_images
R2_PUBLIC_BASE_URL=https://cdn.example.com/assets
```

L'endpoint S3-compatibile usato dal codice e':

`https://<R2_ACCOUNT_ID>.r2.cloudflarestorage.com`

Quando `USE_R2_UPLOAD` e' `false` (default), il comportamento resta invariato e vengono
ritornati i path locali relativi nel campo `images[].files`.

Quando `USE_R2_UPLOAD` e' `true`, i file vengono prima scaricati localmente e poi caricati
su R2 con chiave `images/...` se `R2_BUCKET_FOLDER` e' vuota. Se imposti `R2_BUCKET_FOLDER`,
questa sostituisce la radice locale `images/` (ad esempio `rrc_images/...`). Il valore salvato in
`images[].files` diventa l'URL finale (da `R2_PUBLIC_BASE_URL` se configurata, altrimenti
URL endpoint/bucket/key).

La cartella puo' essere scelta per ogni esecuzione, prevalendo sul valore nel `.env`:

```bash
python scraperissimo.py --bucket-folder rrc_images
```

## Error handling

Con flag attivo, il sistema registra log robusti per:

- configurazione R2 mancante/invalida (fallback automatico a storage locale);
- errori di autenticazione/permessi verso R2;
- errori di upload per singolo file.

## Fault tolerance e resume robusto

Lo scraper ora include retry robusti, checkpoint persistenti e pausa automatica in caso di outage:

- retry HTTP con backoff esponenziale + jitter su timeout/rete/DNS e su status `429`/`5xx`;
- distinzione tra errori retryable e non-retryable;
- checkpoint atomico su disco (scrittura su file temporaneo + replace) per resume sicuro anche dopo shutdown improvviso;
- resume automatico all'avvio dal checkpoint piu' recente, con skip degli elementi gia' completati;
- protezione da duplicati: gli elementi gia' processati in checkpoint non vengono rieseguiti;
- rilevamento outage (soglia di errori consecutivi), pausa, health-check periodico e ripresa automatica;
- log espliciti per retry, save/load checkpoint, pause/resume e summary finale.

### Nuove opzioni CLI

- `--max-request-retries` (default `4`)
- `--max-task-retries` (default `2`)
- `--backoff-base-sec` (default `1.0`)
- `--backoff-max-sec` (default `45.0`)
- `--backoff-jitter-sec` (default `0.5`)
- `--outage-threshold` (default `6`)
- `--healthcheck-interval-sec` (default `30.0`)
- `--checkpoint-path` (default: `<output>.checkpoint.json`)
- `--checkpoint-frequency` (default `10`)

## Formato output JSON (coin type)

Ogni record nella lista salvata dall'export usa questo ordine delle chiavi al primo livello
(inserzione dict Python 3.7+: `json.dump(..., indent=2)` leggibile e stabile, come in NumisRoma):

`_id`, `authority`, `classification`, `coinage`, `created_at`, `descriptions`, `images`, `reference`, `references`, `source_ocre_url`, `subjects`, `title`, `updated_at`.

Campi principali:

- **`_id`**: slug derivato dal tipo OCRE (`ric_id`).
- **`authority`**: `{ "issuer", "dynasty" }` (slug).
- **`classification`**: `{ "denomination", "material", "mint" }` (slug).
- **`coinage`**: solo una chiave `date` valorizzata come `{ "from", "to" }` (interi anni; negativi per a.C.) quando il testo delle date nella descrizione OCRE è parsabile; se non è parsabile rimane `{}` (nessuna `culture` / `period`).
- **`created_at` / `updated_at`**: stesso UTC per tutti i record nella stessa run, ISO 8601 con suffisso `Z`.
- **`descriptions`**: `obverse` con `legend`, `type` e opzionale `portrait`; `reverse` con `legend`, `type`.
- **`images`**: array di set; vale `[]` se non ci sono immagini. Ogni elemento mantiene l'ordine `index`, `layout` (`split` o `unified`), `license`, `source`, `copyright_holder`, `files` (per `split`: `obverse` e/o `reverse`; per `unified`: `unified`). URL pubblici vs path relativi dipendono da R2/`R2_PUBLIC_BASE_URL` e da `finalize_file` (senza duplicare logica di path lato exporter).
- **`reference`** / **`references`**: oggetto riferimento strutturato (RIC o RRC) e copia in lista `references` (lunghezza 1).
- **`source_ocre_url`**: URL della pagina OCRE; per `numismatics.org` la query include `lang=en`.
- **`subjects`**: slug.
- **`title`**: `{ "en": "<name verbatim dall'OCRE>" }`.

### Nota migrazione

Non sono richieste modifiche ai file di input/output esistenti. Se non specifichi nulla, il nuovo checkpoint viene creato automaticamente vicino all'output JSON.
