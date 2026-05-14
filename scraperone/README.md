# Scraperone - Configurazione upload immagini

Di default, le immagini vengono salvate localmente nel percorso `images/...`.

## Installazione dipendenze

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
R2_PUBLIC_BASE_URL=https://cdn.example.com/assets
```

L'endpoint S3-compatibile usato dal codice e':

`https://<R2_ACCOUNT_ID>.r2.cloudflarestorage.com`

Quando `USE_R2_UPLOAD` e' `false` (default), il comportamento resta invariato e vengono
ritornati i path locali relativi nel campo `images[].files`.

Quando `USE_R2_UPLOAD` e' `true`, i file vengono prima scaricati localmente e poi caricati
su R2 mantenendo la stessa chiave relativa (`images/...`); il valore salvato in
`images[].files` diventa l'URL finale (da `R2_PUBLIC_BASE_URL` se configurata, altrimenti
URL endpoint/bucket/key).

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

### Nota migrazione

Non sono richieste modifiche ai file di input/output esistenti. Se non specifichi nulla, il nuovo checkpoint viene creato automaticamente vicino all'output JSON.
