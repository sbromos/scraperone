"""
Scraper OCRE → metadati tipo + immagini dalla sezione Esempi (#examples).

Rileva tutte le candidate in ordine DOM; per quelle gestite tenta fetch (MANTIS
numismatics.org/collection/&lt;id&gt;, Münzkabinett Berlin IKMK,
cataloghi IKMK “stile Wien” multi-istituto (``ikmk.at``, Tuebingen, Freiburg, Freiberg NUMID, Bonn, …),
Gallica ARK) oppure
legge dal markup OCRE gli esempi British Museum (URL oggetto + thumbnail ``media.britishmuseum.org``,
senza GET alle pagine collection BM). Le righe di log ``supported: no`` su link BM di sola
navigazione o CDN includono un ``hint``; gli scarichi dal CDN usano ``Referer`` BM dove serve
e TLS tramite ``utils.download`` (fallback solo su ``SSLError``).

Scraping interno (prima della serializzazione): ``name``, ``ric_id``, ``authority``, ``description``
(``date``, ``mint``, ``denomination``, ``material``, ``subjects`` come testo), ``obverse`` / ``reverse``.

Output JSON (record moneta): ordine canonico delle chiavi di primo livello (Python 3.7+,
``json.dump(..., indent=2)``) come per l'export NumisRoma:

1. ``_id`` (slug da ``ric_id`` / tipo OCRE)
2. ``authority`` — ``{ issuer, dynasty }`` (slug)
3. ``classification`` — ``{ denomination, material, mint }`` (slug)
4. ``coinage`` — solo ``date`` numerica ``{ from, to }`` (BCE come negativi) se ricavabile dalle
   stringhe data OCRE; se non c'è un intervallo/anno parsabile resta ``{}`` (nessuna chiave
   ``culture`` / ``period``)
5. ``created_at`` — UTC ISO-8601 con suffisso ``Z`` (batch run)
6. ``descriptions`` — ``obverse``: ``legend``, ``type`` e opz. ``portrait``; ``reverse``: ``legend``, ``type``
7. ``images`` — array di set (``[]`` se assenti); per set v. ``_serialized_images_item`` (ordine:
   ``index``, ``layout``, ``license``, ``source``, ``copyright_holder``, ``files``)
8. ``reference`` — un oggetto RIC strutturato; ``references`` — array di lunghezza 1 con lo stesso oggetto
9. ``source_ocre_url`` — URL pagina OCRE normalizzato (host ``numismatics.org``: query ``lang=en``)
10. ``subjects`` — slug
11. ``title`` — ``{ "en": <name verbatim> }``
12. ``updated_at`` — come ``created_at`` (stesso valore per record nella stessa run)

Riesecuzione (--stesso ``-o``): se sul disco sono già presenti file ai path salvati nel
JSON e il layout inferito coincide con ``layout`` salvato per quel set,
gli JPEG non vengono riscaricati (log ``[img resume]``).

Parallelismo (``--workers`` > 1): ogni moneta è elaborata in un worker thread dedicato
con ``requests.Session`` separata (``threading.local``); il throttle ``--min-host-interval``
(uso consigliato ≥ 1.0 s per host) serializza gli intervalli minimi per ``netloc``. L'ordine
dei record nel JSON coincide con l'ordine degli URL in input (come a 1 worker). Più worker
aumentano throughput ma tengono più pagine OCRE / soup / risultati concorrenti in RAM;
``--low-memory`` imposta ``--workers 1`` e checkpoint sharded (vedi sotto).


Regole sul dict parsato (pre-download): due URL distinti → split; stesso URL,
un solo URL, o ``unified_image`` esplicito → unified. Gallica: fallback unified se
una sola .highres coerente (con log quando applicabile).

Ordine cartelle ``1``, ``2``, … come prima apparizione OCRE dopo dedup.

Dipendenze: requests, beautifulsoup4
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import re
import shutil
import sys
import threading
import warnings
import tempfile
import time
import unicodedata
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, NamedTuple, Optional, Sequence, Set, Tuple
from urllib.parse import quote, urlparse, urljoin, parse_qsl, urlencode, urlunparse

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_SCRIPTS_ROOT = Path(__file__).resolve().parent.parent
if str(_SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_ROOT))
from utils.download import download_image as _shared_download_image
from utils.download import http_request_with_ssl_fallback
from image_storage import (
    ImageStorageBackend,
    resolve_image_storage_from_env,
    wrap_storage_delete_local_after_upload,
)

REQUEST_DELAY_SEC = 1.0
logger = logging.getLogger(__name__)
DEFAULT_REQUEST_TIMEOUT_SEC = 60


@dataclass(frozen=True)
class FaultToleranceConfig:
    max_request_retries: int = 4
    max_task_retries: int = 2
    backoff_base_sec: float = 1.0
    backoff_max_sec: float = 45.0
    backoff_jitter_sec: float = 0.5
    outage_failure_threshold: int = 6
    outage_healthcheck_interval_sec: float = 30.0


@dataclass(frozen=True)
class CheckpointConfig:
    path: Path
    frequency_items: int = 10
    """Se True, checkpoint su manifest JSON + directory ``*.parts`` (un record per file)."""
    sharded: bool = False


CHECKPOINT_FORMAT_SHARDED_V1 = "sharded_v1"


class ParseProcessingError(Exception):
    """Errore inatteso durante parsing/normalizzazione dati."""


def _retry_delay_sec(
    attempt: int,
    *,
    base_sec: float,
    max_sec: float,
    jitter_sec: float,
) -> float:
    exp = min(max_sec, base_sec * (2 ** max(0, attempt - 1)))
    return max(0.0, exp + random.uniform(0.0, max(0.0, jitter_sec)))

def load_environment_from_dotenv(dotenv_path: Optional[Path] = None) -> None:
    """
    Carica variabili da `.env` se presente, senza sovrascrivere
    variabili gia' presenti nell'ambiente OS.
    """
    target = dotenv_path or (Path(__file__).resolve().parent / ".env")
    load_dotenv(dotenv_path=target, override=False)


# Ogni worker in ``scrape_all`` deve avere una propria ``requests.Session`` (non thread-safe).
_worker_tls = threading.local()


def worker_session() -> requests.Session:
    s = getattr(_worker_tls, "session", None)
    if s is None:
        s = session_with_retries()
        _worker_tls.session = s
    return s


class HostThrottle:
    """
    Intervallo minimo tra richieste verso lo stesso ``netloc`` (thread-safe).
    Complementa/sostituisce sleep globali: host diversi possono procedere in parallelo
    rispettando comunque la cortesia verso ciascun sito.
    """

    def __init__(self, min_interval_sec: float) -> None:
        self._min = float(min_interval_sec)
        self._host_locks: Dict[str, threading.Lock] = {}
        self._meta_lock = threading.Lock()
        self._last_monotonic: Dict[str, float] = {}

    def wait_before_request(self, url: str) -> float:
        """Attende se necessario; ritorna secondi effettivamente dormiti (>= 0)."""
        if self._min <= 0:
            return 0.0
        p = urlparse(url)
        host = (p.netloc or "").lower() or "_default"
        with self._meta_lock:
            if host not in self._host_locks:
                self._host_locks[host] = threading.Lock()
            host_lock = self._host_locks[host]
        slept = 0.0
        with host_lock:
            now = time.monotonic()
            prev = self._last_monotonic.get(host)
            if prev is not None:
                need = self._min - (now - prev)
                if need > 0:
                    time.sleep(need)
                    slept = need
            self._last_monotonic[host] = time.monotonic()
        return slept


class TimingStats:
    """Accumulatori thread-safe per ``--timing`` (secondi wall-clock per categoria)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.ocre_fetch_sec = 0.0
        self.example_fetch_sec = 0.0
        self.image_download_sec = 0.0
        self.throttle_sleep_sec = 0.0

    def add_ocre(self, dt: float) -> None:
        with self._lock:
            self.ocre_fetch_sec += dt

    def add_examples(self, dt: float) -> None:
        with self._lock:
            self.example_fetch_sec += dt

    def add_images(self, dt: float) -> None:
        with self._lock:
            self.image_download_sec += dt

    def add_throttle_sleep(self, dt: float) -> None:
        with self._lock:
            self.throttle_sleep_sec += dt

    def summary_lines(self) -> List[str]:
        with self._lock:
            o, e, i, t = (
                self.ocre_fetch_sec,
                self.example_fetch_sec,
                self.image_download_sec,
                self.throttle_sleep_sec,
            )
        tot = o + e + i + t
        return [
            f"  OCRE page fetch (HTTP + parse entry, wall-clock): {o:.2f}s",
            f"  Example fetches (wall-clock per tentativo cache-miss): {e:.2f}s",
            f"  Image downloads (wall-clock per GET stream): {i:.2f}s",
            f"  Host throttle sleep (somma attese per politeness): {t:.2f}s",
            f"  Nota: la somma {tot:.2f}s può sovrapporsi (le attese throttle sono incluse anche nei wall precedenti).",
        ]


DEFAULT_JSON_INPUT = Path(__file__).resolve().parent.parent / "File scraper Numis" / "monete_links.json"
OUTPUT_FILENAME = "output.json"
IMAGES_SUBDIR = "images"
UNIFIED_IMAGE_FILENAME = "unified.jpg"
IMAGE_HTTP_TIMEOUT = 120
IMAGE_CHUNK_SIZE = 65536

IKMK_HOST = "ikmk.smb.museum"
IKMK_OBJECT_URL = "https://ikmk.smb.museum/object"
# Base ``/object`` predefinito KHM Wien (retrocompat); le richieste runtime usano sempre
# ``ikmk_wien_style_object_base_url`` derivata dal link d'esempio sui cataloghi “stile Wien”.
IKMK_WIEN_OBJECT_URL = "https://www.ikmk.at/object"
DEFAULT_WIEN_COPYRIGHT_BASE = "Münzkabinett, Kunsthistorisches Museum Wien"
IKMK_WIEN_CC_DEED_URL = "https://creativecommons.org/licenses/by-nc-sa/3.0/at/deed.en"

# Hostname canonici (minuscolo, senza ``www.``, senza porta) per cataloghi IKM con path
# ``/object``, param ``id=ID…`` e viste ``view=vs`` / ``view=rs`` come ``ikmk.at`` (KHM Wien).
_IKMK_WIEN_STYLE_CANONICAL_HOST_KEYS = frozenset(
    {
        "ikmk.at",
        "ikmk.uni-tuebingen.de",
        "ikmk.uni-freiburg.de",
        "numid.ub.tu-freiberg.de",
        "mk-bonn.ikmk.net",
    }
)

_IKMK_IMG_PATH_MAIN_RE = re.compile(
    r"\bimg_path_main\s*=\s*(['\"])([^'\"]+)\1",
    re.I,
)
_IKMK_WIEN_CC_HREF_RE = re.compile(
    r"https://creativecommons\.org/licenses/by-nc-sa/3\.0/at[^\s\"'<>]*",
    re.I,
)

GALLICA_HOST = "gallica.bnf.fr"
GALLICA_ARK_PATH_HINT = "/ark:/12148/"
GALLICA_USER_LICENSE_LITERAL = "Conditions d'utilisation des contenus de Gallica"
GALLICA_COPYRIGHT_HOLDER_LITERAL = "Bibliothèque nationale de France"
GALLICA_SOURCE_INSTITUTION = "Bibliothèque nationale de France (Gallica)"
GALLICA_HIGHRES_URL_RE = re.compile(
    r"https://(?:www\.)?gallica\.bnf\.fr/ark:/12148/[^\s\"'<>\\]+/f\d+\.highres",
    re.I,
)
RELATIVE_GALLICA_HIGHRES_RE = re.compile(
    r'/ark:/12148/([^/\s"\'<>\\]+)/f(\d+)\.highres',
    re.I,
)
GALLICA_TWO_FACE_HINTS = (
    r'"nbTotalVues"\s*:\s*2\b',
    r"'nbTotalVues'\s*:\s*2\b",
    r'"nombreVues"\s*:\s*2\b',
    r'"nombreDeVues"\s*:\s*2\b',
    r'"nbVues"\s*:\s*2\b',
    r'"totalPages"\s*:\s*2\b',  # viewer shell (alcune versioni Gallica)
)
JSON_PARSE_SINGLE = re.compile(
    r"JSON\.parse\s*\(\s*'((?:\\.|[^'\\])*)'\s*\)",
    re.DOTALL,
)
JSON_PARSE_DOUBLE = re.compile(
    r'JSON\.parse\s*\(\s*"((?:\\.|[^"\\])*)"\s*\)',
    re.DOTALL,
)

BM_HOST_MARKER = "britishmuseum.org"
BM_MEDIA_HOST_MARKER = "media.britishmuseum.org"
BM_USER_LICENSE_LITERAL = "CC BY-NC-SA 4.0"
BM_COPYRIGHT_HOLDER_LITERAL = "The Trustees of the British Museum"
BM_SOURCE_INSTITUTION = "British Museum"
BM_MEDIA_IMAGE_EXT_RE = re.compile(r"\.(jpe?g|png|gif|webp)$", re.I)
TONGEREN_HANDLE_HOST = "hdl.handle.net"
TONGEREN_HANDLE_PREFIX = "/21.15108/"
TONGEREN_PROXY_HOST = "exploratorium.galloromeinsmuseum.be"
TONGEREN_PROXY_PATH = "/cc/imageproxy.ashx"
TONGEREN_PROXY_REFERER = "https://exploratorium.galloromeinsmuseum.be/"
TONGEREN_SOURCE_INSTITUTION = "Gallo-Roman Museum Tongeren"
TONGEREN_USER_LICENSE_LITERAL = "CC0 1.0"
TONGEREN_COPYRIGHT_HOLDER_LITERAL = TONGEREN_SOURCE_INSTITUTION

# Pagina tipo usata da ``--smoke-bm`` (Esempi con link BM ``/collection/object/``).
BM_OCRE_REGRESSION_TYPE_URL = "https://numismatics.org/ocre/id/ric.1(2).aug.100"
TONGEREN_OCRE_REGRESSION_TYPE_URL = "https://numismatics.org/ocre/id/ric.1(2).aug.245"

# Sottohost numismatics.org da non trattare come MANTIS /collection/<id> (anti-falsi positivi).
SKIP_IMAGE_EXAMPLE_HOST_HINTS = ()
DEFAULT_BERLIN_COPYRIGHT = (
    "Münzkabinett der Staatlichen Museen zu Berlin – Stiftung Preußischer Kulturbesitz"
)

LABEL_EQUIV: Mapping[str, Tuple[str, ...]] = {
    "mint": ("Zecca", "Mint"),
    "denomination": ("Nominale", "Denomination"),
    "material": ("Materiale", "Material"),
    "authority": ("Autorità emittente", "Authority"),
    "dynasty": ("Dinastia", "Dynasty"),
}

SIDE_LABELS_LEGEND = ("Legenda", "Legend")
SIDE_LABEL_TYPE = ("Tipo", "Type")
SIDE_LABEL_PORTRAIT = ("Ritratto", "Portrait")

COPYRIGHT_KEYS = ("CopyrightHolder",)
LICENSE_KEYS = ("Licenza d'uso", "Licenza d'utilizzo", "License")


def normalize_text(raw: Optional[str]) -> str:
    if raw is None:
        return ""
    t = unicodedata.normalize("NFKC", raw)
    t = re.sub(r"[\r\n\t]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def canonical_label(fragment: str) -> str:
    return normalize_text(fragment).rstrip(":").strip()


TYP_DATE_RANGE_LABELS = frozenset(
    {canonical_label(x).lower() for x in ("Arco cronologico", "Date Range")}
)
TYP_DATE_SINGLE_LABELS = frozenset(
    {canonical_label(x).lower() for x in ("Date", "Data")}
)
TYP_DATE_SECTION_ANCHOR_LABELS = TYP_DATE_RANGE_LABELS | TYP_DATE_SINGLE_LABELS

_DATE_RANGE_SPLIT_RE = re.compile(r"[\u2013\u2014\-]+")
_BCE_CE_SEGMENT_RE = re.compile(
    r"\d+(?:\s*[\u2013\u2014\-]\s*\d+)?\s*(?:BCE|CE|BC\.?|AD\.?)\b",
    re.I,
)
_BATCH_UTC_TIMESTAMP: Optional[str] = None


def batch_export_timestamps_reset() -> None:
    """Un solo timestamp UTC per tutta la run (opzionale; usato da ``scrape_all``)."""
    global _BATCH_UTC_TIMESTAMP
    _BATCH_UTC_TIMESTAMP = None


def batch_export_timestamp_utc_iso() -> str:
    """ISO 8601 UTC con suffisso Z, condiviso tra i record della stessa run."""
    global _BATCH_UTC_TIMESTAMP
    if _BATCH_UTC_TIMESTAMP is None:
        _BATCH_UTC_TIMESTAMP = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return _BATCH_UTC_TIMESTAMP


# Per ``IKMK_WIEN`` lo slug effettivo dipende dall'host del link (cartelle export distinte);
# vedi ``ikmk_wien_style_export_slug_from_example_url``. ``khm_wien`` resta il valore legacy
# nel mapping per retrocompat / fallback.
EXPORT_IMAGE_SOURCE_BY_EXAMPLE_KIND: Mapping[str, str] = {
    "MANTIS": "ans",
    "IKMK": "berlin",
    "IKMK_WIEN": "khm_wien",
    "GALLICA": "bnf",
    "BM": "british_museum",
    "BM_MEDIA": "british_museum",
    "TONGEREN": "gallo_roman_museum_tongeren",
}

_ROMAN_NUMERAL_TABLE: Tuple[Tuple[int, str], ...] = (
    (1000, "m"),
    (900, "cm"),
    (500, "d"),
    (400, "cd"),
    (100, "c"),
    (90, "xc"),
    (50, "l"),
    (40, "xl"),
    (10, "x"),
    (9, "ix"),
    (5, "v"),
    (4, "iv"),
    (1, "i"),
)


def int_to_roman(num: int) -> str:
    if num <= 0:
        return str(max(num, 0))
    n = int(num)
    parts: List[str] = []
    for val, sym in _ROMAN_NUMERAL_TABLE:
        while n >= val:
            parts.append(sym)
            n -= val
    return "".join(parts)


def slugify_es(raw: Optional[str]) -> str:
    if raw is None:
        return ""
    n = unicodedata.normalize("NFKD", str(raw))
    n = "".join(ch for ch in n if not unicodedata.combining(ch))
    n = n.lower()
    n = re.sub(r"[^a-z0-9]+", "_", n)
    n = re.sub(r"_+", "_", n).strip("_")
    return n


_DYNASTY_REDUNDANT_SUFFIX = "_dynasty"


def normalize_dynasty_slug(s: str) -> str:
    """
    Normalizza lo slug della dinastia dopo ``slugify_es``.

    Rimuove solo il suffisso esatto ``_dynasty`` dal fondo della stringa, e solo se
    la lunghezza è strettamente maggiore di quel suffisso (così stringhe vuote o
    quasi vuote non diventano accidentalmente ``""``). Altri slug non sono toccati.
    """
    suf = _DYNASTY_REDUNDANT_SUFFIX
    if len(s) > len(suf) and s.endswith(suf):
        return s[: -len(suf)]
    return s


def ocre_type_id_to_doc_id(ric_id: str) -> str:
    """Slug documento da path OCRE (es. ``ric.1(2).aug.108a`` → ``ric_1_2_aug_108a``)."""
    rid = normalize_text(ric_id).strip().lower()
    if rid.startswith("ric."):
        rid = rid[4:]
    if not rid:
        return "ric_unknown"
    segs: List[str] = []
    for seg in rid.split("."):
        s = seg.strip()
        mvol = re.match(r"^(\d+)\s*\(\s*(\d+)\s*\)$", s)
        if mvol:
            segs.extend([mvol.group(1), mvol.group(2)])
        elif s:
            segs.append(s)
    return "ric_" + "_".join(segs)


def _first_segment_to_ric_series_slug(first: str) -> str:
    m = re.match(r"^(\d+)\s*\(\s*(\d+)\s*\)$", normalize_text(first))
    if m:
        return f"ric_{int_to_roman(int(m.group(1)))}_{m.group(2)}"
    if re.match(r"^\d+$", normalize_text(first)):
        return f"ric_{int_to_roman(int(normalize_text(first)))}"
    return f"ric_{slugify_es(first)}"


def parse_ric_reference(ric_id: str) -> Dict[str, Any]:
    """Oggetto ``reference`` / voce ``references`` (RIC) da ``ric_id`` OCRE."""
    blank: Dict[str, Any] = {"system": "RIC", "series": "", "number": None, "suffix": ""}
    rid = normalize_text(ric_id).strip().lower()
    if not rid:
        return dict(blank)
    body = rid[4:] if rid.startswith("ric.") else rid
    parts = [p for p in body.split(".") if normalize_text(p)]
    if not parts:
        return dict(blank)
    last = parts[-1]
    num: Optional[int] = None
    suff = ""
    series_parts = parts
    m_last = re.match(r"^(\d+)([a-z]*)$", last, re.I)
    if m_last:
        num = int(m_last.group(1))
        suff = (m_last.group(2) or "").upper()
        series_parts = parts[:-1]
    if not series_parts:
        return {"system": "RIC", "series": slugify_es(body), "number": num, "suffix": suff}
    first = series_parts[0]
    series_slug = _first_segment_to_ric_series_slug(first)
    return {"system": "RIC", "series": series_slug, "number": num, "suffix": suff}


def parse_rrc_reference(rrc_id: str) -> Dict[str, Any]:
    """Oggetto ``reference`` / voce ``references`` (RRC) da id CRRO ``rrc-...``."""
    blank: Dict[str, Any] = {"system": "RRC", "series": "", "number": None, "suffix": ""}
    rid = normalize_text(rrc_id).strip().lower()
    if not rid:
        return dict(blank)
    m = re.match(r"^rrc-(\d+)(?:\.(\d+))?([a-z]*)$", rid, re.I)
    if not m:
        return dict(blank)
    major = int(m.group(1))
    minor = m.group(2)
    suff = (m.group(3) or "").upper()
    if minor is not None:
        return {"system": "RRC", "series": f"rrc_{major}", "number": int(minor), "suffix": suff}
    return {"system": "RRC", "series": "rrc", "number": major, "suffix": suff}


def parse_reference(coin_type_id: str) -> Dict[str, Any]:
    """Parser riferimento con branch per sistemi supportati (RIC, RRC)."""
    rid = normalize_text(coin_type_id).strip().lower()
    if rid.startswith("ric."):
        return parse_ric_reference(rid)
    if rid.startswith("rrc-"):
        return parse_rrc_reference(rid)
    return parse_ric_reference(rid)


def parse_year_token_for_coinage(token: str) -> Optional[int]:
    """Anno numerico: BCE / a.C. / BC → negativo; CE / AD / d.C. → positivo."""
    t = normalize_text(token)
    if not t:
        return None
    m = re.search(r"-?\d+", t)
    if not m:
        return None
    n = int(m.group(0))
    tl = t.lower()
    if "a.c" in tl or "avanti cristo" in tl or "bce" in tl or re.search(r"\bbc\.?\b", tl):
        return -abs(n)
    if (
        "d.c" in tl
        or "dopo cristo" in tl
        or re.search(r"\bce\b", tl)
        or re.search(r"\bad\.?\b", tl)
    ):
        return abs(n)
    return n


def coinage_date_from_description_dates(date_strings: List[str]) -> Optional[Dict[str, int]]:
    """Da ``description.date`` (stringhe OCRE) a ``{ from, to }`` (BCE negativi)."""
    flat: List[str] = []
    for s in date_strings or []:
        for b in _DATE_RANGE_SPLIT_RE.split(s):
            bit = normalize_text(b)
            if bit:
                flat.append(bit)
    if len(flat) >= 2:
        y1 = parse_year_token_for_coinage(flat[0])
        y2 = parse_year_token_for_coinage(flat[1])
        if y1 is not None and y2 is not None:
            return {"from": y1, "to": y2}
    if len(flat) == 1:
        y = parse_year_token_for_coinage(flat[0])
        if y is not None:
            return {"from": y, "to": y}
    return None


def record_to_export_payload(record: Dict[str, Any], *, page_url: str) -> Dict[str, Any]:
    """
    Trasforma il record interno post-scrape nel layout JSON target (ordine chiavi NumisRoma).

    ``coinage``: solo ``date`` ``{ from, to }`` se parsabile dalle date in descrizione; altrimenti ``{}``.
    ``title.en`` = ``name`` verbatim; ``authority`` / ``classification`` come slug; ``dynasty`` con
    eventuale rimozione del suffisso ridondante ``_dynasty`` (``normalize_dynasty_slug``).
    ``source_ocre_url``: per OCRE su ``numismatics.org`` query ``lang=en`` (``force_lang_en_on_input_url``).
    """
    ric_id = normalize_text(str(record.get("ric_id") or ""))
    doc_id = ocre_type_id_to_doc_id(ric_id) if ric_id else slugify_es(str(record.get("name") or "coin"))
    reference_obj = parse_reference(ric_id)
    references_list = [dict(reference_obj)]

    name_now = record.get("name")
    title_obj = {"en": str(name_now) if name_now is not None else ""}

    coinage: Dict[str, Any] = {}
    desc_block = record.get("description") if isinstance(record.get("description"), dict) else {}
    date_arr = desc_block.get("date") if isinstance(desc_block, dict) else []
    if not isinstance(date_arr, list):
        date_arr = []
    cd = coinage_date_from_description_dates([str(x) for x in date_arr])
    if cd is not None:
        coinage["date"] = cd

    auth_in = record.get("authority") if isinstance(record.get("authority"), dict) else {}
    authority_out = {
        "issuer": slugify_es(str(auth_in.get("emperor") or "")),
        "dynasty": normalize_dynasty_slug(slugify_es(str(auth_in.get("dynasty") or ""))),
    }

    desc = desc_block if isinstance(desc_block, dict) else {}
    classification = {
        "denomination": slugify_es(str(desc.get("denomination") or "")),
        "material": slugify_es(str(desc.get("material") or "")),
        "mint": slugify_es(str(desc.get("mint") or "")),
    }

    obv = record.get("obverse") if isinstance(record.get("obverse"), dict) else {}
    rev = record.get("reverse") if isinstance(record.get("reverse"), dict) else {}
    descriptions_out: Dict[str, Any] = {
        "obverse": {
            "legend": str(obv.get("legend") or ""),
            "type": str(obv.get("type") or ""),
        },
        "reverse": {
            "legend": str(rev.get("legend") or ""),
            "type": str(rev.get("type") or ""),
        },
    }
    portrait = obv.get("portrait")
    if portrait:
        descriptions_out["obverse"]["portrait"] = str(portrait)

    subj_raw = desc.get("subjects")
    if not isinstance(subj_raw, list):
        subj_raw = []
    subjects_out: List[str] = []
    for x in subj_raw:
        sx = slugify_es(str(x))
        if sx:
            subjects_out.append(sx)

    images_out: List[Dict[str, Any]] = []
    for row in record.get("images") or []:
        if isinstance(row, dict):
            images_out.append(dict(row))

    ts = batch_export_timestamp_utc_iso()
    source_url = force_lang_en_on_input_url(normalize_text(page_url))
    return {
        "_id": doc_id,
        "authority": authority_out,
        "classification": classification,
        "coinage": coinage,
        "created_at": ts,
        "descriptions": descriptions_out,
        "images": images_out,
        "reference": dict(reference_obj),
        "references": references_list,
        "source_ocre_url": source_url,
        "subjects": subjects_out,
        "title": title_obj,
        "updated_at": ts,
    }


def strip_label_prefix(li_text: str, label_text: str) -> str:
    whole = normalize_text(li_text)
    prefix = normalize_text(label_text).rstrip(":")
    variants = ([f"{prefix}:", prefix] + ([f"{prefix} :"] if prefix else []))
    for v in variants:
        if whole.lower().startswith(v.lower()):
            return normalize_text(whole[len(v) :])
    return whole


def with_lang_it(url: str) -> str:
    p = urlparse(url.strip())
    query = parse_qsl(p.query, keep_blank_values=True)
    if any(k == "lang" for k, _ in query):
        return url.strip()
    query.append(("lang", "it"))
    new_query = urlencode(query, quote_via=quote)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))


def force_lang_en_on_input_url(url: str) -> str:
    """
    Per gli URL in input (solo host ``numismatics.org``): rimuove dalla query ogni parametro
    ``lang`` (qualsiasi valore, confronto case-insensitive sulla chiave), poi aggiunge una
    sola coppia ``lang=en`` — coerente col comportamento live del sito (OCRE accetta ``lang=en``).

    Altri host restano invariati (es. IKM può usare convenzioni diverse; non forzare qui).
    """
    p = urlparse((url or "").strip())
    if "numismatics.org" not in (p.netloc or "").lower():
        return (url or "").strip()
    pairs = parse_qsl(p.query, keep_blank_values=True)
    kept = [(k, v) for k, v in pairs if k.lower() != "lang"]
    kept.append(("lang", "en"))
    new_query = urlencode(kept, quote_via=quote)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_query, p.fragment))


_COIN_TYPE_PATH_PREFIXES = ("/ocre/id/", "/crro/id/")


def extract_ocre_type_id_from_url(url: str) -> str:
    """
    Restituisce il segmento di path dopo ``/ocre/id/`` o ``/crro/id/`` (senza query né fragment),
    oppure ``""`` se host/path non corrispondono a un URL tipo atteso (es. numismatics.org).
    """
    p = urlparse((url or "").strip())
    if "numismatics.org" not in (p.netloc or "").lower():
        return ""
    path = (p.path or "").rstrip("/")
    for prefix in _COIN_TYPE_PATH_PREFIXES:
        if path.startswith(prefix):
            rid = path[len(prefix) :]
            return rid if rid else ""
    return ""


SOURCE_OCRE_URL_KEY = "source_ocre_url"


def normalize_ocre_source_key(url: str) -> str:
    """
    Chiave stabile per abbinare un record a una run precedente (URL OCRE richiesto).

    Per ``numismatics.org`` applica ``force_lang_en_on_input_url`` prima del confronto, così
    ``lang=it``, ``lang=en`` o assenza di ``lang`` sullo stesso tipo OCRE collidono (allineato
    a ``source_ocre_url`` nell'export).
    """
    base = normalize_text(url)
    if not base:
        return ""
    return force_lang_en_on_input_url(base).lower()


def _shallow_row_copy(row: Any) -> Any:
    return dict(row) if isinstance(row, dict) else row


def slim_coin_record_for_resume(rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Riduce i dict tenuti in RAM per il resume immagini: solo chiavi usate da
    ``match_previous_coin_record`` / ``save_coin_images_local`` (layout e path).
    """
    slim: Dict[str, Any] = {}
    if SOURCE_OCRE_URL_KEY in rec:
        slim[SOURCE_OCRE_URL_KEY] = rec[SOURCE_OCRE_URL_KEY]
    imgs = rec.get("images")
    if isinstance(imgs, list):
        slim["images"] = [_shallow_row_copy(x) for x in imgs if isinstance(x, dict)]
    legacy = rec.get("image_sets")
    if isinstance(legacy, list):
        slim["image_sets"] = [_shallow_row_copy(x) for x in legacy]
    return slim


def load_previous_results_for_resume(out_path: Path) -> Optional[List[Dict[str, Any]]]:
    """
    Carica l'output JSON precedente se presente e valido (lista di record).

    Serve al resume degli scarichi: None se file assente o non parsabile come lista.

    I record possono ancora usare lo schema legacy ``image_sets`` (vedi
    ``_prior_layout_and_files``); dopo una run completa l'output usa ``images``.
    """
    if not out_path.is_file():
        return None
    try:
        data = json.loads(out_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    if isinstance(data, list):
        slimmed = [slim_coin_record_for_resume(x) for x in data if isinstance(x, dict)]
        del data
        return slimmed
    return None


def load_checkpoint(
    checkpoint_path: Path,
) -> Tuple[Set[int], Dict[int, Dict[str, Any]], bool]:
    """
    Carica checkpoint da disco.

    Terzo valore: ``legacy_inline_on_disk`` è True se il file era in formato
    monolitico (``results_by_index`` nel manifest). Serve a ``--checkpoint-sharded``:
    al primo salvataggio vanno scritti tutti i ``.parts`` già presenti in RAM.
    """
    if not checkpoint_path.is_file():
        return set(), {}, False
    try:
        payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        logger.warning("Checkpoint non leggibile/corrotto (%s), riparto senza checkpoint.", checkpoint_path)
        return set(), {}, False

    fmt = payload.get("checkpoint_format")
    if fmt == CHECKPOINT_FORMAT_SHARDED_V1:
        parts_rel = payload.get("parts_dir") or (checkpoint_path.name + ".parts")
        parts_dir = checkpoint_path.parent / str(parts_rel)
        done_raw = payload.get("completed_indices", [])
        out_done: Set[int] = set()
        if isinstance(done_raw, list):
            for v in done_raw:
                if isinstance(v, int) and v >= 0:
                    out_done.add(v)
        out_items: Dict[int, Dict[str, Any]] = {}
        for idx in sorted(out_done):
            part_path = parts_dir / f"{idx:08d}.json"
            if not part_path.is_file():
                logger.warning(
                    "Checkpoint sharded: indice %s in manifest ma file assente (%s).",
                    idx,
                    part_path,
                )
                continue
            try:
                row = json.loads(part_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning("Checkpoint sharded: part illeggibile %s (%s).", part_path, e)
                continue
            if isinstance(row, dict):
                out_items[idx] = row
        try:
            for p in sorted(parts_dir.glob("????????.json")):
                try:
                    idx = int(p.stem)
                except ValueError:
                    continue
                if idx in out_items:
                    continue
                try:
                    row = json.loads(p.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if isinstance(row, dict):
                    out_items[idx] = row
                    out_done.add(idx)
                    logger.info("Checkpoint recovery: indice %s ripreso da part orfano.", idx)
        except OSError:
            pass
        logger.info("Checkpoint caricato (sharded): %s elementi.", len(out_done))
        return out_done, out_items, False

    legacy_inline_on_disk = True
    done = payload.get("completed_indices", [])
    items = payload.get("results_by_index", {})
    out_done = set()
    out_items = {}
    if isinstance(done, list):
        for v in done:
            if isinstance(v, int) and v >= 0:
                out_done.add(v)
    if isinstance(items, dict):
        for k, v in items.items():
            if isinstance(v, dict):
                try:
                    idx = int(k)
                except (TypeError, ValueError):
                    continue
                out_items[idx] = v
                out_done.add(idx)
    logger.info("Checkpoint caricato: %s elementi completati.", len(out_done))
    return out_done, out_items, legacy_inline_on_disk


def checkpoint_parts_dir(checkpoint_path: Path) -> Path:
    """Directory dei record ``NNNNNNNN.json`` accanto al manifest checkpoint."""
    return checkpoint_path.with_name(checkpoint_path.name + ".parts")


def _write_checkpoint_part_atomic(parts_dir: Path, idx: int, record: Dict[str, Any]) -> None:
    parts_dir.mkdir(parents=True, exist_ok=True)
    part_path = parts_dir / f"{idx:08d}.json"
    tmp = part_path.with_suffix(part_path.suffix + ".tmp")
    tmp.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, part_path)


def save_checkpoint_sharded_atomic(
    checkpoint_path: Path,
    completed_indices: Set[int],
    results_by_index: Dict[int, Dict[str, Any]],
    *,
    dirty_indices: Optional[Set[int]] = None,
    write_all_parts: bool = False,
) -> None:
    """
    Manifest compatto + un JSON per indice. ``dirty_indices`` limita i part riscritti;
    con ``write_all_parts`` si ignorano i dirty e si riserializza tutto ``results_by_index``.
    """
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    parts_dir = checkpoint_parts_dir(checkpoint_path)
    if write_all_parts or dirty_indices is None:
        to_write = sorted(results_by_index.keys())
    else:
        to_write = sorted(i for i in dirty_indices if i in results_by_index)
    for idx in to_write:
        _write_checkpoint_part_atomic(parts_dir, idx, results_by_index[idx])
    manifest = {
        "checkpoint_format": CHECKPOINT_FORMAT_SHARDED_V1,
        "completed_indices": sorted(completed_indices),
        "parts_dir": parts_dir.name,
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    tmp = checkpoint_path.with_suffix(checkpoint_path.suffix + ".tmp")
    tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, checkpoint_path)
    logger.info("Checkpoint salvato (sharded): %s", checkpoint_path)


def save_checkpoint_atomic(
    checkpoint_path: Path,
    completed_indices: Set[int],
    results_by_index: Dict[int, Dict[str, Any]],
) -> None:
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = checkpoint_path.with_suffix(checkpoint_path.suffix + ".tmp")
    payload = {
        "completed_indices": sorted(completed_indices),
        "results_by_index": {str(k): v for k, v in sorted(results_by_index.items(), key=lambda it: it[0])},
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, checkpoint_path)
    logger.info("Checkpoint salvato: %s", checkpoint_path)


def write_json_array_indented_atomic(out_path: Path, records: Sequence[Dict[str, Any]]) -> None:
    """
    Scrive un array JSON pretty-printed senza costruire l'intera serializzazione in una stringa.

    Riduce il picco RAM rispetto a ``write_text(json.dumps(...))`` su output molto grandi.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as fh:
        fh.write("[")
        for i, rec in enumerate(records):
            if i:
                fh.write(",")
            fh.write("\n")
            json.dump(rec, fh, ensure_ascii=False, indent=2)
        fh.write("\n]")
        fh.write("\n")
    os.replace(tmp, out_path)


def match_previous_coin_record(
    previous_list: Optional[List[Dict[str, Any]]],
    urls: List[str],
    idx: int,
) -> Optional[Dict[str, Any]]:
    """
    Abbinamento record precedente alla moneta ``urls[idx]``.

    Se il JSON precedente contiene ancora la chiave legacy ``source_ocre_url``, si preferisce
    il record il cui valore coincide (case-insensitive) con l'URL richiesto.
    Altrimenti: stesso indice ``idx`` quando ``len(previous_list) == len(urls)``.
    """
    if not previous_list:
        return None
    raw = urls[idx]
    key = normalize_ocre_source_key(raw)
    if key:
        for pr in previous_list:
            purl = normalize_text(str(pr.get(SOURCE_OCRE_URL_KEY, "") or ""))
            if purl and normalize_ocre_source_key(purl) == key:
                return pr
    if len(previous_list) == len(urls) and 0 <= idx < len(previous_list):
        return previous_list[idx]
    return None


def disk_file_exists_under_output(output_base: Path, rel_or_empty: str) -> bool:
    """Verifica ``output_base`` / ``rel_or_empty`` (path relativo posix tipico nell'output JSON)."""
    rel = normalize_text(rel_or_empty)
    if not rel:
        return False
    target = output_base.joinpath(rel.replace("\\", "/"))
    try:
        return target.is_file()
    except OSError:
        return False


def _prior_layout_and_files(prior_row: Optional[Dict[str, Any]]) -> Tuple[str, Dict[str, str]]:
    """
    Estrae da una voce di ``images`` (o legacy ``image_sets``) il layout e i path noti.

    Compatibilità una tantum: legge ``layout`` / ``files`` (nuovo) oppure
    ``image_layout`` / ``image_set.{obverse_image,reverse_image,unified_image}`` (vecchio).
    Se entrambi sono presenti, i valori in ``files`` sovrascrivono quelli legacy.
    """
    if not isinstance(prior_row, dict):
        return "", {}
    layout = normalize_text(str(prior_row.get("layout", "") or ""))
    if not layout:
        layout = normalize_text(str(prior_row.get("image_layout", "") or ""))
    paths: Dict[str, str] = {}
    inner = prior_row.get("image_set")
    if isinstance(inner, dict):
        o = normalize_text(str(inner.get("obverse_image", "") or ""))
        r = normalize_text(str(inner.get("reverse_image", "") or ""))
        u = normalize_text(str(inner.get("unified_image", "") or ""))
        if o:
            paths["obverse"] = o
        if r:
            paths["reverse"] = r
        if u:
            paths["unified"] = u
    files_obj = prior_row.get("files")
    if isinstance(files_obj, dict):
        for k in ("obverse", "reverse", "unified"):
            v = normalize_text(str(files_obj.get(k, "") or ""))
            if v:
                paths[k] = v
    return layout, paths


def layouts_match_for_resume(prior_layout: str, inferred_new_layout: str) -> bool:
    """Reuse dei path precedenti solo se il layout coincide (evita stale split/unified)."""
    pl = normalize_text(prior_layout)
    nl = normalize_text(inferred_new_layout)
    return bool(pl and nl and pl == nl and nl in ("split", "unified"))


def build_empty_record() -> Dict[str, Any]:
    return {
        "name": "",
        "ric_id": "",
        "authority": {"emperor": "", "dynasty": ""},
        "description": {
            "date": [],
            "mint": "",
            "denomination": "",
            "material": "",
            "subjects": [],
        },
        "obverse": {
            "legend": "",
            "type": "",
            "portrait": "",
        },
        "reverse": {
            "legend": "",
            "type": "",
        },
        # Voci: index, layout, license, copyright_holder, files { obverse? reverse? unified? }
        "images": [],
    }


def infer_image_layout_and_urls(
    parsed: Optional[Mapping[str, str]],
) -> Tuple[str, str, str, str]:
    """
    Interpreta dict parsato (fetch) come split o unified.

    Ritorna (layout, unified_url, obverse_url, reverse_url): layout è ``split``, ``unified``
    oppure stringa vuota se incompleto. Per split, unified_url è vuoto; per unified,
    gli URL per lati separati nell'output sono vuoti e si usa unified_url per il download.
    """
    if not parsed:
        return "", "", "", ""
    o = normalize_text(str(parsed.get("obverse_image", "")))
    r = normalize_text(str(parsed.get("reverse_image", "")))
    u = normalize_text(str(parsed.get("unified_image", "")))
    if u:
        return "unified", u, "", ""
    if o and r:
        if o == r:
            return "unified", o, "", ""
        return "split", "", o, r
    if o or r:
        return "unified", o or r, "", ""
    return "", "", "", ""


def parsed_image_set_complete(parsed: Optional[Mapping[str, str]]) -> bool:
    layout, uni, obv, rev = infer_image_layout_and_urls(parsed)
    if layout == "split":
        return bool(obv and rev)
    if layout == "unified":
        return bool(uni)
    return False


def _bm_resolved_collect_dedupe_key(parsed: Optional[Mapping[str, str]]) -> Optional[str]:
    """
    Chiave stabile per dedup degli esempi BM / BM_MEDIA dopo fetch: stessa immagine CDN
    (unificata) o stessa coppia obv/rev normalizzata, indipendentemente dall'href OCRE.
    """
    if not parsed:
        return None
    layout, uni, obv, rev = infer_image_layout_and_urls(parsed)
    if layout == "unified" and uni:
        return "u:" + _bm_normalize_media_url_large_thumbnail(uni).lower()
    if layout == "split" and obv and rev:
        o = _bm_normalize_media_url_large_thumbnail(obv).lower()
        r = _bm_normalize_media_url_large_thumbnail(rev).lower()
        return f"s:{o}|{r}"
    return None


def _tongeren_resolved_collect_dedupe_key(parsed: Optional[Mapping[str, str]]) -> Optional[str]:
    """Chiave stabile per dedup set Tongeren: stessa immagine unified dopo normalizzazione proxy."""
    if not parsed:
        return None
    layout, uni, _, _ = infer_image_layout_and_urls(parsed)
    if layout == "unified" and uni:
        return "u:" + _normalize_tongeren_imageproxy_url(uni).lower()
    return None


def _bm_register_fetch_cache_aliases(
    fetch_cache: Dict[str, Optional[Dict[str, str]]],
    *,
    kind: str,
    url: str,
    parsed: Optional[Dict[str, str]],
) -> None:
    """
    Dopo parse BM/BM_MEDIA completo, registra gli stessi ``parsed`` anche sotto le altre
    chiavi ``classify_example_url`` (pagina oggetto vs CDN) così ``fetch_cache`` evita due
    passate sul markup OCRE per lo stesso asset.
    """
    if not parsed or not parsed_image_set_complete(parsed):
        return
    if kind not in ("BM", "BM_MEDIA"):
        return
    layout, uni, _, _ = infer_image_layout_and_urls(parsed)

    def _poke_bm_object_keys(obj_abs: str) -> None:
        if not obj_abs or not is_british_museum_collection_object_url(obj_abs):
            return
        raw_l = normalize_text(obj_abs).lower()
        can_l = _bm_canonical_object_url(obj_abs).lower()
        fetch_cache[f"bm:{raw_l}"] = parsed
        fetch_cache[f"bm:{can_l}"] = parsed

    _poke_bm_object_keys(url)

    parsed_page = normalize_text(parsed.get("parsed_page_url", ""))
    if parsed_page and normalize_text(parsed_page) != normalize_text(url):
        _poke_bm_object_keys(parsed_page)

    if layout == "unified" and uni:
        media_norm = _bm_normalize_media_url_large_thumbnail(uni).lower()
        fetch_cache[f"bm_media:{media_norm}"] = parsed


def session_with_retries() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1.5,
        status_forcelist=(408, 409, 429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(
        {
            "User-Agent": (
                "scraperissimo/1.0 (+https://numismatics.org; educational research; respectful crawl)"
            ),
            "Accept-Language": "it,en;q=0.9",
        }
    )
    return s


class OutageGuard:
    """Rileva outage globali e sospende le richieste finché il sito non torna disponibile."""

    def __init__(self, cfg: FaultToleranceConfig) -> None:
        self._cfg = cfg
        self._lock = threading.Lock()
        self._consecutive_failures = 0
        self._paused = False

    def register_success(self) -> None:
        with self._lock:
            self._consecutive_failures = 0
            if self._paused:
                self._paused = False
                logger.warning("Sito di nuovo raggiungibile: riprendo lo scraping.")

    def register_failure(self) -> bool:
        with self._lock:
            self._consecutive_failures += 1
            if not self._paused and self._consecutive_failures >= self._cfg.outage_failure_threshold:
                self._paused = True
                logger.warning(
                    "Possibile outage rilevato (%s errori consecutivi). Metto in pausa.",
                    self._consecutive_failures,
                )
            return self._paused

    def wait_until_recovered(self, healthcheck: Callable[[], bool]) -> None:
        while True:
            with self._lock:
                if not self._paused:
                    return
            logger.warning(
                "Outage attivo: health-check tra %.1fs.",
                self._cfg.outage_healthcheck_interval_sec,
            )
            time.sleep(self._cfg.outage_healthcheck_interval_sec)
            ok = False
            try:
                ok = bool(healthcheck())
            except Exception:
                ok = False
            if ok:
                self.register_success()
                return


def _is_retryable_status(status_code: int) -> bool:
    return status_code == 429 or 500 <= status_code <= 599


def _is_retryable_request_error(exc: Exception) -> bool:
    if isinstance(exc, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(exc, requests.HTTPError):
        resp = exc.response
        return bool(resp is not None and _is_retryable_status(resp.status_code))
    return False


def request_with_fault_tolerance(
    session: requests.Session,
    method: str,
    url: str,
    *,
    fault_cfg: FaultToleranceConfig,
    outage_guard: Optional[OutageGuard] = None,
    timeout: int = DEFAULT_REQUEST_TIMEOUT_SEC,
    **kwargs: Any,
) -> requests.Response:
    def _healthcheck() -> bool:
        try:
            hr = session.request("GET", url, timeout=min(10, timeout), **kwargs)
            return hr.status_code < 500
        except Exception:
            return False

    for attempt in range(1, fault_cfg.max_request_retries + 2):
        if outage_guard is not None:
            outage_guard.wait_until_recovered(_healthcheck)
        try:
            response = session.request(method, url, timeout=timeout, **kwargs)
            if _is_retryable_status(response.status_code):
                raise requests.HTTPError(
                    f"HTTP {response.status_code} retryable",
                    response=response,
                )
            if outage_guard is not None:
                outage_guard.register_success()
            return response
        except Exception as exc:
            retryable = _is_retryable_request_error(exc)
            if outage_guard is not None and retryable:
                outage_guard.register_failure()
            if not retryable or attempt > fault_cfg.max_request_retries:
                raise
            delay = _retry_delay_sec(
                attempt,
                base_sec=fault_cfg.backoff_base_sec,
                max_sec=fault_cfg.backoff_max_sec,
                jitter_sec=fault_cfg.backoff_jitter_sec,
            )
            logger.warning(
                "Retry richiesta %s %s tentativo %s/%s tra %.2fs (%s)",
                method,
                url,
                attempt,
                fault_cfg.max_request_retries,
                delay,
                type(exc).__name__,
            )
            time.sleep(delay)
    raise RuntimeError("unreachable")


def fetch(
    session: requests.Session,
    url: str,
    *,
    throttle: Optional[HostThrottle] = None,
    timing: Optional[TimingStats] = None,
    fault_cfg: Optional[FaultToleranceConfig] = None,
    outage_guard: Optional[OutageGuard] = None,
) -> str:
    if throttle is not None:
        slp = throttle.wait_before_request(url)
        if timing is not None:
            timing.add_throttle_sleep(slp)
    cfg = fault_cfg or FaultToleranceConfig()
    r = request_with_fault_tolerance(
        session,
        "GET",
        url,
        timeout=DEFAULT_REQUEST_TIMEOUT_SEC,
        fault_cfg=cfg,
        outage_guard=outage_guard,
    )
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text


def find_typological_metadata_section(soup: BeautifulSoup) -> Optional[BeautifulSoup]:
    """Sezione contenente gli attributi bibliografici (non Subjects né References)."""

    candidates: List[BeautifulSoup] = []
    seen_ids: set[int] = set()

    def add_sec(sec: Optional[BeautifulSoup]) -> None:
        if not sec:
            return
        sid = id(sec)
        if sid not in seen_ids:
            seen_ids.add(sid)
            candidates.append(sec)

    for sec in soup.select("div.metadata_section"):
        for li in sec.find_all("li"):
            b = li.find("b", recursive=False)
            if not b:
                continue
            lbl = canonical_label(b.get_text()).lower()
            if lbl in TYP_DATE_SECTION_ANCHOR_LABELS:
                add_sec(sec)
                break

    for sec in soup.select("div.metadata_section"):
        if sec.find("ul", rel="nmo:hasObverse"):
            add_sec(sec)
            break

    return candidates[0] if candidates else None


def li_field_value(li: BeautifulSoup) -> str:
    """Valore dopo l'etichetta (include testo prima/dopo link, utile es. Licenza d'uso)."""
    b = li.find("b", recursive=False)
    if not b:
        return normalize_text(li.get_text())
    return strip_label_prefix(li.get_text(), b.get_text())


def _nonempty_trimmed_parts(parts: Iterable[str]) -> List[str]:
    out: List[str] = []
    for p in parts:
        t = normalize_text(p)
        if t:
            out.append(t)
    return out


def date_range_value_to_array(raw: str) -> List[str]:
    """
    Testo dell'intervallo dopo l'etichetta (Date Range / Arco cronologico):
    separa sugli usuali trattini OCRE; se lo split produce più di due parti
    significative, tenta segmenti ``... BCE/CE`` oppure una singola voce (warning).
    """
    s = normalize_text(raw)
    if not s:
        return []
    split_bits = _DATE_RANGE_SPLIT_RE.split(s)
    parts = _nonempty_trimmed_parts(split_bits)
    if len(parts) == 2:
        return parts
    if len(parts) < 2:
        return parts
    matches = _BCE_CE_SEGMENT_RE.findall(s)
    if len(matches) == 2:
        return [normalize_text(m) for m in matches]
    warnings.warn(
        f"date range: forma inattesa ({len(parts)} segmenti), uso stringa unica: {s!r}",
        RuntimeWarning,
        stacklevel=2,
    )
    return [s]


def _typological_single_date_text(li: BeautifulSoup) -> str:
    """Preferisce il testo umano in ``span`` con ``property`` che include ``hasDate``."""
    for span in li.find_all("span"):
        prop = span.get("property") or ""
        if "hasdate" in normalize_text(str(prop)).lower().replace(" ", ""):
            t = normalize_text(span.get_text())
            if t:
                return t
    return li_field_value(li)


def _first_li_matching_labels(
    section: BeautifulSoup,
    labels: frozenset[str],
) -> Optional[BeautifulSoup]:
    for li in section.find_all("li"):
        b = li.find("b", recursive=False)
        if not b:
            continue
        lbl = canonical_label(b.get_text()).lower()
        if lbl in labels:
            return li
    return None


def parse_typological_date_array(section: Optional[BeautifulSoup]) -> List[str]:
    """
    Estrae ``date`` dalla Descrizione del tipo: intervallo (Date Range / Arco cronologico)
    o istante (Date / Data). Se compaiono entrambe le voci, prevale l'intervallo.
    Senza blocco riconosciuto: ``[]``.
    """
    if not section:
        return []
    range_li = _first_li_matching_labels(section, TYP_DATE_RANGE_LABELS)
    if range_li is not None:
        return date_range_value_to_array(li_field_value(range_li))
    single_li = _first_li_matching_labels(section, TYP_DATE_SINGLE_LABELS)
    if single_li is not None:
        one = _typological_single_date_text(single_li)
        return [one] if one else []
    return []


def _anchor_has_dcterms_subject_rel(rel_attr: Any) -> bool:
    if rel_attr is None:
        return False
    if isinstance(rel_attr, list):
        bits = [normalize_text(str(x)).lower().replace(" ", "") for x in rel_attr]
    else:
        bits = [normalize_text(str(rel_attr)).lower().replace(" ", "")]
    return any("subject" in b or "dcterms:subject" in b for b in bits)


def concept_li_label_value(li: BeautifulSoup) -> str:
    """
    Testo del concetto dopo <b>Concept:</b>: preferisce i chunk di testo fuori dai link;
    se mancano, usa il testo dei soli <a rel="dcterms:subject">. Altri link (es. Wikidata) ignorati.
    """
    b = li.find("b", recursive=False)
    if not b:
        return normalize_text(li.get_text())
    plain_parts: List[str] = []
    subj_parts: List[str] = []
    for sib in b.next_siblings:
        nm = getattr(sib, "name", None)
        if nm is None:
            t = normalize_text(str(sib))
            if t:
                plain_parts.append(t)
        elif nm == "a":
            if _anchor_has_dcterms_subject_rel(sib.get("rel")):
                t = normalize_text(sib.get_text())
                if t:
                    subj_parts.append(t)
        else:
            inner = BeautifulSoup(str(sib), "html.parser")
            root = inner.find() or inner
            for a in root.find_all("a"):
                if not _anchor_has_dcterms_subject_rel(a.get("rel")):
                    a.decompose()
            t = normalize_text(root.get_text())
            if t:
                plain_parts.append(t)
    if plain_parts:
        return normalize_text(" ".join(plain_parts))
    if subj_parts:
        return normalize_text(" ".join(subj_parts))
    return ""


def first_label_match(
    section: Optional[BeautifulSoup],
    wanted_labels: Iterable[str],
) -> str:
    if not section:
        return ""
    wanted = {canonical_label(x).lower() for x in wanted_labels}
    for li in section.find_all("li"):
        b = li.find("b", recursive=False)
        if not b:
            continue
        lbl = canonical_label(b.get_text())
        if lbl.lower() in wanted:
            return li_field_value(li)
    return ""


SUBJECTS_SECTION_H3_TEXT = frozenset(
    {"soggetti", "subjects"},
)
CONCEPT_LI_LABELS = frozenset(
    {canonical_label(x).lower() for x in ("Concept", "Concetto")},
)


def _subjects_ul_in_section(sec: BeautifulSoup, h3: BeautifulSoup) -> Optional[BeautifulSoup]:
    """Primo `ul` che segue l'h3 nella stessa metadata_section."""
    for sib in h3.next_siblings:
        if getattr(sib, "name", None) == "ul":
            return sib
    nxt = h3.find_next("ul")
    if nxt and nxt.find_parent("div", class_="metadata_section") is sec:
        return nxt
    return None


def _dedupe_subject_labels_ordered(labels: List[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for x in labels:
        t = normalize_text(x)
        if not t:
            continue
        k = t.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(t)
    return out


def parse_subjects_concept_labels(soup: BeautifulSoup) -> List[str]:
    """
    Sezione metadata Soggetti/Subjects: ogni `li` con etichetta Concept (it/en), valore via concept_li_label_value
    (solo etichetta testuale, senza URI Wikidata).
    """
    for sec in soup.select("div.metadata_section"):
        first_heading = sec.find(["h2", "h3", "h4", "h5"])
        if first_heading is None or first_heading.name.lower() != "h3":
            continue
        if normalize_text(first_heading.get_text()).lower() not in SUBJECTS_SECTION_H3_TEXT:
            continue
        h3 = first_heading
        ul = _subjects_ul_in_section(sec, h3)
        if not ul:
            return []
        labels: List[str] = []
        for li in ul.find_all("li", recursive=False):
            b = li.find("b", recursive=False)
            if not b:
                continue
            if canonical_label(b.get_text()).lower() not in CONCEPT_LI_LABELS:
                continue
            val = concept_li_label_value(li)
            if val:
                labels.append(val)
        return _dedupe_subject_labels_ordered(labels)
    return []


def parse_side_ul(ul: Optional[BeautifulSoup]) -> Dict[str, str]:
    out = {"legend": "", "type": "", "portrait": ""}
    if not ul:
        return out
    for li in ul.find_all("li", recursive=False):
        b = li.find("b", recursive=False)
        if not b:
            continue
        lbl = canonical_label(b.get_text())
        val = li_field_value(li)
        if lbl in SIDE_LABEL_LEGEND:
            out["legend"] = val
        elif lbl in SIDE_LABEL_TYPE:
            out["type"] = val
        elif lbl in SIDE_LABEL_PORTRAIT:
            out["portrait"] = val
    return out


# alias sets for side labels (Italian + English canonicalized)
SIDE_LABEL_LEGEND = frozenset({canonical_label(x) for x in SIDE_LABELS_LEGEND})
SIDE_LABEL_TYPE = frozenset({canonical_label(x) for x in SIDE_LABEL_TYPE})
SIDE_LABEL_PORTRAIT = frozenset({canonical_label(x) for x in SIDE_LABEL_PORTRAIT})


def _query_params(url: str) -> Dict[str, str]:
    return dict(parse_qsl(urlparse(url).query, keep_blank_values=True))


def _canonical_ikmk_wien_style_hostname_key(netloc: str) -> str:
    """
    Host minuscolo per chiavi dedup/export: come ``HostThrottle`` considera la porta se presente,
    ma la chiave stabile ignora la porta e rimuove un solo prefisso ``www.``.
    """
    n = normalize_text(netloc).strip()
    if not n:
        return ""
    # urlparse hostname gestisce anche bracket IPv6 e porta
    ph = urlparse(f"//{n}/")
    h = (ph.hostname or "").lower()
    if len(h) > 4 and h.startswith("www."):
        h = h[4:]
    return h


def _netloc_is_ikmk_wien(netloc: str) -> bool:
    return _canonical_ikmk_wien_style_hostname_key(netloc) in _IKMK_WIEN_STYLE_CANONICAL_HOST_KEYS


def ikmk_wien_style_object_base_url(example_abs_url: str) -> str:
    """``scheme://netloc/object`` in minuscolo sul netloc (mantiene porta se presente)."""
    p = urlparse(normalize_text(example_abs_url).strip())
    scheme = (p.scheme or "https").lower()
    netloc = normalize_text(p.netloc).lower().strip(".")
    return f"{scheme}://{netloc}/object"


def ikmk_wien_style_export_slug_from_example_url(abs_url: str) -> str:
    """
    Slug cartella ``images`` / campo ``source``: ``khm_wien`` solo per ``ikmk.at`` (KHM Wien);
    altre istanze IKMK stile Wien → ``slugify_es`` dell'hostname canonico (nessuna collisione tra musei).
    """
    hkey = _canonical_ikmk_wien_style_hostname_key(urlparse(normalize_text(abs_url).strip()).netloc)
    if hkey == "ikmk.at":
        return EXPORT_IMAGE_SOURCE_BY_EXAMPLE_KIND["IKMK_WIEN"]
    slug = slugify_es(hkey)
    return slug if slug else EXPORT_IMAGE_SOURCE_BY_EXAMPLE_KIND["IKMK_WIEN"]



def normalize_ikmk_wien_object_id(raw: str) -> Optional[str]:
    """Canonico ``ID`` + sole cifre (case-sensitive ``ID``), da ``id56829`` o ``ID56829``."""
    s = normalize_text(raw).strip()
    if not s:
        return None
    if re.match(r"^ID\d+$", s):
        return s
    m = re.match(r"^id(\d+)$", s, re.I)
    if m:
        return "ID" + m.group(1)
    return None


def _ikmk_catalog_object_path_ok(abs_url: str) -> bool:
    segs = [x for x in normalize_text(urlparse(abs_url).path).split("/") if x]
    return bool(segs) and segs[-1].lower() == "object"


def _should_skip_example_host(netloc_lower: str) -> bool:
    return any(h in netloc_lower for h in SKIP_IMAGE_EXAMPLE_HOST_HINTS)


def is_numismatics_mantis_collection_candidate(abs_url: str) -> bool:
    """
    Pagina oggetti MANTIS su numismatics.org: .../collection/<id> senza prefisso /collection/object/.
    Altri musei sullo stesso path pattern: filtrati da SKIP_IMAGE_EXAMPLE_HOST_HINTS sul netloc (se non vuoto).
    """
    p = urlparse(abs_url)
    if "numismatics.org" not in p.netloc.lower():
        return False
    if _should_skip_example_host(p.netloc.lower()):
        return False
    path = normalize_text(p.path).rstrip("/")
    seg = path.split("/")
    if len(seg) == 3 and seg[1] == "collection" and seg[2]:
        return True
    return False


def iter_unique_examples_absolute_urls(soup: BeautifulSoup, base_url: str) -> List[str]:
    """Tutti i link #examples (ordine DOM), dedup case-insensitive su URL canonico."""
    block = soup.select_one("#examples")
    if block is None:
        return []
    seen: set[str] = set()
    out: List[str] = []
    for a in block.select("a[href]"):
        href_raw = normalize_text(a.get("href"))
        if not href_raw or href_raw.startswith("#"):
            continue
        abs_u = normalize_text(urljoin(base_url, href_raw))
        if not abs_u:
            continue
        low = abs_u.lower()
        if low in seen:
            continue
        seen.add(low)
        out.append(abs_u)
    return out


def iter_numismatics_mantis_example_urls(soup: BeautifulSoup, base_url: str) -> List[str]:
    """Solo candidate numismatics.org/collection/&lt;singolo-segmento&gt; in ordine Esempi."""
    return [
        u
        for u in iter_unique_examples_absolute_urls(soup, base_url)
        if is_numismatics_mantis_collection_candidate(u)
    ]


def iter_ikmk_example_object_ids(soup: BeautifulSoup, base_url: str) -> List[str]:
    """ID numerici Berlin ``ikmk.smb.museum`` in ordine Esempi, senza ripetizioni."""
    block = soup.select_one("#examples")
    if block is None:
        return []
    seen_set: set[str] = set()
    ids: List[str] = []
    for a in block.select("a[href]"):
        href_raw = normalize_text(a.get("href"))
        if not href_raw:
            continue
        abs_u = normalize_text(urljoin(base_url, href_raw))
        if not abs_u:
            continue
        parsed = urlparse(abs_u)
        if IKMK_HOST not in parsed.netloc.lower():
            continue
        oid = normalize_text(_query_params(abs_u).get("id", "")).strip()
        if not oid or not re.match(r"^\d+$", oid):
            continue
        if oid in seen_set:
            continue
        seen_set.add(oid)
        ids.append(oid)
    return ids


def iter_ikmk_wien_example_object_ids(soup: BeautifulSoup, base_url: str) -> List[str]:
    """
    ID oggetti cataloghi IKMK “stile Wien” (``ikmk.at``, Tuebingen, Freiburg, Freiberg NUMID, Bonn, …):
    param ``id`` con prefisso ``ID`` + cifre. Ordine DOM; dedup per coppia ``(host_canonico, id)``.
    Ogni elemento è ``"{host_key}:{wid}"`` così lo stesso ``ID…`` su musei diversi resta distinguibile.
    """
    block = soup.select_one("#examples")
    if block is None:
        return []
    seen_set: set[Tuple[str, str]] = set()
    ids: List[str] = []
    for a in block.select("a[href]"):
        href_raw = normalize_text(a.get("href"))
        if not href_raw:
            continue
        abs_u = normalize_text(urljoin(base_url, href_raw))
        if not abs_u:
            continue
        parsed = urlparse(abs_u)
        if not _netloc_is_ikmk_wien(parsed.netloc):
            continue
        if not _ikmk_catalog_object_path_ok(abs_u):
            continue
        oid_raw = normalize_text(_query_params(abs_u).get("id", "")).strip()
        wid = normalize_ikmk_wien_object_id(oid_raw)
        if not wid:
            continue
        hk = _canonical_ikmk_wien_style_hostname_key(parsed.netloc)
        pair = (hk, wid)
        if pair in seen_set:
            continue
        seen_set.add(pair)
        ids.append(f"{hk}:{wid}")
    return ids


def is_gallica_ark_document_url(abs_url: str) -> bool:
    if not abs_url or GALLICA_HOST not in urlparse(abs_url).netloc.lower():
        return False
    return GALLICA_ARK_PATH_HINT in normalize_text(abs_url)


def _bm_collection_object_urls_in_g_doc(g_doc: BeautifulSoup, base_url: str) -> List[str]:
    """Tutti gli URL canonical object BM in un blocco .g_doc (ordine DOM)."""
    out: List[str] = []
    seen: set[str] = set()
    for a in g_doc.select("a[href]"):
        cand = normalize_text(urljoin(base_url, normalize_text(a.get("href", ""))))
        if is_british_museum_collection_object_url(cand):
            c = _bm_canonical_object_url(cand)
            if c not in seen:
                seen.add(c)
                out.append(c)
    return out


def is_british_museum_non_object_site_url(abs_url: str) -> bool:
    """
    Host britishmuseum.org ma non una pagina oggetto ``/collection/object/…``.

    Esempi OCRE: link alla home, a sezioni CMS — il supporto BM passa solo dagli URL oggetto.
    """
    u = normalize_text(abs_url)
    if not u:
        return False
    p = urlparse(u)
    if BM_HOST_MARKER not in p.netloc.lower():
        return False
    return not is_british_museum_collection_object_url(u)


def unsupported_example_log_note(url: str) -> str:
    """
    Nota breve per righe ``supported: no`` (evita “BM non supportato” quando è solo home/CDN).
    """
    u = normalize_text(url)
    if is_british_museum_non_object_site_url(u):
        return (
            " | hint: British Museum site navigation (not handled); "
            "use the /collection/object/… line in Examples for BM images"
        )
    if _bm_media_url_is_usable(u):
        return (
            " | hint: BM media URL without a handled pattern (or not in #examples <a href>); "
            "prefer the …/collection/object/… line for BM"
        )
    return ""


def iter_gallica_example_urls(soup: BeautifulSoup, base_url: str) -> List[str]:
    """URL documenti Gallica (ark /12148) nella sezione Esempi, ordine DOM, dedup."""
    return [
        u
        for u in iter_unique_examples_absolute_urls(soup, base_url)
        if is_gallica_ark_document_url(u)
    ]


def is_british_museum_collection_object_url(abs_url: str) -> bool:
    """URL pagina oggetto BM ``…/collection/object/…`` (link in #examples su OCRE)."""
    u = normalize_text(abs_url)
    if not u:
        return False
    p = urlparse(u)
    if BM_HOST_MARKER not in p.netloc.lower():
        return False
    seg = [x for x in normalize_text(p.path).split("/") if x]
    if len(seg) < 3:
        return False
    return seg[0].lower() == "collection" and seg[1].lower() == "object"


def _bm_canonical_object_url(abs_url: str) -> str:
    p = urlparse(normalize_text(abs_url).strip())
    path = normalize_text(p.path).rstrip("/").lower()
    net = p.netloc.lower()
    if net.startswith("www."):
        net = net[4:]
    return f"https://{net}{path}"


def _bm_normalize_media_url_large_thumbnail(u: str) -> str:
    """
    Se il basename del path contiene ``small`` (case-insensitive), sostituisce
    la prima occorrenza di ``small`` con ``large`` nel solo basename (es.
    small_00632846_001.jpg → large_00632846_001.jpg). Non modifica le directory.
    """
    raw = normalize_text(u.strip())
    if not raw:
        return raw
    p = urlparse(raw)
    path = p.path or ""
    if "/" not in path:
        dirpart, base = "", path
    else:
        dirpart, base = path.rsplit("/", 1)
    if re.search(r"small", base, flags=re.I):
        base = re.sub(r"small", "large", base, count=1, flags=re.I)
    newpath = f"{dirpart}/{base}" if dirpart else base
    return urlunparse((p.scheme, p.netloc, newpath, p.params, p.query, p.fragment))


def _bm_media_url_is_usable(abs_url: str) -> bool:
    u = normalize_text(abs_url).strip()
    if not u:
        return False
    p = urlparse(u)
    if BM_MEDIA_HOST_MARKER not in p.netloc.lower():
        return False
    return bool(BM_MEDIA_IMAGE_EXT_RE.search(p.path or ""))


def _is_tongeren_handle_example_url(abs_url: str) -> bool:
    u = normalize_text(abs_url).strip()
    if not u:
        return False
    p = urlparse(u)
    if TONGEREN_HANDLE_HOST not in p.netloc.lower():
        return False
    return normalize_text(p.path).startswith(TONGEREN_HANDLE_PREFIX)


def _normalize_tongeren_handle_url(abs_url: str) -> str:
    p = urlparse(normalize_text(abs_url).strip())
    net = p.netloc.lower()
    if net.startswith("www."):
        net = net[4:]
    path = normalize_text(p.path).rstrip("/")
    return urlunparse(("https", net, path, "", "", ""))


def _tongeren_imageproxy_url_is_usable(abs_url: str) -> bool:
    u = normalize_text(abs_url).strip()
    if not u:
        return False
    p = urlparse(u)
    if TONGEREN_PROXY_HOST not in p.netloc.lower():
        return False
    return normalize_text(p.path).lower() == TONGEREN_PROXY_PATH


def _normalize_tongeren_imageproxy_url(abs_url: str) -> str:
    """
    Canonicalizza URL ``imageproxy.ashx`` Tongeren eliminando parametri preview (es. ``maxwidth``)
    e mantenendo i parametri utili all'asset originale.
    """
    u = normalize_text(abs_url).strip()
    if not u:
        return u
    p = urlparse(u)
    pairs = parse_qsl(p.query, keep_blank_values=True)
    keep: List[Tuple[str, str]] = []
    for k, v in pairs:
        lk = normalize_text(k).lower()
        if lk in {"maxwidth", "maxheight", "w", "h"}:
            continue
        keep.append((k, v))
    query = urlencode(keep, doseq=True)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, query, p.fragment))


def _tongeren_object_urls_in_g_doc(g_doc: BeautifulSoup, base_url: str) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for a in g_doc.select("a[href]"):
        cand = normalize_text(urljoin(base_url, normalize_text(a.get("href", ""))))
        if _is_tongeren_handle_example_url(cand):
            c = _normalize_tongeren_handle_url(cand)
            if c not in seen:
                seen.add(c)
                out.append(c)
    return out


def _tongeren_ordered_media_urls_from_g_doc(
    g_doc: BeautifulSoup,
    *,
    base_url: str,
) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []

    def push_raw(href: str) -> None:
        abs_u = normalize_text(urljoin(base_url, href.strip()))
        if not abs_u or not _tongeren_imageproxy_url_is_usable(abs_u):
            return
        final_u = _normalize_tongeren_imageproxy_url(abs_u)
        lk = final_u.lower()
        if lk in seen:
            return
        seen.add(lk)
        ordered.append(final_u)

    for a in g_doc.select("a.thumbImage[href]"):
        push_raw(normalize_text(a.get("href", "")))
    for a in g_doc.select(f'a[href*="{TONGEREN_PROXY_HOST}"]'):
        push_raw(normalize_text(a.get("href", "")))
    for img in g_doc.select("img.combined-thumbnail[src]"):
        push_raw(normalize_text(img.get("src", "")))
    for img in g_doc.select("img.combined-thumbnail[data-src]"):
        push_raw(normalize_text(img.get("data-src", "")))
    for img in g_doc.select(f'img[src*="{TONGEREN_PROXY_HOST}"]'):
        push_raw(normalize_text(img.get("src", "")))
    for img in g_doc.select(f'img[data-src*="{TONGEREN_PROXY_HOST}"]'):
        push_raw(normalize_text(img.get("data-src", "")))
    return ordered


def _bm_ordered_media_urls_from_g_doc(
    g_doc: BeautifulSoup,
    *,
    base_url: str,
) -> List[str]:
    """
    Estrae URL immagine BM dal blocco ``.g_doc`` (ordine: thumb/link, combined, contenitore).

    Include ``a[href*="media.britishmuseum.org"]``, ``img[src|data-src]`` verso il CDN e ``srcset``.
    """
    seen: set[str] = set()
    ordered: List[str] = []

    def push_raw(href: str) -> None:
        abs_u = normalize_text(urljoin(base_url, href.strip()))
        if not abs_u:
            return
        if not _bm_media_url_is_usable(abs_u):
            return
        final_u = _bm_normalize_media_url_large_thumbnail(abs_u)
        lk = final_u.lower()
        if lk in seen:
            return
        seen.add(lk)
        ordered.append(final_u)

    for a in g_doc.select("a.thumbImage[href]"):
        push_raw(normalize_text(a.get("href", "")))
    for a in g_doc.select('a[href*="media.britishmuseum.org"]'):
        push_raw(normalize_text(a.get("href", "")))
    for img in g_doc.select("img.combined-thumbnail[src]"):
        push_raw(normalize_text(img.get("src", "")))
    for img in g_doc.select("img.combined-thumbnail[data-src]"):
        push_raw(normalize_text(img.get("data-src", "")))
    for img in g_doc.select(".gi_c img[src]"):
        push_raw(normalize_text(img.get("src", "")))
    for img in g_doc.select(".gi_c img[data-src]"):
        push_raw(normalize_text(img.get("data-src", "")))
    for img in g_doc.select('img[src*="media.britishmuseum.org"]'):
        push_raw(normalize_text(img.get("src", "")))
    for img in g_doc.select('img[data-src*="media.britishmuseum.org"]'):
        push_raw(normalize_text(img.get("data-src", "")))
    for src in g_doc.select("source[srcset], source[data-srcset]"):
        raw_st = normalize_text(src.get("srcset") or src.get("data-srcset") or "")
        for chunk in raw_st.split(","):
            parts = normalize_text(chunk).split()
            bit = parts[0] if parts else ""
            if bit:
                push_raw(bit)
    return ordered


def parse_bm_example_from_ocre_soup(
    soup: BeautifulSoup,
    *,
    base_url: str,
    object_url: str,
) -> Optional[Dict[str, str]]:
    """
    Risolve immagine BM (unificata) dal solo markup OCRE nella sezione #examples.

    Cerca ``.g_doc`` in cui **qualsiasi** ancora ``a[href]`` punta all'URL oggetto BM
    richiesto (non solo la prima ancora BM nel blocco).
    """
    block = soup.select_one("#examples")
    if block is None:
        return None
    want = _bm_canonical_object_url(object_url)
    for g_doc in block.select(".g_doc"):
        objs = _bm_collection_object_urls_in_g_doc(g_doc, base_url)
        if want not in objs:
            continue
        media_urls = _bm_ordered_media_urls_from_g_doc(g_doc, base_url=base_url)
        if not media_urls:
            return None
        canon_page = normalize_text(object_url.strip())
        return {
            "obverse_image": "",
            "reverse_image": "",
            "unified_image": media_urls[0],
            "user_license": BM_USER_LICENSE_LITERAL,
            "copyright_holder": BM_COPYRIGHT_HOLDER_LITERAL,
            "source_institution": BM_SOURCE_INSTITUTION,
            "parsed_page_url": canon_page,
        }
    return None


def parse_tongeren_example_from_ocre_soup(
    soup: BeautifulSoup,
    *,
    base_url: str,
    object_url: str,
) -> Optional[Dict[str, str]]:
    """
    Risolve immagine Tongeren (unificata) dal solo markup OCRE in ``#examples``.

    Copyright/licenza: OCRE espone questi esempi Tongeren con proxy immagine e metadati
    CC0; il parser usa ``CC0 1.0`` con holder istituzionale come campo descrittivo.
    """
    block = soup.select_one("#examples")
    if block is None:
        return None
    want = _normalize_tongeren_handle_url(object_url)
    for g_doc in block.select(".g_doc"):
        objs = _tongeren_object_urls_in_g_doc(g_doc, base_url)
        if want not in objs:
            continue
        media_urls = _tongeren_ordered_media_urls_from_g_doc(g_doc, base_url=base_url)
        if not media_urls:
            return None
        return {
            "obverse_image": "",
            "reverse_image": "",
            "unified_image": media_urls[0],
            "user_license": TONGEREN_USER_LICENSE_LITERAL,
            "copyright_holder": TONGEREN_COPYRIGHT_HOLDER_LITERAL,
            "source_institution": TONGEREN_SOURCE_INSTITUTION,
            "parsed_page_url": normalize_text(object_url.strip()),
        }
    return None


def parse_bm_media_example_from_ocre_soup(
    soup: BeautifulSoup,
    *,
    base_url: str,
    media_anchor_url: str,
) -> Optional[Dict[str, str]]:
    """
    Stesso dizionario licenza/diritti di BM, quando ``#examples`` espone direttamente
    un'<a href> verso ``media.britishmuseum.org``: trova il ``.g_doc`` che lo contiene.

    Preferisce l'immagine il cui URL normalizzato coincide con ``media_anchor_url``;
    se non coincide (varianti minori), usa la prima immagine CDN del blocco.
    """
    block = soup.select_one("#examples")
    if block is None:
        return None
    want_n = _bm_normalize_media_url_large_thumbnail(
        normalize_text(urljoin(base_url, normalize_text(media_anchor_url)))
    ).lower()
    for g_doc in block.select(".g_doc"):
        media_urls = _bm_ordered_media_urls_from_g_doc(g_doc, base_url=base_url)
        if not media_urls:
            continue
        in_block = any(u.lower() == want_n for u in media_urls)
        if not in_block:
            for tag in g_doc.select("a[href], img[src], img[data-src]"):
                if tag.name == "a":
                    href = normalize_text(urljoin(base_url, tag.get("href") or ""))
                else:
                    href = normalize_text(urljoin(base_url, tag.get("src") or ""))
                    if not href:
                        href = normalize_text(urljoin(base_url, tag.get("data-src") or ""))
                if not href:
                    continue
                if _bm_normalize_media_url_large_thumbnail(href).lower() == want_n:
                    in_block = True
                    break
        if not in_block:
            continue
        chosen = ""
        for u in media_urls:
            if u.lower() == want_n:
                chosen = u
                break
        chosen = chosen or media_urls[0]
        obj_list = _bm_collection_object_urls_in_g_doc(g_doc, base_url)
        best_obj = obj_list[0] if obj_list else normalize_text(media_anchor_url.strip())
        return {
            "obverse_image": "",
            "reverse_image": "",
            "unified_image": chosen,
            "user_license": BM_USER_LICENSE_LITERAL,
            "copyright_holder": BM_COPYRIGHT_HOLDER_LITERAL,
            "source_institution": BM_SOURCE_INSTITUTION,
            "parsed_page_url": best_obj,
        }
    return None


def classify_example_url(abs_url: str) -> Tuple[str, bool, Optional[str], Optional[str]]:
    """
    (kind, supported, fetch_dedup_key, ikmk_object_id).
    fetch_dedup_key unisce fetch univoci; quarto elemento: id Berlin numerico o id catalogo Wien-style ``ID…``;
    per ``IKMK_WIEN`` la chiave è ``ikmk_wien:<host_canonico>:<ID…>`` (stesso id su musei diversi → fetch distinti).
    kind ``BM`` / ``BM_MEDIA`` risolvono il dict sul markup OCRE (nessun GET al sito BM).
    """
    p = urlparse(abs_url)
    host = p.netloc.lower()
    if is_numismatics_mantis_collection_candidate(abs_url):
        return ("MANTIS", True, f"mantis:{normalize_text(abs_url).lower()}", None)
    if IKMK_HOST in host:
        oid = normalize_text(_query_params(abs_url).get("id", "")).strip()
        if oid and re.match(r"^\d+$", oid):
            return ("IKMK", True, f"ikmk:{oid}", oid)
        return ("IKMK", False, None, None)
    if _netloc_is_ikmk_wien(p.netloc):
        if not _ikmk_catalog_object_path_ok(abs_url):
            return ("OTHER", False, None, None)
        oid_raw = normalize_text(_query_params(abs_url).get("id", "")).strip()
        wid = normalize_ikmk_wien_object_id(oid_raw)
        if wid:
            hkey = _canonical_ikmk_wien_style_hostname_key(p.netloc)
            return ("IKMK_WIEN", True, f"ikmk_wien:{hkey}:{wid}", wid)
        return ("IKMK_WIEN", False, None, None)
    if is_gallica_ark_document_url(abs_url):
        return ("GALLICA", True, f"gallica:{normalize_text(abs_url).lower()}", None)
    if _is_tongeren_handle_example_url(abs_url):
        norm = _normalize_tongeren_handle_url(abs_url)
        if norm:
            return ("TONGEREN", True, f"tongeren:{norm.lower()}", None)
    if _bm_media_url_is_usable(abs_url):
        norm = _bm_normalize_media_url_large_thumbnail(normalize_text(abs_url))
        if norm:
            return ("BM_MEDIA", True, f"bm_media:{norm.lower()}", None)
    if is_british_museum_collection_object_url(abs_url):
        return ("BM", True, f"bm:{normalize_text(abs_url).lower()}", None)
    return ("OTHER", False, None, None)


def _faces_rights_availability(parsed: Optional[Dict[str, str]]) -> Tuple[str, str]:
    """(faces, rights) ognuno: ok | partial | missing."""
    if not parsed:
        return ("missing", "missing")
    layout, uni, obv, rev = infer_image_layout_and_urls(parsed)
    if layout == "unified":
        fl = "ok" if uni else "missing"
    elif layout == "split":
        if obv and rev:
            fl = "ok"
        elif obv or rev:
            fl = "partial"
        else:
            fl = "missing"
    elif obv or rev:
        fl = "partial"
    else:
        fl = "missing"
    lic = normalize_text(parsed.get("user_license", ""))
    cr = normalize_text(parsed.get("copyright_holder", ""))
    if lic and cr:
        rl = "ok"
    elif lic or cr:
        rl = "partial"
    else:
        rl = "missing"
    return (fl, rl)


def _fetch_parsed_for_example(
    session: requests.Session,
    kind: str,
    url: str,
    ikmk_id: Optional[str],
    *,
    ocre_soup: Optional[BeautifulSoup] = None,
    ocre_base_url: str = "",
    throttle: Optional[HostThrottle] = None,
    timing: Optional[TimingStats] = None,
    fault_cfg: Optional[FaultToleranceConfig] = None,
    outage_guard: Optional[OutageGuard] = None,
) -> Optional[Dict[str, str]]:
    if kind == "MANTIS":
        return try_fetch_and_parse_numismatics_collection(
            session,
            url,
            throttle=throttle,
            timing=timing,
            fault_cfg=fault_cfg,
            outage_guard=outage_guard,
        )
    if kind == "IKMK" and ikmk_id:
        return try_fetch_ikmk_image_set(
            session,
            ikmk_id,
            throttle=throttle,
            timing=timing,
            fault_cfg=fault_cfg,
            outage_guard=outage_guard,
        )
    if kind == "IKMK_WIEN" and ikmk_id:
        return try_fetch_ikmk_wien_image_set(
            session,
            ikmk_id,
            example_url=url,
            throttle=throttle,
            timing=timing,
            fault_cfg=fault_cfg,
            outage_guard=outage_guard,
        )
    if kind == "GALLICA":
        return try_fetch_and_parse_gallica(
            session,
            url,
            throttle=throttle,
            timing=timing,
            fault_cfg=fault_cfg,
            outage_guard=outage_guard,
        )
    if kind == "TONGEREN":
        if ocre_soup is None:
            return None
        return parse_tongeren_example_from_ocre_soup(
            ocre_soup,
            base_url=ocre_base_url,
            object_url=url,
        )
    if kind == "BM":
        if ocre_soup is None:
            return None
        return parse_bm_example_from_ocre_soup(
            ocre_soup,
            base_url=ocre_base_url,
            object_url=url,
        )
    if kind == "BM_MEDIA":
        if ocre_soup is None:
            return None
        return parse_bm_media_example_from_ocre_soup(
            ocre_soup,
            base_url=ocre_base_url,
            media_anchor_url=url,
        )
    return None


def _canonicalize_gallica_ark_slug(slug: str) -> str:
    s = normalize_text(slug).strip("/")
    for suf in (".item", ".double"):
        if s.lower().endswith(suf.lower()):
            s = s[: -len(suf)]
            break
    return s


def _gallica_document_identifier_from_page_url(page_url: Optional[str]) -> Optional[str]:
    if not page_url:
        return None
    m = re.search(r"/12148/([^/?#]+)", page_url, re.I)
    return _canonicalize_gallica_ark_slug(m.group(1)) if m else None


def _normalize_gallica_highres_url(raw: str) -> str:
    u = normalize_text(raw.strip().rstrip("\\").rstrip("/"))
    u = _gallica_canonical_https_scheme(u.split("?")[0])
    return u


def _gallica_canonical_https_scheme(u: str) -> str:
    u = normalize_text(u)
    u = re.sub(r"^https://www\.gallica\.bnf\.fr", "https://gallica.bnf.fr", u, flags=re.I)
    u = re.sub(r"^http://gallica\.bnf\.fr", "https://gallica.bnf.fr", u, flags=re.I)
    return u


def _gallica_highres_slug_from_url(u: str) -> Optional[str]:
    m = re.search(r"/12148/([^/?#]+)/f\d+\.highres", u, re.I)
    return _canonicalize_gallica_ark_slug(m.group(1)) if m else None


def _gallica_dominant_document_slug(highres_urls: Iterable[str]) -> Optional[str]:
    counts: Dict[str, int] = {}
    for u in highres_urls:
        slug = _gallica_highres_slug_from_url(u)
        if slug:
            key = slug.lower()
            counts[key] = counts.get(key, 0) + 1
    if not counts:
        return None
    return max(counts, key=counts.get)


def _gallica_collect_highres_url_strings(html: str) -> List[str]:
    """
    Estrazione ampia (HTML grezzo + stringhe JSON.parse) per ridurre perdite quando un solo
    lato è in un blob grande o quoting misto.
    """
    blob = normalize_text(html)
    blob = blob.replace("\\/", "/")
    out: List[str] = []
    out.extend(GALLICA_HIGHRES_URL_RE.findall(blob))
    for rm in RELATIVE_GALLICA_HIGHRES_RE.finditer(blob):
        out.append(f"https://gallica.bnf.fr/ark:/12148/{rm.group(1)}/f{rm.group(2)}.highres")
    for rx in (JSON_PARSE_SINGLE, JSON_PARSE_DOUBLE):
        for m in rx.finditer(html):
            inner = normalize_text(m.group(1)).replace("\\/", "/")
            out.extend(GALLICA_HIGHRES_URL_RE.findall(inner))
            for rm in RELATIVE_GALLICA_HIGHRES_RE.finditer(inner):
                out.append(
                    f"https://gallica.bnf.fr/ark:/12148/{rm.group(1)}/f{rm.group(2)}.highres"
                )
    return out


def _unique_preserve_order(urls: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for u in urls:
        nu = _normalize_gallica_highres_url(u)
        if nu and nu not in seen:
            seen.add(nu)
            out.append(nu)
    return out


def _gallica_html_suggests_two_faces(html: str) -> bool:
    return any(re.search(pat, html) for pat in GALLICA_TWO_FACE_HINTS)


def _gallica_synthesize_face_url(face1_url: str, target_face: int) -> Optional[str]:
    u = _normalize_gallica_highres_url(face1_url)
    m = re.match(r"^(.*?/f)\d+(\.highres)$", u, re.I)
    if not m:
        return None
    return normalize_text(m.group(1) + str(target_face) + m.group(2))


def _scoped_highres_for_document_slug(
    highres_ordered: List[str],
    slug: Optional[str],
) -> List[str]:
    if not slug:
        return highres_ordered
    want = slug.lower()
    scoped = []
    for u in highres_ordered:
        hid = _gallica_highres_slug_from_url(u)
        if hid and hid.lower() == want:
            scoped.append(u)
    return scoped if scoped else highres_ordered


def extract_gallica_obverse_reverse_highres(
    html: str,
    page_url: Optional[str] = None,
) -> Optional[Tuple[str, str]]:
    """
    Coppia …/f1.highres (dritto) e …/f2.highres (rovescio) per il documento richiesto.
    Restringe gli URL allo stesso ARK della pagina (page_url); se nell'HTML compaiono altri ARK non li mescola.
    Se il viewer dichiara ~2 facce ma manca solo f2, sintetizza f2 sostituendo l'indice nella stessa URL f1.
    """
    slug_request = _gallica_document_identifier_from_page_url(page_url)
    raw = _gallica_collect_highres_url_strings(html)
    uniq = _unique_preserve_order(raw)
    dominant_slug = _gallica_dominant_document_slug(uniq)
    slug_eff = slug_request or dominant_slug
    scoped = _scoped_highres_for_document_slug(uniq, slug_eff)
    by_n: Dict[int, str] = {}
    for u in scoped:
        m = re.search(r"/f(\d+)\.highres$", normalize_text(u), re.I)
        if m:
            by_n[int(m.group(1))] = normalize_text(u)
    hints_two = _gallica_html_suggests_two_faces(html)
    if 1 in by_n and 2 in by_n and by_n[1] != by_n[2]:
        return normalize_text(by_n[1]), normalize_text(by_n[2])
    if 1 in by_n and 2 not in by_n and hints_two:
        cand = _gallica_synthesize_face_url(by_n[1], 2)
        if cand and cand != by_n[1]:
            return normalize_text(by_n[1]), cand
    nums = sorted(by_n.keys())
    if len(nums) >= 2:
        a, b = nums[0], nums[1]
        if by_n[a] != by_n[b]:
            return normalize_text(by_n[a]), normalize_text(by_n[b])
        return None
    if len(nums) == 1 and hints_two:
        lone = nums[0]
        u0 = normalize_text(by_n[lone])
        if lone == 1:
            cand = _gallica_synthesize_face_url(u0, 2)
            if cand and cand != u0:
                return u0, cand
        elif lone == 2:
            cand = _gallica_synthesize_face_url(u0, 1)
            if cand and cand != u0:
                return cand, u0
    return None


def extract_gallica_unified_highres_fallback(
    html: str,
    page_url: Optional[str] = None,
) -> Optional[str]:
    """
    Una sola URL *.highres per il documento (stesso slug ARK della pagina) quando
    la coppia dritto/rovescio non è ricostruibile. Evita falsi unified se il viewer
    dichiara più facce e compaiono più URL distinti tra gli .highres.
    """
    slug_request = _gallica_document_identifier_from_page_url(page_url)
    raw = _gallica_collect_highres_url_strings(html)
    uniq = _unique_preserve_order(raw)
    dominant_slug = _gallica_dominant_document_slug(uniq)
    slug_eff = slug_request or dominant_slug
    scoped = _scoped_highres_for_document_slug(uniq, slug_eff)
    if not scoped:
        return None
    hints_two = _gallica_html_suggests_two_faces(html)
    distinct: List[str] = []
    seen: set[str] = set()
    for u in scoped:
        nu = normalize_text(u)
        if nu and nu not in seen:
            seen.add(nu)
            distinct.append(nu)
    if len(distinct) != 1:
        return None
    only = normalize_text(distinct[0])
    if hints_two:
        by_n: Dict[int, str] = {}
        for u in scoped:
            m = re.search(r"/f(\d+)\.highres$", normalize_text(u), re.I)
            if m:
                by_n[int(m.group(1))] = normalize_text(u)
        nums = sorted(by_n.keys())
        if len(nums) >= 2 and by_n[nums[0]] != by_n[nums[-1]]:
            # Documento a due facce reale ma senza sintesi f2 → non unificare.
            return None
    return only


def try_fetch_and_parse_gallica(
    session: requests.Session,
    abs_url: str,
    *,
    throttle: Optional[HostThrottle] = None,
    timing: Optional[TimingStats] = None,
    fault_cfg: Optional[FaultToleranceConfig] = None,
    outage_guard: Optional[OutageGuard] = None,
) -> Optional[Dict[str, str]]:
    """
    Pagina Gallica: coppia f1/f2 .highres quando possibile (split); altrimenti, una sola
    .highres distintiva sul documento (unified + log euristica).
    Diritti fissi come da requisito progetto.
    """
    try:
        if throttle is not None:
            slp = throttle.wait_before_request(abs_url)
            if timing is not None:
                timing.add_throttle_sleep(slp)
        r = request_with_fault_tolerance(
            session,
            "GET",
            abs_url,
            timeout=DEFAULT_REQUEST_TIMEOUT_SEC,
            fault_cfg=fault_cfg or FaultToleranceConfig(),
            outage_guard=outage_guard,
        )
        if r.status_code in (401, 403, 429):
            return None
        r.raise_for_status()
        r.encoding = r.apparent_encoding or "utf-8"
        html = r.text
        pair = extract_gallica_obverse_reverse_highres(html, page_url=r.url)
        if pair:
            obv, rev = pair
            obv = normalize_text(obv)
            rev = normalize_text(rev)
            if not obv or not rev:
                return None
            return {
                "obverse_image": obv,
                "reverse_image": rev,
                "user_license": GALLICA_USER_LICENSE_LITERAL,
                "copyright_holder": GALLICA_COPYRIGHT_HOLDER_LITERAL,
                "source_institution": GALLICA_SOURCE_INSTITUTION,
                "parsed_page_url": normalize_text(r.url),
            }

        uni = extract_gallica_unified_highres_fallback(html, page_url=r.url)
        if uni:
            disp = uni if len(uni) <= 100 else uni[:97] + "..."
            print(
                f"  [Gallica] unified heuristic: singola vista .highres (coppia f1/f2 assente): {disp}",
                flush=True,
            )
            return {
                "obverse_image": "",
                "reverse_image": "",
                "unified_image": uni,
                "user_license": GALLICA_USER_LICENSE_LITERAL,
                "copyright_holder": GALLICA_COPYRIGHT_HOLDER_LITERAL,
                "source_institution": GALLICA_SOURCE_INSTITUTION,
                "parsed_page_url": normalize_text(r.url),
            }
        return None
    except requests.RequestException:
        return None
    except Exception:
        return None


def parse_rights_section_mantis_like(soup: BeautifulSoup) -> Tuple[str, str]:
    """Sezione Diritti/Rights sulle pagine collection numismatics (markup tipo MANTIS)."""
    user_license = ""
    copyright_holder = ""
    rights_h3 = None
    for h3 in soup.find_all("h3"):
        t = normalize_text(h3.get_text()).lower()
        if t in ("diritti", "rights"):
            rights_h3 = h3
            break
    if rights_h3:
        sec = rights_h3.find_parent("div", class_="metadata_section") or rights_h3.parent
        for li in sec.find_all("li"):
            b = li.find("b", recursive=False)
            if not b:
                continue
            lbl = canonical_label(b.get_text())
            if lbl in COPYRIGHT_KEYS:
                copyright_holder = li_field_value(li)
            elif lbl in LICENSE_KEYS or lbl.lower().startswith("license"):
                user_license = li_field_value(li)
    return user_license, copyright_holder


def collection_label_from_definition_list(soup: BeautifulSoup) -> str:
    for dt in soup.find_all("dt"):
        lbl = canonical_label(dt.get_text()).lower()
        if lbl in ("collection", "collezione"):
            dd = dt.find_next_sibling("dd")
            if dd:
                return normalize_text(dd.get_text(separator=" ", strip=True))
            return ""
    return ""


def derive_institution_display_name(copyright_holder: str, coll_label: str, final_page_url: str) -> str:
    if normalize_text(copyright_holder):
        return normalize_text(copyright_holder)
    if normalize_text(coll_label):
        return normalize_text(coll_label)
    h = urlparse(final_page_url).netloc.strip()
    return normalize_text(h) or normalize_text(final_page_url)


def try_fetch_and_parse_numismatics_collection(
    session: requests.Session,
    abs_url: str,
    *,
    throttle: Optional[HostThrottle] = None,
    timing: Optional[TimingStats] = None,
    fault_cfg: Optional[FaultToleranceConfig] = None,
    outage_guard: Optional[OutageGuard] = None,
) -> Optional[Dict[str, str]]:
    """
    Scarica una pagina /collection/&lt;id&gt; numismatics. Se 403/struttura inattesa / immagini
    incomplete → None per passare all'esempio successivo.
    """
    try:
        p = urlparse(abs_url)
        req_url = with_lang_it(abs_url) if "numismatics.org" in p.netloc.lower() else abs_url
        if throttle is not None:
            slp = throttle.wait_before_request(req_url)
            if timing is not None:
                timing.add_throttle_sleep(slp)
        r = request_with_fault_tolerance(
            session,
            "GET",
            req_url,
            timeout=DEFAULT_REQUEST_TIMEOUT_SEC,
            fault_cfg=fault_cfg or FaultToleranceConfig(),
            outage_guard=outage_guard,
        )
        if r.status_code in (401, 403):
            return None
        if r.status_code == 429:
            return None
        r.raise_for_status()
        r.encoding = r.apparent_encoding or "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        containers = soup.select(".image-container")
        if len(containers) < 2:
            return None
        obv = normalize_text(_image_from_container(containers[0]))
        rev = normalize_text(_image_from_container(containers[1]))
        if not obv or not rev:
            return None
        lic, ch = parse_rights_section_mantis_like(soup)
        coll_txt = collection_label_from_definition_list(soup)
        inst = derive_institution_display_name(ch, coll_txt, r.url)
        return {
            "obverse_image": obv,
            "reverse_image": rev,
            "user_license": lic if isinstance(lic, str) else (str(lic) if lic is not None else ""),
            "copyright_holder": normalize_text(ch),
            "source_institution": normalize_text(inst),
            "parsed_page_url": normalize_text(r.url),
        }
    except requests.RequestException:
        return None
    except Exception:
        return None


def parse_ikmk_og_image_url(html: str, response_url: str) -> str:
    """Da pagina vista IKM estrae meta og:image (_org preferito tramite contenuto della pagina)."""
    soup = BeautifulSoup(html, "html.parser")
    meta = soup.find("meta", property="og:image")
    if not meta or not meta.get("content"):
        return ""
    rel = normalize_text(meta["content"])
    p = urlparse(response_url)
    base = f"{p.scheme}://{p.netloc}/"
    return normalize_text(urljoin(base, rel))


def parse_ikmk_wien_face_image_url(html: str, response_url: str) -> str:
    """
    Immagine per una vista vs/rs KHM Wien: preferisce ``img_path_main`` nel JS della pagina,
    altrimenti ``og:image`` (percorsi relativi risolti con ``response_url``).
    """
    m = _IKMK_IMG_PATH_MAIN_RE.search(html)
    if m:
        rel = normalize_text(m.group(2))
        if rel:
            p = urlparse(response_url)
            base = f"{p.scheme}://{p.netloc}/"
            return normalize_text(urljoin(base, rel))
    return parse_ikmk_og_image_url(html, response_url)


def parse_ikmk_wien_rights_from_html(
    html: str,
    *,
    catalogue_hostname_key: str = "",
) -> Tuple[str, str]:
    """
    Licenza CC (preferenza link AT 3.0 dove presente, altrimenti primo deed Creative Commons in pagina)
    e titolare dal testo ``Münzkabinett…`` se rilevabile.

    Per ``ikmk.at`` (KHM Wien) si conservano i default Austria (deed AT + base copyright) se la pagina
    non offre alternative; su altre istanze IKMK stile Wien si evitano assunzioni legali austriache
    se non supportate dal markup.
    """
    soup = BeautifulSoup(html, "html.parser")
    user_license = ""
    for a in soup.find_all("a", href=True):
        href = normalize_text(a.get("href", ""))
        if "creativecommons.org/licenses/by-nc-sa/3.0/at" in href.lower():
            txt = normalize_text(a.get_text())
            user_license = txt if txt else href
            break
    if not user_license:
        for a in soup.find_all("a", href=True):
            href = normalize_text(a.get("href", "")).lower()
            if "creativecommons.org/licenses/" not in href:
                continue
            raw = normalize_text(a.get("href", ""))
            txt = normalize_text(a.get_text())
            user_license = txt if txt else raw
            break
    if not user_license:
        m = _IKMK_WIEN_CC_HREF_RE.search(html)
        if m:
            user_license = normalize_text(m.group(0))
    if not user_license:
        m_any = re.search(
            r'https://creativecommons\.org/licenses/[^\s"\'<>]+',
            html,
            re.I,
        )
        if m_any:
            user_license = normalize_text(m_any.group(0))
    if not user_license and catalogue_hostname_key == "ikmk.at":
        user_license = IKMK_WIEN_CC_DEED_URL

    copyright_holder = ""
    m_m = re.search(
        r"(Münzkabinett\s*,\s*Kunsthistorisches Museum[^<\n]{0,200})",
        html,
        re.I,
    )
    if m_m:
        copyright_holder = normalize_text(m_m.group(1)).rstrip(",.; ")
    elif re.search(r"Münzkabinett", html, re.I):
        m_g = re.search(r"(Münzkabinett[^\n<]{10,180})", html, re.I)
        if m_g:
            copyright_holder = normalize_text(m_g.group(1)).rstrip(",.; ")
    if not copyright_holder and catalogue_hostname_key == "ikmk.at":
        copyright_holder = DEFAULT_WIEN_COPYRIGHT_BASE
    photo_note = ""
    if re.search(r"Margit\s+Redl", html, re.I):
        photo_note = "Photographs by Margit Redl, KHM"
    if photo_note and copyright_holder and photo_note.lower() not in copyright_holder.lower():
        copyright_holder = f"{copyright_holder} ({photo_note})"

    return user_license, copyright_holder


def parse_ikmk_rights_from_html(html: str) -> Tuple[str, str]:
    """Licenza dal testo della pagina IKM + titolare predefinito."""
    soup = BeautifulSoup(html, "html.parser")
    user_license = ""
    copyright_holder = DEFAULT_BERLIN_COPYRIGHT

    for a in soup.find_all("a", href=True):
        t = normalize_text(a.get_text())
        lab = normalize_text(a.get("title") or "")
        if "public domain mark" in t.lower():
            user_license = t
            break
        if "public domain mark" in lab.lower():
            user_license = lab
            break
    if not user_license:
        m = re.search(
            r"Public Domain Mark\s*[\d.]*(?:\s*\([^)]+\))?",
            html,
            flags=re.I,
        )
        if m:
            user_license = normalize_text(m.group(0))

    m3 = re.search(
        r"(Münzkabinett[^.]+\.?)\s*,\s*[^<]{0,200}",
        html,
        flags=re.I,
    )
    if m3:
        copyright_holder = normalize_text(m3.group(1))

    return user_license, copyright_holder


def fetch_ikmk_side_image_urls(
    session: requests.Session,
    object_id: str,
    *,
    throttle: Optional[HostThrottle] = None,
    timing: Optional[TimingStats] = None,
    fault_cfg: Optional[FaultToleranceConfig] = None,
    outage_guard: Optional[OutageGuard] = None,
) -> Tuple[str, str, str, str]:
    """
    Recupera URL immagini _org dalla vista vs/rs; usa diritti dalla pagina obverse (vs).
    """
    lic = ""
    ch = ""

    vs_params = {"lang": "en", "id": object_id, "view": "vs"}
    if throttle is not None:
        slp = throttle.wait_before_request(IKMK_OBJECT_URL)
        if timing is not None:
            timing.add_throttle_sleep(slp)
    r_vs = request_with_fault_tolerance(
        session,
        "GET",
        IKMK_OBJECT_URL,
        params=vs_params,
        timeout=DEFAULT_REQUEST_TIMEOUT_SEC,
        fault_cfg=fault_cfg or FaultToleranceConfig(),
        outage_guard=outage_guard,
    )
    r_vs.raise_for_status()
    html_vs = r_vs.text
    obv_img = parse_ikmk_og_image_url(html_vs, r_vs.url)
    lic, ch = parse_ikmk_rights_from_html(html_vs)
    if throttle is not None:
        slp = throttle.wait_before_request(IKMK_OBJECT_URL)
        if timing is not None:
            timing.add_throttle_sleep(slp)
    else:
        time.sleep(REQUEST_DELAY_SEC)

    rs_params = {"lang": "en", "id": object_id, "view": "rs"}
    r_rs = request_with_fault_tolerance(
        session,
        "GET",
        IKMK_OBJECT_URL,
        params=rs_params,
        timeout=DEFAULT_REQUEST_TIMEOUT_SEC,
        fault_cfg=fault_cfg or FaultToleranceConfig(),
        outage_guard=outage_guard,
    )
    r_rs.raise_for_status()
    rev_img = parse_ikmk_og_image_url(r_rs.text, r_rs.url)

    return obv_img, rev_img, lic, ch


def fetch_ikmk_wien_side_image_urls(
    session: requests.Session,
    object_id: str,
    *,
    object_base_url: str,
    throttle: Optional[HostThrottle] = None,
    timing: Optional[TimingStats] = None,
    fault_cfg: Optional[FaultToleranceConfig] = None,
    outage_guard: Optional[OutageGuard] = None,
) -> Tuple[str, str, str, str]:
    """GET ``view=vs`` / ``view=rs`` su ``object_base_url`` (origine del link d'esempio + ``/object``)."""
    lic = ""
    ch = ""
    base = normalize_text(object_base_url).rstrip("/")
    cat_key = _canonical_ikmk_wien_style_hostname_key(urlparse(base).netloc)

    vs_params = {"lang": "en", "id": object_id, "view": "vs"}
    vs_req = f"{base}?{urlencode(vs_params)}"
    if throttle is not None:
        slp = throttle.wait_before_request(vs_req)
        if timing is not None:
            timing.add_throttle_sleep(slp)
    r_vs = request_with_fault_tolerance(
        session,
        "GET",
        base,
        params=vs_params,
        timeout=DEFAULT_REQUEST_TIMEOUT_SEC,
        fault_cfg=fault_cfg or FaultToleranceConfig(),
        outage_guard=outage_guard,
    )
    r_vs.raise_for_status()
    html_vs = r_vs.text
    obv_img = parse_ikmk_wien_face_image_url(html_vs, r_vs.url)
    lic, ch = parse_ikmk_wien_rights_from_html(html_vs, catalogue_hostname_key=cat_key)
    rs_params = {"lang": "en", "id": object_id, "view": "rs"}
    rs_req = f"{base}?{urlencode(rs_params)}"
    if throttle is not None:
        slp = throttle.wait_before_request(rs_req)
        if timing is not None:
            timing.add_throttle_sleep(slp)
    else:
        time.sleep(REQUEST_DELAY_SEC)

    r_rs = request_with_fault_tolerance(
        session,
        "GET",
        base,
        params=rs_params,
        timeout=DEFAULT_REQUEST_TIMEOUT_SEC,
        fault_cfg=fault_cfg or FaultToleranceConfig(),
        outage_guard=outage_guard,
    )
    r_rs.raise_for_status()
    rev_img = parse_ikmk_wien_face_image_url(r_rs.text, r_rs.url)

    return obv_img, rev_img, lic, ch


def try_fetch_ikmk_wien_image_set(
    session: requests.Session,
    object_id: str,
    *,
    example_url: str,
    throttle: Optional[HostThrottle] = None,
    timing: Optional[TimingStats] = None,
    fault_cfg: Optional[FaultToleranceConfig] = None,
    outage_guard: Optional[OutageGuard] = None,
) -> Optional[Dict[str, str]]:
    """Come ``try_fetch_ikmk_image_set`` per cataloghi IKMK stile Wien (vs/rs sulla stessa origine del link)."""
    try:
        obj_base = ikmk_wien_style_object_base_url(example_url)
        o_b, r_b, lic_b, ch_b = fetch_ikmk_wien_side_image_urls(
            session,
            object_id,
            object_base_url=obj_base,
            throttle=throttle,
            timing=timing,
            fault_cfg=fault_cfg,
            outage_guard=outage_guard,
        )
        o_b = normalize_text(o_b)
        r_b = normalize_text(r_b)
        ch_norm = normalize_text(ch_b)
        lic_out = lic_b if isinstance(lic_b, str) else (str(lic_b) if lic_b is not None else "")
        if not (o_b and r_b):
            return None
        if o_b == r_b:
            return None
        ex_p = urlparse(normalize_text(example_url).strip())
        host_disp = normalize_text(ex_p.netloc).lower().strip(".") or _canonical_ikmk_wien_style_hostname_key(
            ex_p.netloc
        )
        holder = ch_norm
        if not holder and _canonical_ikmk_wien_style_hostname_key(ex_p.netloc) == "ikmk.at":
            holder = DEFAULT_WIEN_COPYRIGHT_BASE
        display_name = holder if holder else host_disp
        vs_params = {"lang": "en", "id": object_id, "view": "vs"}
        parsed_vs = normalize_text(f"{obj_base.rstrip('/')}?{urlencode(vs_params)}")
        return {
            "obverse_image": o_b,
            "reverse_image": r_b,
            "user_license": lic_out,
            "copyright_holder": holder,
            "source_institution": display_name,
            "parsed_page_url": parsed_vs,
        }
    except Exception:
        return None


def try_fetch_ikmk_image_set(
    session: requests.Session,
    object_id: str,
    *,
    throttle: Optional[HostThrottle] = None,
    timing: Optional[TimingStats] = None,
    fault_cfg: Optional[FaultToleranceConfig] = None,
    outage_guard: Optional[OutageGuard] = None,
) -> Optional[Dict[str, str]]:
    """
    Come fetch branch in get_images: prova IKMK vs/rs; dict coerente con try_fetch_* collection/Gallica.
    """
    try:
        o_b, r_b, lic_b, ch_b = fetch_ikmk_side_image_urls(
            session,
            object_id,
            throttle=throttle,
            timing=timing,
            fault_cfg=fault_cfg,
            outage_guard=outage_guard,
        )
        o_b = normalize_text(o_b)
        r_b = normalize_text(r_b)
        ch_norm = normalize_text(ch_b)
        lic_out = lic_b if isinstance(lic_b, str) else (str(lic_b) if lic_b is not None else "")
        if not (o_b and r_b):
            return None
        holder = ch_norm or DEFAULT_BERLIN_COPYRIGHT
        display_name = holder if holder else IKMK_HOST
        return {
            "obverse_image": o_b,
            "reverse_image": r_b,
            "user_license": lic_out,
            "copyright_holder": holder,
            "source_institution": display_name,
            "parsed_page_url": normalize_text(
                f"{IKMK_OBJECT_URL}?lang=en&id={object_id}&view=vs"
            ),
        }
    except Exception:
        return None


class CoinImages(NamedTuple):
    """Primo bundle “legacy”: stessi URL usati sopra nei campi top-level prima del download."""

    obverse_url: str
    reverse_url: str
    source_name: str
    user_license: str
    copyright_holder: str


class ResolvedExampleImageSet(NamedTuple):
    """Fetch completo per una dedup_key: layout ``split`` o ``unified``; ordine = prima apparizione OCRE."""

    source_type: str
    candidate_url: str
    parsed: Dict[str, str]


def _export_image_source_slug_for_resolved_example(ent: ResolvedExampleImageSet) -> str:
    if ent.source_type == "IKMK_WIEN":
        return ikmk_wien_style_export_slug_from_example_url(ent.candidate_url)
    return EXPORT_IMAGE_SOURCE_BY_EXAMPLE_KIND.get(ent.source_type, "")


def _legacy_obverse_reverse_urls_from_parsed(p: Mapping[str, str]) -> Tuple[str, str]:
    """Per compat legacy: unified → stesso URL in entrambi i campi; split → URL distinti."""
    lay, uni, o, r = infer_image_layout_and_urls(p)
    if lay == "unified":
        return (normalize_text(uni), normalize_text(uni))
    return (normalize_text(o), normalize_text(r))


def _pick_legacy_default_bundle(collected: List[ResolvedExampleImageSet]) -> CoinImages:
    """Priorità storica: MANTIS → IKMK → Gallica → BM (quest'ultimo solo da markup OCRE)."""
    mant = [x for x in collected if x.source_type == "MANTIS"]
    if mant:
        p = mant[0].parsed
        lo, lr = _legacy_obverse_reverse_urls_from_parsed(p)
        return CoinImages(
            obverse_url=lo,
            reverse_url=lr,
            source_name=normalize_text(p.get("source_institution", "")),
            user_license=normalize_text(p.get("user_license", "")),
            copyright_holder=normalize_text(p.get("copyright_holder", "")),
        )
    ikm = [x for x in collected if x.source_type in ("IKMK", "IKMK_WIEN")]
    if ikm:
        p = ikm[0].parsed
        lo, lr = _legacy_obverse_reverse_urls_from_parsed(p)
        return CoinImages(
            obverse_url=lo,
            reverse_url=lr,
            source_name=normalize_text(p.get("source_institution", "")),
            user_license=normalize_text(p.get("user_license", "")),
            copyright_holder=normalize_text(p.get("copyright_holder", "")),
        )
    gal = [x for x in collected if x.source_type == "GALLICA"]
    if gal:
        p = gal[0].parsed
        lo, lr = _legacy_obverse_reverse_urls_from_parsed(p)
        return CoinImages(
            obverse_url=lo,
            reverse_url=lr,
            source_name=normalize_text(p.get("source_institution", "")),
            user_license=normalize_text(p.get("user_license", "")),
            copyright_holder=normalize_text(p.get("copyright_holder", "")),
        )
    bmus = [x for x in collected if x.source_type in ("BM", "BM_MEDIA", "TONGEREN")]
    if bmus:
        p = bmus[0].parsed
        lo, lr = _legacy_obverse_reverse_urls_from_parsed(p)
        return CoinImages(
            obverse_url=lo,
            reverse_url=lr,
            source_name=normalize_text(p.get("source_institution", "")),
            user_license=normalize_text(p.get("user_license", "")),
            copyright_holder=normalize_text(p.get("copyright_holder", "")),
        )
    return CoinImages("", "", "", "", "")


def discover_log_and_collect_example_images(
    soup: BeautifulSoup,
    *,
    base_url: str,
    session: requests.Session,
    throttle: Optional[HostThrottle] = None,
    timing: Optional[TimingStats] = None,
    fault_cfg: Optional[FaultToleranceConfig] = None,
    outage_guard: Optional[OutageGuard] = None,
) -> Tuple[List[ResolvedExampleImageSet], CoinImages]:
    """
    Log tutte le URL in #examples (ordine DOM). Per gestite fa un solo HTTP attempt per chiave dedup.

    Numerazione salvataggi 1..N: primo fetch ``split`` o ``unified`` completo; per BM / BM_MEDIA
    la dedup sulla raccolta usa l'URL immagine CDN normalizzato (stesso asset da pagina oggetto
    e da link diretto media), oltre alle chiavi letterali fetch.
    """
    urls_all = iter_unique_examples_absolute_urls(soup, base_url)
    # Indice cartella 1…N sul disco ≠ priorità legacy (MANTIS>IKMK>Gallica>BM): ordine DOM qui.
    total = len(urls_all)
    fetch_cache: Dict[str, Optional[Dict[str, str]]] = {}
    collected: List[ResolvedExampleImageSet] = []
    seen_collect_dk: set[str] = set()
    seen_bm_resolved_collect: set[str] = set()
    seen_tongeren_resolved_collect: set[str] = set()

    if total == 0:
        print("  [eso] Nessun esempio in #examples.", flush=True)

    for idx, url in enumerate(urls_all, 1):
        kind, supported, dk, ikm_id = classify_example_url(url)
        host = urlparse(url).netloc or ""
        if not supported:
            note = unsupported_example_log_note(url)
            if kind == "IKMK" and not note:
                note = " | hint: IKM Berlin link without a valid numeric id= parameter"
            elif kind == "IKMK_WIEN" and not note:
                note = (
                    " | hint: IKMK (Wien-style) link without a valid id= parameter (expected ID…digits)"
                )
            print(
                f"  [{idx}/{total}] {kind} ({host}) | {url[:120]}{'…' if len(url) > 120 else ''} "
                f"| supported: no{note}",
                flush=True,
            )
            continue

        assert dk is not None
        if dk not in fetch_cache and kind == "BM":
            for alt in (
                f"bm:{_bm_canonical_object_url(url).lower()}",
                f"bm:{normalize_text(url).lower()}",
            ):
                hit = fetch_cache.get(alt)
                if hit is not None:
                    fetch_cache[dk] = hit
                    break

        if dk not in fetch_cache:
            t_ex0 = time.perf_counter()
            fetch_cache[dk] = _fetch_parsed_for_example(
                session,
                kind,
                url,
                ikm_id,
                ocre_soup=soup,
                ocre_base_url=base_url,
                throttle=throttle,
                timing=timing,
                fault_cfg=fault_cfg,
                outage_guard=outage_guard,
            )
            if timing is not None:
                timing.add_examples(time.perf_counter() - t_ex0)
            if throttle is None:
                time.sleep(REQUEST_DELAY_SEC)

        parsed = fetch_cache.get(dk)
        _bm_register_fetch_cache_aliases(fetch_cache, kind=kind, url=url, parsed=parsed)
        face_l, rights_l = _faces_rights_availability(parsed)
        lg, _, _, _ = infer_image_layout_and_urls(parsed or {})
        disp_layout = lg or "—"
        print(
            f"  [{idx}/{total}] {kind} | "
            f"{url[:110]}{'…' if len(url) > 110 else ''} | "
            f"supported: yes | faces: {face_l} | rights: {rights_l} | layout: {disp_layout}",
            flush=True,
        )

        complete = parsed_image_set_complete(parsed) if parsed else False
        if kind in ("BM", "BM_MEDIA") and not complete:
            print(
                "    [BM] incomplete: nessun URL media.britishmuseum.org (.jpg/.png/…) "
                "nel blocco .g_doc OCRE per questa chiave.",
                flush=True,
            )
        if kind == "TONGEREN" and not complete:
            print(
                "    [TONGEREN] incomplete: nessun URL imageproxy.ashx utile nel blocco .g_doc OCRE "
                "per questo handle.",
                flush=True,
            )
        if complete and parsed is not None:
            bm_ck = (
                _bm_resolved_collect_dedupe_key(parsed)
                if kind in ("BM", "BM_MEDIA")
                else None
            )
            if bm_ck is not None:
                if bm_ck in seen_bm_resolved_collect:
                    continue
                seen_bm_resolved_collect.add(bm_ck)
            tong_ck = (
                _tongeren_resolved_collect_dedupe_key(parsed) if kind == "TONGEREN" else None
            )
            if tong_ck is not None:
                if tong_ck in seen_tongeren_resolved_collect:
                    continue
                seen_tongeren_resolved_collect.add(tong_ck)
            if dk not in seen_collect_dk:
                seen_collect_dk.add(dk)
                collected.append(ResolvedExampleImageSet(kind, url, parsed))

    default_bundle = _pick_legacy_default_bundle(collected)
    return collected, default_bundle


def get_images(
    soup: BeautifulSoup,
    *,
    base_url: str,
    session: requests.Session,
    throttle: Optional[HostThrottle] = None,
    timing: Optional[TimingStats] = None,
    fault_cfg: Optional[FaultToleranceConfig] = None,
    outage_guard: Optional[OutageGuard] = None,
) -> CoinImages:
    """
    Solo bundle legacy dalla priorità MANTIS → IKMK → Gallica → BM dopo discovery completa sulla pagina
    (stampa comunque tutte le candidate tramite discover_log_and_collect_example_images).
    """
    _, default_bundle = discover_log_and_collect_example_images(
        soup,
        base_url=base_url,
        session=session,
        throttle=throttle,
        timing=timing,
        fault_cfg=fault_cfg,
        outage_guard=outage_guard,
    )
    return default_bundle


def parse_main_page(
    url: str,
    session: Optional[requests.Session] = None,
    *,
    throttle: Optional[HostThrottle] = None,
    timing: Optional[TimingStats] = None,
    fault_cfg: Optional[FaultToleranceConfig] = None,
    outage_guard: Optional[OutageGuard] = None,
) -> Tuple[Dict[str, Any], BeautifulSoup, str]:
    """
    Scarica la pagina tipo OCRE (lang=it) e estrae i campi testuali; restituisce anche soup.

    ``ric_id`` deriva da ``page_url`` (URL effettivo della GET, tipicamente dopo ``with_lang_it``),
    così coincide con la richiesta HTTP e ignora query/fragment nel identificativo tipo.
    """
    sess = session or session_with_retries()
    page_url = with_lang_it(url)
    t0 = time.perf_counter()
    html = fetch(
        sess,
        page_url,
        throttle=throttle,
        timing=timing,
        fault_cfg=fault_cfg,
        outage_guard=outage_guard,
    )
    soup = BeautifulSoup(html, "html.parser")
    if timing is not None:
        timing.add_ocre(time.perf_counter() - t0)

    record = build_empty_record()

    h1 = soup.select_one("h1#object_title")
    if h1:
        record["name"] = normalize_text(h1.get_text())
    record["ric_id"] = extract_ocre_type_id_from_url(page_url)

    sec = find_typological_metadata_section(soup)
    record["description"]["date"] = parse_typological_date_array(sec)
    record["description"]["mint"] = first_label_match(sec, LABEL_EQUIV["mint"])
    record["description"]["denomination"] = first_label_match(sec, LABEL_EQUIV["denomination"])
    record["description"]["material"] = first_label_match(sec, LABEL_EQUIV["material"])
    record["description"]["subjects"] = parse_subjects_concept_labels(soup)
    record["authority"]["emperor"] = first_label_match(sec, LABEL_EQUIV["authority"])
    record["authority"]["dynasty"] = first_label_match(sec, LABEL_EQUIV["dynasty"])

    obv_ul = soup.select_one('ul[rel="nmo:hasObverse"]')
    rev_ul = soup.select_one('ul[rel="nmo:hasReverse"]')
    obv_data = parse_side_ul(obv_ul)
    rev_data = parse_side_ul(rev_ul)
    record["obverse"]["legend"] = obv_data["legend"]
    record["obverse"]["type"] = obv_data["type"]
    record["obverse"]["portrait"] = obv_data["portrait"]
    record["reverse"]["legend"] = rev_data["legend"]
    record["reverse"]["type"] = rev_data["type"]

    return record, soup, page_url


def _image_from_container(container: BeautifulSoup) -> str:
    for a in container.select('a[href*=".jpg"]'):
        title = (a.get("title") or "") + " " + (a.get_text() or "")
        href = a.get("href") or ""
        if "noscale" in href or "full" in title.lower() or "risoluzione" in title.lower():
            return normalize_text(href)
    a2 = container.select_one('a[href*=".jpg"]')
    if a2 and a2.has_attr("href"):
        return normalize_text(a2["href"])

    ns = container.find("noscript")
    if ns:
        img = ns.find("img")
        if img and img.has_attr("src"):
            return normalize_text(img["src"])

    iiif = container.select_one(".iiif-container[service]")
    if iiif and iiif.has_attr("service"):
        return normalize_text(iiif["service"])
    return ""


def parse_example_page(url: str, session: Optional[requests.Session] = None) -> Dict[str, str]:
    """Fetch singola pagina numismatics.org/collection/… (stesso parser dei cicli Esempi multipli)."""
    sess = session or session_with_retries()
    parsed = try_fetch_and_parse_numismatics_collection(sess, url)
    if not parsed:
        return {
            "obverse_image": "",
            "reverse_image": "",
            "unified_image": "",
            "user_license": "",
            "copyright_holder": "",
        }
    return {
        "obverse_image": parsed["obverse_image"],
        "reverse_image": parsed["reverse_image"],
        "unified_image": normalize_text(parsed.get("unified_image", "")),
        "user_license": parsed["user_license"],
        "copyright_holder": parsed["copyright_holder"],
    }


def load_coin_urls_from_json(path: Path) -> List[str]:
    """Carica gli URL da monete_links.json (chiave coin_links o lista radice)."""
    text = path.read_text(encoding="utf-8-sig")
    data = json.loads(text)
    if isinstance(data, list):
        return [str(u).strip() for u in data if str(u).strip()]
    links = data.get("coin_links")
    if isinstance(links, list):
        return [str(u).strip() for u in links if str(u).strip()]
    raise ValueError("JSON atteso: array di URL oppure oggetto con chiave coin_links (array)")


def sanitize_coin_filename_base(name: str) -> str:
    """
    Nome file sicuro da name: minuscolo, segmenti tra parentesi rimossi,
    caratteri speciali → '_'.
    """
    n = normalize_text(name)
    if not n:
        return "coin"
    if n.startswith("__error__"):
        raw = normalize_text(n.replace("__error__", "", 1))
        suf = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]
        return f"error_{suf}"
    n = re.sub(r"\([^)]*\)", "", n)
    n = unicodedata.normalize("NFKD", n)
    n = "".join(ch for ch in n if not unicodedata.combining(ch))
    n = n.lower()
    n = re.sub(r"[^a-z0-9]+", "_", n)
    n = re.sub(r"_+", "_", n).strip("_")
    if not n:
        n = "coin"
    return n[:120]


def download_image(
    url: str,
    folder: str | Path,
    filename: str,
    *,
    throttle: Optional[HostThrottle] = None,
    timing: Optional[TimingStats] = None,
) -> bool:
    """Scarica un'immagine in folder/filename (stream); errore → False.

    Per CDN con policy referer (BM / Tongeren), di fronte a HTTP 403 si ritenta con
    ``Referer`` istituzionale e ``Accept`` simile al browser. La verifica TLS usa
    il trust store SSL predefinito; su ``SSLError`` si ritenta senza verifica (log in ``utils.download``).
    """
    if not (url or "").strip():
        return False
    if throttle is not None:
        slp = throttle.wait_before_request(url.strip())
        if timing is not None:
            timing.add_throttle_sleep(slp)
    dest_dir = Path(folder)
    path = dest_dir / filename
    p = urlparse(url.strip())
    host_l = p.netloc.lower()
    is_bm_cdn = BM_MEDIA_HOST_MARKER in host_l
    is_tongeren_proxy = TONGEREN_PROXY_HOST in host_l
    scraper_headers = {
        "User-Agent": (
            "scraperissimo/1.0 (+https://numismatics.org; educational research; respectful crawl)"
        ),
    }
    overlays: List[Dict[str, str]] = [{}]
    if is_bm_cdn:
        overlays.append(
            {
                "Referer": "https://www.britishmuseum.org/",
                "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            }
        )
    if is_tongeren_proxy:
        overlays.append(
            {
                "Referer": TONGEREN_PROXY_REFERER,
                "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            }
        )
    retry_statuses = (403,) if len(overlays) > 1 else ()
    t0 = time.perf_counter()
    ok = _shared_download_image(
        url.strip(),
        path,
        timeout=IMAGE_HTTP_TIMEOUT,
        chunk_size=IMAGE_CHUNK_SIZE,
        headers=scraper_headers,
        header_overlays=overlays,
        skip_if_exists=False,
        retry_http_statuses=retry_statuses,
    )
    if timing is not None:
        timing.add_images(time.perf_counter() - t0)
    return ok


def create_coin_folder(images_root: Path, coin_name: str) -> Path:
    """
    Crea images/<nome_sanificato>/ sotto images_root (exist_ok).
    Restituisce il path della sottocartella moneta.
    """
    sub = sanitize_coin_filename_base(coin_name or "")
    out = images_root / sub
    os.makedirs(out, exist_ok=True)
    return out


def _serialized_images_item(
    index: int,
    layout: str,
    *,
    license_text: Any,
    copyright_holder: str,
    obverse_image: str,
    reverse_image: str,
    unified_image: str,
    source: str = "",
) -> Dict[str, Any]:
    """Una voce di ``images`` con ordine di chiavi stabile (solo path non vuoti in ``files``)."""
    if license_text is None:
        lic = ""
    else:
        lic = license_text if isinstance(license_text, str) else str(license_text)
    ch = normalize_text(copyright_holder)
    lay = normalize_text(layout)
    src = normalize_text(source)
    files: Dict[str, str] = {}
    if lay == "split":
        o = normalize_text(obverse_image)
        r = normalize_text(reverse_image)
        if o:
            files["obverse"] = o
        if r:
            files["reverse"] = r
    elif lay == "unified":
        u = normalize_text(unified_image)
        if u:
            files["unified"] = u
    return {
        "index": index,
        "layout": lay,
        "license": lic,
        "source": src,
        "copyright_holder": ch,
        "files": files,
    }


def save_coin_images_local(
    record: Dict[str, Any],
    images_dir: Path,
    *,
    resolved_sets: Optional[List[ResolvedExampleImageSet]] = None,
    storage_backend: Optional[ImageStorageBackend] = None,
    download_images: bool = True,
    prior_images: Optional[List[Dict[str, Any]]] = None,
    output_base_for_paths: Optional[Path] = None,
    throttle: Optional[HostThrottle] = None,
    timing: Optional[TimingStats] = None,
) -> Tuple[int, List[int]]:
    """
    Scrive sotto ``images/<base>/{N}/`` secondo ``layout`` del set parsato:

    - ``split``: ``obverse.jpg`` e ``reverse.jpg`` da due URL distinti;
    - ``unified``: un solo scarico verso ``unified.jpg``.

    Aggiorna ``record[\"images\"]``: ogni voce ha ``index``, ``layout``, ``license``,
    ``copyright_holder`` e ``files`` (solo chiavi con path non vuoti). Nessun URL remoto.

    Resume: con ``prior_images`` (stesso indice del set; accetta anche JSON legacy
    ``image_sets`` se passato da chi carica ancora il vecchio schema) e
    ``output_base_for_paths`` (cartella del file ``-o``), non riscarica se il path
    relativo nel JSON precedente punta a un file esistente e il ``layout`` salvato
    coincide con quello inferito ora.

    Se ``download_images`` è False non vengono effettuati download HTTP; si possono
    comunque ripopolare i path dal resume se i file esistono sotto ``output_base_for_paths``.

    Ritorna (quante cartelle N hanno almeno un file salvato; indici 1-based ``completi``:
    per split due lati salvati entrambi, per unified file unificato ok).
    """
    resolved_sets = resolved_sets or []
    storage = storage_backend or resolve_image_storage_from_env()
    output_base = output_base_for_paths
    coin_dir: Optional[Path] = None
    base = ""
    if download_images:
        coin_dir = create_coin_folder(images_dir, record.get("name") or "")
        base = coin_dir.name

    if not resolved_sets:
        record["images"] = []
        return 0, []

    rows: List[Dict[str, Any]] = []
    complete_sets: List[int] = []
    any_saved = 0

    for i, ent in enumerate(resolved_sets, 1):
        p = ent.parsed
        lic_raw = p.get("user_license", "")
        if lic_raw is None:
            lic = ""
        elif isinstance(lic_raw, str):
            lic = lic_raw
        else:
            lic = str(lic_raw)
        ch = normalize_text(p.get("copyright_holder", ""))
        img_src = _export_image_source_slug_for_resolved_example(ent)
        layout, uni_url, obv_url, rev_url = infer_image_layout_and_urls(p)
        set_dir = coin_dir / str(i) if coin_dir is not None else None

        prior_row: Optional[Dict[str, Any]] = None
        if prior_images and 0 <= i - 1 < len(prior_images):
            prior_row = prior_images[i - 1]
        prior_layout, prior_paths = _prior_layout_and_files(prior_row)

        if layout == "unified":
            rel_u = ""
            ok_u = False
            can_resume = (
                output_base is not None
                and layouts_match_for_resume(prior_layout, layout)
                and prior_paths.get("unified")
                and disk_file_exists_under_output(output_base, prior_paths["unified"])
            )
            rel_canon = f"{IMAGES_SUBDIR}/{base}/{i}/{UNIFIED_IMAGE_FILENAME}".replace("\\", "/")
            if can_resume:
                rel_u = prior_paths["unified"]
                ok_u = True
                any_saved += 1
                complete_sets.append(i)
                print(f"    [img resume] set {i} unified: skip download (on disk)", flush=True)
            elif download_images and set_dir is not None:
                rel_u = rel_canon
                ok_u = download_image(
                    uni_url,
                    set_dir,
                    UNIFIED_IMAGE_FILENAME,
                    throttle=throttle,
                    timing=timing,
                )
                if ok_u:
                    local_unified = set_dir / UNIFIED_IMAGE_FILENAME
                    stored_ref = storage.finalize_file(local_path=local_unified, relative_path=rel_canon)
                    if stored_ref:
                        rel_u = stored_ref
                        any_saved += 1
                        complete_sets.append(i)
                    else:
                        ok_u = False
                        rel_u = ""
                else:
                    rel_u = ""
            rows.append(
                _serialized_images_item(
                    i,
                    "unified",
                    license_text=lic,
                    copyright_holder=ch,
                    obverse_image="",
                    reverse_image="",
                    unified_image=rel_u if ok_u else "",
                    source=img_src,
                )
            )
        elif layout == "split":
            rel_obv = ""
            rel_rev = ""
            ok_o = False
            ok_r = False
            rel_obv_canon = f"{IMAGES_SUBDIR}/{base}/{i}/obverse.jpg".replace("\\", "/")
            rel_rev_canon = f"{IMAGES_SUBDIR}/{base}/{i}/reverse.jpg".replace("\\", "/")
            layout_ok = layouts_match_for_resume(prior_layout, layout)
            if output_base is not None and layout_ok:
                if (
                    prior_paths.get("obverse")
                    and disk_file_exists_under_output(output_base, prior_paths["obverse"])
                ):
                    rel_obv = prior_paths["obverse"]
                    ok_o = True
                    print(f"    [img resume] set {i} obverse: skip download (on disk)", flush=True)
                if (
                    prior_paths.get("reverse")
                    and disk_file_exists_under_output(output_base, prior_paths["reverse"])
                ):
                    rel_rev = prior_paths["reverse"]
                    ok_r = True
                    print(f"    [img resume] set {i} reverse: skip download (on disk)", flush=True)
            if download_images and set_dir is not None:
                if not ok_o:
                    rel_obv = rel_obv_canon
                    ok_o = download_image(
                        obv_url,
                        set_dir,
                        "obverse.jpg",
                        throttle=throttle,
                        timing=timing,
                    )
                    if ok_o:
                        local_obv = set_dir / "obverse.jpg"
                        stored_obv = storage.finalize_file(local_path=local_obv, relative_path=rel_obv_canon)
                        if stored_obv:
                            rel_obv = stored_obv
                        else:
                            ok_o = False
                            rel_obv = ""
                    if not ok_o:
                        rel_obv = ""
                if not ok_r:
                    rel_rev = rel_rev_canon
                    ok_r = download_image(
                        rev_url,
                        set_dir,
                        "reverse.jpg",
                        throttle=throttle,
                        timing=timing,
                    )
                    if ok_r:
                        local_rev = set_dir / "reverse.jpg"
                        stored_rev = storage.finalize_file(local_path=local_rev, relative_path=rel_rev_canon)
                        if stored_rev:
                            rel_rev = stored_rev
                        else:
                            ok_r = False
                            rel_rev = ""
                    if not ok_r:
                        rel_rev = ""
            if ok_o or ok_r:
                any_saved += 1
            if ok_o and ok_r:
                complete_sets.append(i)
            rows.append(
                _serialized_images_item(
                    i,
                    "split",
                    license_text=lic,
                    copyright_holder=ch,
                    obverse_image=rel_obv if ok_o else "",
                    reverse_image=rel_rev if ok_r else "",
                    unified_image="",
                    source=img_src,
                )
            )
        else:
            rows.append(
                _serialized_images_item(
                    i,
                    "",
                    license_text=lic,
                    copyright_holder=ch,
                    obverse_image="",
                    reverse_image="",
                    unified_image="",
                    source=img_src,
                )
            )

    record["images"] = rows

    return any_saved, complete_sets


def _scrape_one_coin(
    idx_raw: Tuple[int, str],
    *,
    urls: List[str],
    images_dir: Path,
    storage_backend: ImageStorageBackend,
    download_images: bool,
    previous_results: Optional[List[Dict[str, Any]]],
    output_base: Path,
    throttle: Optional[HostThrottle],
    timing_stats: Optional[TimingStats],
    fault_cfg: FaultToleranceConfig,
    outage_guard: Optional[OutageGuard],
) -> Dict[str, Any]:
    """Elabora una moneta (thread-safe: sessione HTTP da ``worker_session()``)."""
    idx, raw_in = idx_raw
    raw = normalize_text(raw_in)
    print(f"[{idx + 1}/{len(urls)}] {raw}", flush=True)
    prev_coin = match_previous_coin_record(previous_results, urls, idx)
    prior_rows: Optional[List[Dict[str, Any]]] = None
    if isinstance(prev_coin, dict):
        ps = prev_coin.get("images")
        if not isinstance(ps, list):
            ps = prev_coin.get("image_sets")
        if isinstance(ps, list):
            prior_rows = ps
    sess = worker_session()
    page_url = with_lang_it(raw)
    for attempt in range(1, fault_cfg.max_task_retries + 2):
        try:
            record, ocre_soup, page_url = parse_main_page(
                raw,
                sess,
                throttle=throttle,
                timing=timing_stats,
                fault_cfg=fault_cfg,
                outage_guard=outage_guard,
            )
            resolved_sets, bundle = discover_log_and_collect_example_images(
                ocre_soup,
                base_url=page_url,
                session=sess,
                throttle=throttle,
                timing=timing_stats,
                fault_cfg=fault_cfg,
                outage_guard=outage_guard,
            )
            if bundle.obverse_url and bundle.reverse_url:
                print(f"    legacy (priorità): {bundle.source_name}", flush=True)
            elif not resolved_sets:
                coin_label = normalize_text(record.get("name")) or raw
                print(
                    f"    [WARNING] Nessun esempio con immagine completa (layout split o unified) per "
                    f"{coin_label}",
                    flush=True,
                )
            dirs_touched, complete_sets = save_coin_images_local(
                record,
                images_dir,
                resolved_sets=resolved_sets,
                storage_backend=storage_backend,
                download_images=download_images,
                prior_images=prior_rows,
                output_base_for_paths=output_base,
                throttle=throttle,
                timing=timing_stats,
            )
            sets_rows = record.get("images") or []
            n_split = sum(1 for row in sets_rows if row.get("layout") == "split")
            n_unified = sum(1 for row in sets_rows if row.get("layout") == "unified")
            if download_images:
                print(
                    f"    images (markup): {len(resolved_sets)} | layout: split={n_split} unified={n_unified} | "
                    f"cartelle con >=1 salvataggio: {dirs_touched} | "
                    f"set completi: {complete_sets if complete_sets else '—'}",
                    flush=True,
                )
            else:
                print(
                    f"    images (markup): {len(resolved_sets)} | layout: split={n_split} unified={n_unified} | "
                    f"download immagini disabilitato (--no-img); path ripristinati da disco se presenti nel JSON precedente",
                    flush=True,
                )
            del ocre_soup
            break
        except Exception as e:
            retryable = _is_retryable_request_error(e) or isinstance(e, ParseProcessingError)
            if retryable and attempt <= fault_cfg.max_task_retries:
                delay = _retry_delay_sec(
                    attempt,
                    base_sec=fault_cfg.backoff_base_sec,
                    max_sec=fault_cfg.backoff_max_sec,
                    jitter_sec=fault_cfg.backoff_jitter_sec,
                )
                logger.warning(
                    "Retry task moneta idx=%s tentativo %s/%s tra %.2fs (%s)",
                    idx,
                    attempt,
                    fault_cfg.max_task_retries,
                    delay,
                    type(e).__name__,
                )
                time.sleep(delay)
                continue
            print(f"    ERRORE: {e}", flush=True)
            record = build_empty_record()
            record["name"] = f"__error__ {raw}"
            record["ric_id"] = extract_ocre_type_id_from_url(raw)
            break
    return record_to_export_payload(record, page_url=page_url)


def scrape_all(
    urls: List[str],
    images_dir: Path,
    *,
    download_images: bool = True,
    previous_results: Optional[List[Dict[str, Any]]] = None,
    max_workers: int = 1,
    min_host_interval_sec: float = REQUEST_DELAY_SEC,
    collect_timing: bool = False,
    storage_backend: Optional[ImageStorageBackend] = None,
    fault_cfg: Optional[FaultToleranceConfig] = None,
    checkpoint_cfg: Optional[CheckpointConfig] = None,
) -> List[Dict[str, Any]]:
    """
    Scorre gli URL moneta in ordine; con ``max_workers`` > 1 usa un pool di thread
    (stessa politica host di default ``min_host_interval_sec`` ≈ 1 s). L'ordine della
    lista restituità coincide sempre con ``urls`` (``executor.map``).

    Scelta del parallelismo: **per moneta** (non dentro ``discover`` / ``save``) per
    diff minima, ``fetch_cache`` ancora locale e legato a una sola pagina OCRE, e overlap
    I/O tra tipi OCRE diversi; download dei set di una stessa moneta resta sequenziale.
    """
    batch_export_timestamps_reset()
    if download_images:
        images_dir.mkdir(parents=True, exist_ok=True)
    output_base = images_dir.parent
    workers = max(1, int(max_workers))
    throttle: Optional[HostThrottle] = None
    if float(min_host_interval_sec) > 0:
        throttle = HostThrottle(float(min_host_interval_sec))
    timing_stats: Optional[TimingStats] = TimingStats() if collect_timing else None
    backend = storage_backend or resolve_image_storage_from_env()
    ft_cfg = fault_cfg or FaultToleranceConfig()
    outage_guard = OutageGuard(ft_cfg)
    checkpoint_done: Set[int] = set()
    checkpoint_results: Dict[int, Dict[str, Any]] = {}
    legacy_inline_ckpt = False
    if checkpoint_cfg is not None:
        checkpoint_done, checkpoint_results, legacy_inline_ckpt = load_checkpoint(checkpoint_cfg.path)
    use_sharded_ckpt = checkpoint_cfg is not None and checkpoint_cfg.sharded
    checkpoint_dirty: Set[int] = set()
    needs_full_sharded_flush = bool(use_sharded_ckpt and legacy_inline_ckpt)

    worker_fn = partial(
        _scrape_one_coin,
        urls=urls,
        images_dir=images_dir,
        storage_backend=backend,
        download_images=download_images,
        previous_results=previous_results,
        output_base=output_base,
        throttle=throttle,
        timing_stats=timing_stats,
        fault_cfg=ft_cfg,
        outage_guard=outage_guard,
    )

    indexed = list(enumerate(urls))
    out_by_idx: Dict[int, Dict[str, Any]] = dict(checkpoint_results)
    to_run = [ir for ir in indexed if ir[0] not in checkpoint_done]
    if checkpoint_done:
        logger.info("Resume da checkpoint: salto %s elementi gia' completati.", len(checkpoint_done))
    completed_since_save = 0

    def persist_checkpoint(*, reset_periodic_counter: bool) -> None:
        nonlocal needs_full_sharded_flush, completed_since_save
        if checkpoint_cfg is None:
            return
        if use_sharded_ckpt:
            save_checkpoint_sharded_atomic(
                checkpoint_cfg.path,
                checkpoint_done,
                out_by_idx,
                dirty_indices=None if needs_full_sharded_flush else set(checkpoint_dirty),
                write_all_parts=needs_full_sharded_flush,
            )
            checkpoint_dirty.clear()
            needs_full_sharded_flush = False
        else:
            save_checkpoint_atomic(checkpoint_cfg.path, checkpoint_done, out_by_idx)
        if reset_periodic_counter:
            completed_since_save = 0

    if workers == 1:
        for ir in to_run:
            idx = ir[0]
            out_by_idx[idx] = worker_fn(ir)
            checkpoint_done.add(idx)
            if use_sharded_ckpt:
                checkpoint_dirty.add(idx)
            completed_since_save += 1
            if checkpoint_cfg is not None and completed_since_save >= max(1, checkpoint_cfg.frequency_items):
                persist_checkpoint(reset_periodic_counter=True)
    else:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(worker_fn, ir): ir[0] for ir in to_run}
            for fut in as_completed(futs):
                idx = futs[fut]
                out_by_idx[idx] = fut.result()
                checkpoint_done.add(idx)
                if use_sharded_ckpt:
                    checkpoint_dirty.add(idx)
                completed_since_save += 1
                if checkpoint_cfg is not None and completed_since_save >= max(1, checkpoint_cfg.frequency_items):
                    persist_checkpoint(reset_periodic_counter=True)

    if checkpoint_cfg is not None:
        persist_checkpoint(reset_periodic_counter=False)
    out = [out_by_idx[i] for i in range(len(urls)) if i in out_by_idx]

    if timing_stats is not None:
        print("[timing]", flush=True)
        for line in timing_stats.summary_lines():
            print(line, flush=True)

    return out


def run_bm_ocre_regression_smoke() -> int:
    """
    Test di regressione contro rete: pagina OCRE tipo con esempi British Museum.

    Verifica classificazione BM, ``parse_bm_example_from_ocre_soup`` → ``unified_image``,
    licenza BM fissata. Tenta il salvataggio di ``unified.jpg`` (TLS verificato e,
    solo se necessario, retry senza verifica come in ``utils.download``). Se il download e
    il probe HEAD falliscono comunque, distingue ``PARTIAL OK`` (solo TLS oltre il fallback)
    da ``FAIL`` (HTTP/rete).
    """
    sess = session_with_retries()
    uri = normalize_text(BM_OCRE_REGRESSION_TYPE_URL)
    print(f"--smoke-bm: fetching {uri}", flush=True)
    record, soup, page_url = parse_main_page(uri, sess)

    bm_url = ""
    saw_bm_line = False
    urls_all = iter_unique_examples_absolute_urls(soup, page_url)
    for ex in urls_all:
        kind, supported, dk, _ = classify_example_url(ex)
        if kind == "BM" and supported:
            saw_bm_line = True
            bm_url = ex
            break
    if not saw_bm_line or not bm_url:
        print(
            "--smoke-bm FAIL: no British Museum /collection/object/ link in Examples (OCRE markup changed?).",
            file=sys.stderr,
        )
        return 2

    parsed = parse_bm_example_from_ocre_soup(soup, base_url=page_url, object_url=bm_url)
    uni = normalize_text(str((parsed or {}).get("unified_image", "") or ""))
    if not parsed or not uni:
        print(
            "--smoke-bm FAIL: parse_bm_example_from_ocre_soup returned no unified_image.",
            file=sys.stderr,
        )
        return 2
    lic = normalize_text(str(parsed.get("user_license", "")))
    if lic != normalize_text(BM_USER_LICENSE_LITERAL):
        print("--smoke-bm FAIL: unexpected BM license literal.", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory() as td:
        images_root = Path(td) / "images_root"
        images_dir = images_root / IMAGES_SUBDIR
        resolved = [ResolvedExampleImageSet("BM", bm_url, parsed)]
        n, complete = save_coin_images_local(
            record,
            images_dir,
            resolved_sets=resolved,
            download_images=True,
            prior_images=None,
            output_base_for_paths=images_root,
        )
        found = list(images_dir.glob("**/unified.jpg"))
        if found and found[0].is_file() and found[0].stat().st_size > 0:
            nbytes = found[0].stat().st_size
            print(
                f"--smoke-bm OK: BM example {bm_url[:80]}… | unified_image present | "
                f"unified.jpg wrote ({nbytes} bytes).",
                flush=True,
            )
            return 0

    # Download fallito: distingua TLS locale vs altro (403, markup, ecc.)
    probe_headers = {
        "User-Agent": (
            "scraperissimo/1.0 (+https://numismatics.org; educational research; respectful crawl)"
        ),
        "Referer": "https://www.britishmuseum.org/",
    }
    try:
        probes = http_request_with_ssl_fallback(
            "HEAD",
            uni,
            timeout=min(25, IMAGE_HTTP_TIMEOUT),
            headers=probe_headers,
            allow_redirects=True,
        )
        hstatus = probes.status_code
    except requests.exceptions.SSLError as e:
        print(
            "--smoke-bm PARTIAL OK: classify + parse_bm_example_from_ocre_soup unified_image OK; "
            "salvataggio JPEG / HEAD falliti per SSLError anche dopo fallback TLS "
            f"({type(e).__name__}).",
            flush=True,
        )
        return 0
    except requests.exceptions.RequestException as e:
        print(
            f"--smoke-bm FAIL: unified.jpg non scritto; probe HEAD fallita ({type(e).__name__}: {e}). "
            f"(save_coin_images_local: dirs={n}, complete={complete})",
            file=sys.stderr,
        )
        return 2

    print(
        "--smoke-bm FAIL: unified.jpg vuota o assente dopo download; "
        f"HEAD BM CDN risponde HTTP {hstatus}. "
        f"(save_coin_images_local: dirs={n}, complete={complete})",
        file=sys.stderr,
    )
    return 2


def run_tongeren_ocre_regression_smoke() -> int:
    """
    Smoke rete per esempio OCRE con handle Tongeren: verifica almeno un set ``TONGEREN``
    completo e scrittura ``unified.jpg`` in cartella temporanea.
    """
    sess = session_with_retries()
    uri = normalize_text(TONGEREN_OCRE_REGRESSION_TYPE_URL)
    print(f"--smoke-tongeren: fetching {uri}", flush=True)
    record, soup, page_url = parse_main_page(uri, sess)
    collected, _ = discover_log_and_collect_example_images(
        soup,
        base_url=page_url,
        session=sess,
    )
    tong = [x for x in collected if x.source_type == "TONGEREN"]
    if not tong:
        print(
            "--smoke-tongeren FAIL: nessun set TONGEREN completo raccolto dagli examples OCRE.",
            file=sys.stderr,
        )
        return 2
    with tempfile.TemporaryDirectory() as td:
        images_root = Path(td) / "images_root"
        images_dir = images_root / IMAGES_SUBDIR
        n, complete = save_coin_images_local(
            record,
            images_dir,
            resolved_sets=tong,
            download_images=True,
            prior_images=None,
            output_base_for_paths=images_root,
        )
        found = list(images_dir.glob("**/unified.jpg"))
        if found and found[0].is_file() and found[0].stat().st_size > 0:
            nbytes = found[0].stat().st_size
            print(
                f"--smoke-tongeren OK: set={len(tong)} | unified.jpg scritto ({nbytes} bytes).",
                flush=True,
            )
            return 0
    print(
        f"--smoke-tongeren FAIL: unified.jpg assente/vuota (save_coin_images_local: dirs={n}, complete={complete}).",
        file=sys.stderr,
    )
    return 2


def main(argv: Optional[List[str]] = None) -> int:
    load_environment_from_dotenv()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description=(
            "Scraper OCRE (#examples multi-fonte): output JSON e cartella images/ "
            "(layout split | unified per set). Opzione --no-img: salta gli scarichi JPEG mantenendo metadati e images."
        ),
    )
    parser.add_argument(
        "input_file",
        nargs="?",
        default=None,
        metavar="FILE",
        help=(
            "monete_links.json (chiave coin_links); default se omesso: vedi anche -i/-o. "
            f"Percorso predefinito senza questo arg né -i/--input: {DEFAULT_JSON_INPUT}"
        ),
    )
    parser.add_argument(
        "-i",
        "--input",
        dest="cli_input",
        metavar="PATH",
        default=None,
        help="stesso significato dell'argomento posizionale FILE (preferisci uno solo dei due)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=OUTPUT_FILENAME,
        metavar="PATH",
        help=f"file JSON in uscita (default: {OUTPUT_FILENAME})",
    )
    parser.add_argument(
        "-n",
        "-noimg",
        "--no-img",
        dest="no_img",
        action="store_true",
        help="non scaricare immagini su disco; voce images senza path in files (salvo resume da run precedente)",
    )
    parser.add_argument(
        "--delete-local-after-upload",
        "--upload-delete-local",
        "-U",
        dest="delete_local_after_upload",
        action="store_true",
        help=(
            "dopo un upload riuscito su storage remoto (R2 con USE_R2_UPLOAD e credenziali valide), "
            "elimina il JPEG locale per liberare spazio. Senza upload remoto (solo storage locale o "
            "fallback locale) l'opzione non ha effetto. Non elimina mai se finalize/upload fallisce "
            "o il file era assente/vuoto."
        ),
    )
    parser.add_argument(
        "--bucket-folder",
        "--r2-folder",
        dest="bucket_folder",
        default=None,
        metavar="PATH",
        help=(
            "cartella/prefisso di destinazione nel bucket R2 per questa run "
            "(es. rrc_images produce rrc_images/... sostituendo la radice locale images/); se omessa usa "
            "R2_BUCKET_FOLDER dal .env, oppure la radice del bucket"
        ),
    )
    parser.add_argument(
        "-R",
        "-reset",
        "--reset",
        dest="reset",
        action="store_true",
        help=(
            "rimuovi il file JSON di output (come -o, default output.json) e la cartella "
            f"{IMAGES_SUBDIR}/ accanto ad esso; poi termina senza scraping"
        ),
    )
    parser.add_argument(
        "-B",
        "--smoke-bm",
        dest="smoke_bm",
        action="store_true",
        help=(
            "rete richiesta: controlla la pagina OCRE Augustus RIC tipo (examples BM) "
            "e uno scarico unified.jpg verso cartella temporanea"
        ),
    )
    parser.add_argument(
        "--smoke-tongeren",
        dest="smoke_tongeren",
        action="store_true",
        help=(
            "rete richiesta: controlla RIC I (2) Augustus 245 (examples con handle Tongeren) "
            "e scrittura di unified.jpg da set TONGEREN"
        ),
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        metavar="N",
        default=1,
        help=(
            "thread per monete distinte (default 1 = stesso ordine JSON di sempre, niente race su fetch_cache). "
            "Valori > 1 aumentano throughput ma anche uso RAM (sessioni/pagine concorrenti). "
            "``--low-memory`` forza 1. Stesso -H/--min-host-interval condiviso tra thread."
        ),
    )
    parser.add_argument(
        "-H",
        "--min-host-interval",
        dest="min_host_interval",
        type=float,
        default=REQUEST_DELAY_SEC,
        metavar="SEC",
        help=(
            f"pausa minima tra richieste allo stesso host in secondi (default {REQUEST_DELAY_SEC}: ~comportamento "
            "del vecchio sleep fisso per monolite). 0 disattiva il throttle (solo se il sito lo consente)."
        ),
    )
    parser.add_argument(
        "-t",
        "--timing",
        dest="timing",
        action="store_true",
        help="a fine run stampa secondi accumulati: fetch OCRE, fetch esempi, download immagini, sleep throttle",
    )
    parser.add_argument(
        "--forge-eng",
        "-feng",
        dest="forge_eng",
        action="store_true",
        help=(
            "For URLs loaded from the input JSON on numismatics.org only: remove every lang query "
            "parameter (any value) and add a single lang=en (OCRE type page in English). Does not alter "
            "example/collection links parsed from HTML. Export writes canonical ``source_ocre_url`` "
            "(lang=en on numismatics.org); resume compares via the same normalization for that host. "
            "Index fallback still applies when previous and current URL lists have the same length."
        ),
    )
    parser.add_argument("--max-request-retries", type=int, default=4, metavar="N", help="retry massimi per singola richiesta HTTP")
    parser.add_argument("--max-task-retries", type=int, default=2, metavar="N", help="retry massimi per singola moneta su errori retryable")
    parser.add_argument("--backoff-base-sec", type=float, default=1.0, metavar="SEC", help="base backoff esponenziale")
    parser.add_argument("--backoff-max-sec", type=float, default=45.0, metavar="SEC", help="cap massimo backoff")
    parser.add_argument("--backoff-jitter-sec", type=float, default=0.5, metavar="SEC", help="jitter random aggiunto al backoff")
    parser.add_argument("--outage-threshold", type=int, default=6, metavar="N", help="errori retryable consecutivi per dichiarare outage")
    parser.add_argument("--healthcheck-interval-sec", type=float, default=30.0, metavar="SEC", help="intervallo health-check durante pausa outage")
    parser.add_argument("--checkpoint-path", default=None, metavar="PATH", help="path checkpoint (default: <output>.checkpoint.json)")
    parser.add_argument("--checkpoint-frequency", type=int, default=10, metavar="N", help="salva checkpoint ogni N monete completate")
    parser.add_argument(
        "--checkpoint-sharded",
        action="store_true",
        help=(
            "checkpoint su manifest JSON + cartella \"<checkpoint>.parts\" (un file per indice); "
            "a ogni salvataggio riserializza solo i record nuovi/dirty, riducendo picchi RAM/CPU. "
            "Legge ancora i checkpoint monolitici legacy (results_by_index nel file)."
        ),
    )
    parser.add_argument(
        "--low-memory",
        action="store_true",
        help="preset: forza --workers 1 e abilita --checkpoint-sharded (meno RAM, throughput tipicamente minore).",
    )
    args = parser.parse_args(argv)

    if getattr(args, "low_memory", False):
        args.workers = 1
        args.checkpoint_sharded = True

    if args.cli_input is not None and args.input_file is not None:
        parser.error("specifica solo FILE posizionale oppure solo -i/--input, non entrambi")

    if getattr(args, "reset", False):
        out_path = Path(args.output).resolve()
        images_dir = out_path.parent / IMAGES_SUBDIR
        removed: list[str] = []
        if images_dir.is_dir():
            shutil.rmtree(images_dir)
            removed.append(str(images_dir))
        if out_path.is_file():
            out_path.unlink()
            removed.append(str(out_path))
        if removed:
            print("Reset: eliminati:", flush=True)
            for p in removed:
                print(f"  {p}", flush=True)
        else:
            print(
                f"Reset: niente da cancellare ({out_path} assente; {images_dir} assente o non cartella).",
                flush=True,
            )
        return 0

    if getattr(args, "smoke_bm", False):
        return run_bm_ocre_regression_smoke()
    if getattr(args, "smoke_tongeren", False):
        return run_tongeren_ocre_regression_smoke()

    raw_input = args.cli_input if args.cli_input is not None else args.input_file
    in_path = Path(raw_input) if raw_input else DEFAULT_JSON_INPUT
    if not in_path.is_file():
        print(f"File di input non trovato: {in_path}", file=sys.stderr)
        return 1

    urls = load_coin_urls_from_json(in_path)
    if not urls:
        print("Nessun URL da elaborare.", file=sys.stderr)
        return 1
    if getattr(args, "forge_eng", False):
        urls = [force_lang_en_on_input_url(u) for u in urls]

    out_path = Path(args.output).resolve()
    images_dir = out_path.parent / IMAGES_SUBDIR
    checkpoint_path = (
        Path(args.checkpoint_path).resolve()
        if args.checkpoint_path
        else out_path.with_suffix(out_path.suffix + ".checkpoint.json")
    )
    download_images = not args.no_img
    previous_results = load_previous_results_for_resume(out_path)
    if previous_results:
        print(
            f"Resume immagini: trovato output esistente ({len(previous_results)} record in {out_path.name}).",
            flush=True,
        )
    storage_backend = wrap_storage_delete_local_after_upload(
        resolve_image_storage_from_env(bucket_folder=args.bucket_folder),
        enabled=bool(getattr(args, "delete_local_after_upload", False)),
    )
    results = scrape_all(
        urls,
        images_dir,
        download_images=download_images,
        previous_results=previous_results,
        max_workers=args.workers,
        min_host_interval_sec=args.min_host_interval,
        collect_timing=args.timing,
        storage_backend=storage_backend,
        fault_cfg=FaultToleranceConfig(
            max_request_retries=max(0, args.max_request_retries),
            max_task_retries=max(0, args.max_task_retries),
            backoff_base_sec=max(0.0, args.backoff_base_sec),
            backoff_max_sec=max(0.0, args.backoff_max_sec),
            backoff_jitter_sec=max(0.0, args.backoff_jitter_sec),
            outage_failure_threshold=max(1, args.outage_threshold),
            outage_healthcheck_interval_sec=max(1.0, args.healthcheck_interval_sec),
        ),
        checkpoint_cfg=CheckpointConfig(
            path=checkpoint_path,
            frequency_items=max(1, args.checkpoint_frequency),
            sharded=bool(getattr(args, "checkpoint_sharded", False)),
        ),
    )
    write_json_array_indented_atomic(out_path, results)
    processed = len(results)
    failed = sum(1 for r in results if normalize_text(str(r.get("name", ""))).startswith("__error__"))
    skipped = max(0, len(urls) - processed)
    print(f"Summary: processed={processed} skipped={skipped} failed={failed}", flush=True)
    if download_images:
        print(f"Scritto {out_path} ({len(results)} record); immagini in {images_dir}", flush=True)
    else:
        print(
            f"Scritto {out_path} ({len(results)} record); immagini non scaricate (--no-img; cartella prevista {images_dir})",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
