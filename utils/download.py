"""
Download immagini via HTTP con verifica SSL (certifi) e fallback solo su SSLError.
"""

from __future__ import annotations

import logging
import os
import warnings
from contextlib import nullcontext
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Union

import certifi
import requests
from urllib3.exceptions import InsecureRequestWarning

logger = logging.getLogger(__name__)

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _base_headers(extra: Optional[Mapping[str, str]] = None) -> dict[str, str]:
    h: dict[str, str] = {"User-Agent": _BROWSER_UA}
    if extra:
        h.update(extra)
    return h


def _unlink_quiet(path: Path) -> None:
    try:
        if path.is_file():
            path.unlink()
    except OSError:
        pass


def download_image(
    url: str,
    filename: Union[str, Path],
    *,
    timeout: int = 10,
    chunk_size: int = 65536,
    headers: Optional[Mapping[str, str]] = None,
    header_overlays: Optional[Sequence[Mapping[str, str]]] = None,
    skip_if_exists: bool = True,
    retry_http_statuses: Optional[Sequence[int]] = None,
) -> bool:
    """
    Scarica ``url`` in ``filename`` (path completo del file) con streaming.

    - ``verify=certifi.where()``; solo in caso di ``SSLError`` un secondo tentativo
      con ``verify=False`` (con log).
    - Risposte HTTP non ok: ``raise_for_status``; l'eccezione diventa ``False`` e il
      file parziale viene rimosso (compatibilità con i chiamanti esistenti).
    - ``header_overlays``: sequenza di mapping da unire agli header base; si passa al
      tentativo successivo se lo status è in ``retry_http_statuses`` e restano overlay
      successivi.
    """
    if not (url or "").strip():
        return False
    dest = Path(filename)
    if skip_if_exists and dest.is_file() and dest.stat().st_size > 0:
        return True

    os.makedirs(dest.parent, exist_ok=True)
    retry_statuses = frozenset(retry_http_statuses or ())
    overlays: list[Mapping[str, str]]
    if header_overlays is None:
        overlays = [{}]
    else:
        overlays = list(header_overlays)

    merged_list = [{**_base_headers(headers), **ov} for ov in overlays]

    for oi, merged in enumerate(merged_list):
        for use_insecure in (False, True):
            verify: Union[bool, str] = False if use_insecure else certifi.where()
            warn_ctx = (
                warnings.catch_warnings()
                if use_insecure
                else nullcontext()
            )
            try:
                with warn_ctx:
                    if use_insecure:
                        warnings.simplefilter("ignore", InsecureRequestWarning)
                    r = requests.get(
                        url.strip(),
                        stream=True,
                        timeout=timeout,
                        headers=dict(merged),
                        verify=verify,
                    )
            except requests.exceptions.SSLError:
                if not use_insecure:
                    logger.warning(
                        "SSL verification failed, retrying without verification for %s",
                        url.strip(),
                    )
                    continue
                _unlink_quiet(dest)
                return False

            try:
                with r:
                    if (
                        r.status_code in retry_statuses
                        and oi + 1 < len(merged_list)
                    ):
                        break
                    r.raise_for_status()
                    with dest.open("wb") as f:
                        for chunk in r.iter_content(chunk_size):
                            if chunk:
                                f.write(chunk)
                return dest.is_file() and dest.stat().st_size > 0
            except requests.exceptions.RequestException:
                _unlink_quiet(dest)
                return False

    _unlink_quiet(dest)
    return False


def http_request_with_ssl_fallback(
    method: str,
    url: str,
    *,
    timeout: float,
    headers: Optional[Mapping[str, str]] = None,
    **kwargs: Any,
) -> requests.Response:
    """
    Una richiesta HTTP (es. ``HEAD`` / ``GET`` non stream) con ``certifi`` e,
    solo in caso di ``SSLError``, ``verify=False`` (con log, stessa politica di
    ``download_image``).
    """
    merged = _base_headers(headers)
    for use_insecure in (False, True):
        verify: Union[bool, str] = False if use_insecure else certifi.where()
        warn_ctx = (
            warnings.catch_warnings()
            if use_insecure
            else nullcontext()
        )
        try:
            with warn_ctx:
                if use_insecure:
                    warnings.simplefilter("ignore", InsecureRequestWarning)
                return requests.request(
                    method,
                    url.strip(),
                    timeout=timeout,
                    headers=dict(merged),
                    verify=verify,
                    **kwargs,
                )
        except requests.exceptions.SSLError:
            if not use_insecure:
                logger.warning(
                    "SSL verification failed, retrying without verification for %s",
                    url.strip(),
                )
                continue
            raise
    raise requests.exceptions.SSLError(
        f"SSL retry exhausted for {url.strip()}"
    )
