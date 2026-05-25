from __future__ import annotations

import logging
import mimetypes
import os
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, Optional, Protocol

logger = logging.getLogger(__name__)


class ImageStorageBackend(Protocol):
    def finalize_file(self, *, local_path: Path, relative_path: str) -> Optional[str]:
        """Persist a downloaded local file and return stored reference."""


class LocalImageStorageBackend:
    uploads_to_remote_object_storage: ClassVar[bool] = False

    def finalize_file(self, *, local_path: Path, relative_path: str) -> Optional[str]:
        if not local_path.is_file() or local_path.stat().st_size <= 0:
            return None
        return relative_path


@dataclass(frozen=True)
class R2Config:
    account_id: str
    access_key_id: str
    secret_access_key: str
    bucket_name: str
    public_base_url: str
    bucket_folder: str = ""

    @property
    def endpoint_url(self) -> str:
        return f"https://{self.account_id}.r2.cloudflarestorage.com"


def _env_flag_true(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "")
    if not raw:
        return default
    return raw.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _normalize_bucket_folder(folder: str) -> str:
    return "/".join(part for part in folder.replace("\\", "/").split("/") if part)


def _load_r2_config_from_env(*, bucket_folder: Optional[str] = None) -> Optional[R2Config]:
    values = {
        "R2_ACCOUNT_ID": (os.getenv("R2_ACCOUNT_ID") or "").strip(),
        "R2_ACCESS_KEY_ID": (os.getenv("R2_ACCESS_KEY_ID") or "").strip(),
        "R2_SECRET_ACCESS_KEY": (os.getenv("R2_SECRET_ACCESS_KEY") or "").strip(),
        "R2_BUCKET_NAME": (os.getenv("R2_BUCKET_NAME") or "").strip(),
    }
    missing = [k for k, v in values.items() if not v]
    if missing:
        logger.error(
            "USE_R2_UPLOAD enabled but missing required config: %s",
            ", ".join(missing),
        )
        return None
    public_base = (os.getenv("R2_PUBLIC_BASE_URL") or "").strip().rstrip("/")
    selected_folder = (
        os.getenv("R2_BUCKET_FOLDER", "")
        if bucket_folder is None
        else bucket_folder
    )
    return R2Config(
        account_id=values["R2_ACCOUNT_ID"],
        access_key_id=values["R2_ACCESS_KEY_ID"],
        secret_access_key=values["R2_SECRET_ACCESS_KEY"],
        bucket_name=values["R2_BUCKET_NAME"],
        public_base_url=public_base,
        bucket_folder=_normalize_bucket_folder(selected_folder.strip()),
    )


class R2ImageStorageBackend:
    uploads_to_remote_object_storage: ClassVar[bool] = True

    def __init__(self, config: R2Config) -> None:
        self._config = config
        self._endpoint_url = config.endpoint_url
        self._client_error_types = ()
        try:
            import boto3
            from botocore.config import Config as BotoCoreConfig
            from botocore.exceptions import BotoCoreError, ClientError
        except Exception as exc:
            raise RuntimeError(f"boto3/botocore non disponibili: {exc}") from exc
        self._client_error_types = (BotoCoreError, ClientError)
        self._client = boto3.client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=config.access_key_id,
            aws_secret_access_key=config.secret_access_key,
            region_name="auto",
            verify=True,
            config=BotoCoreConfig(retries={"max_attempts": 3, "mode": "standard"}),
        )

    def _build_public_url(self, key: str) -> str:
        if self._config.public_base_url:
            return f"{self._config.public_base_url}/{key}"
        return f"{self._endpoint_url}/{self._config.bucket_name}/{key}"

    def finalize_file(self, *, local_path: Path, relative_path: str) -> Optional[str]:
        if not local_path.is_file() or local_path.stat().st_size <= 0:
            logger.error("R2 upload skipped: local file missing/empty (%s)", local_path)
            return None
        key = relative_path.replace("\\", "/").lstrip("/")
        if self._config.bucket_folder:
            local_images_prefix = "images/"
            if key.startswith(local_images_prefix):
                key = key[len(local_images_prefix):]
            elif key == "images":
                key = ""
            key = f"{self._config.bucket_folder}/{key}".rstrip("/")
        content_type = mimetypes.guess_type(local_path.name)[0] or "application/octet-stream"
        try:
            self._client.upload_file(
                str(local_path),
                self._config.bucket_name,
                key,
                ExtraArgs={"ContentType": content_type},
            )
        except self._client_error_types as exc:
            logger.exception("R2 upload failed for key=%s: %s", key, exc)
            return None
        except Exception as exc:
            logger.exception("Unexpected R2 upload failure for key=%s: %s", key, exc)
            return None
        return self._build_public_url(key)


def _backend_uploads_to_remote(backend: ImageStorageBackend) -> bool:
    return bool(getattr(type(backend), "uploads_to_remote_object_storage", False))


class _DeleteLocalAfterRemoteUploadWrapper:
    """
    Dopo ``finalize_file`` del backend interno, se il riferimento restituito non è None
    tenta di eliminare il file locale. Usato solo quando il backend effettua upload remoto
    (flag ``uploads_to_remote_object_storage`` sulla classe).

    Parallelismo: ogni worker elabora monete distinte con path sotto cartelle diverse; i
    download/finalize per una stessa moneta sono sequenziali nello stesso thread, quindi non
    si condividono path JPEG tra thread.
    """

    __slots__ = ("_inner",)

    def __init__(self, inner: ImageStorageBackend) -> None:
        self._inner = inner

    def finalize_file(self, *, local_path: Path, relative_path: str) -> Optional[str]:
        ref = self._inner.finalize_file(local_path=local_path, relative_path=relative_path)
        if ref is None:
            return None
        existed_before = local_path.is_file()
        try:
            local_path.unlink(missing_ok=True)
        except OSError as exc:
            logger.warning(
                "Impossibile rimuovere file locale dopo upload remoto riuscito (%s): %s",
                local_path,
                exc,
            )
            return ref
        if existed_before:
            logger.info("Rimosso file locale dopo upload remoto riuscito: %s", local_path)
        else:
            logger.debug(
                "delete-local-after-upload: file locale già assente post-upload: %s",
                local_path,
            )
        return ref


def wrap_storage_delete_local_after_upload(
    backend: ImageStorageBackend,
    *,
    enabled: bool,
) -> ImageStorageBackend:
    """
    Se ``enabled`` e il backend carica su storage remoto (es. R2), avvolge il backend in
    modo da eliminare il JPEG locale solo dopo ``finalize_file`` con esito positivo
    (riferimento non None). Con backend locale o ``USE_R2_UPLOAD`` disattivo/fallback,
    ``enabled`` non ha effetto (no-op): il file di progetto resta l'unica copia utile.
    """
    if not enabled:
        return backend
    if not _backend_uploads_to_remote(backend):
        logger.debug(
            "Opzione delete-local-after-upload ignorata: backend senza upload remoto (%s).",
            type(backend).__name__,
        )
        return backend
    return _DeleteLocalAfterRemoteUploadWrapper(backend)


def resolve_image_storage_from_env(*, bucket_folder: Optional[str] = None) -> ImageStorageBackend:
    use_r2 = _env_flag_true("USE_R2_UPLOAD", default=False)
    if not use_r2:
        return LocalImageStorageBackend()
    cfg = _load_r2_config_from_env(bucket_folder=bucket_folder)
    if cfg is None:
        logger.warning("Falling back to local image storage due to invalid R2 config.")
        return LocalImageStorageBackend()
    try:
        return R2ImageStorageBackend(cfg)
    except Exception as exc:
        logger.exception("Failed to initialize R2 storage; falling back to local: %s", exc)
        return LocalImageStorageBackend()
