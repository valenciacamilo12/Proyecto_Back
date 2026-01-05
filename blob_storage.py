import os
import re
from datetime import datetime, timezone
from typing import Optional

from azure.storage.blob.aio import BlobServiceClient
from azure.core.exceptions import ResourceExistsError


def _safe_filename(name: str) -> str:
    name = (name or "").strip().replace(" ", "_")
    name = re.sub(r"[^a-zA-Z0-9._-]", "", name)
    return name or "file.pdf"


class AzureBlobStorageClient:
    """
    Sube archivos a Azure Blob Storage usando Connection String.
    Contenedor privado recomendado.
    """

    def __init__(
        self,
        connection_string: Optional[str] = None,
        container_name: Optional[str] = None,
        folder_prefix: str = "pdf",
    ) -> None:
        self.connection_string = connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.container_name = container_name or os.getenv("AZURE_STORAGE_CONTAINER")
        self.folder_prefix = folder_prefix.strip("/")

        if not self.connection_string:
            raise ValueError("Falta AZURE_STORAGE_CONNECTION_STRING en variables de entorno.")
        if not self.container_name:
            raise ValueError("Falta AZURE_STORAGE_CONTAINER en variables de entorno.")

        self._service = BlobServiceClient.from_connection_string(self.connection_string)

    def _build_blob_name(self, original_filename: str) -> str:
        safe = _safe_filename(original_filename)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        # Ej: pdf/20260105T223000Z_Proceso22.pdf
        return f"{self.folder_prefix}/{ts}_{safe}"

    async def upload_pdf(self, pdf_bytes: bytes, original_filename: str) -> dict:
        if not pdf_bytes:
            raise ValueError("Archivo vacío")

        blob_name = self._build_blob_name(original_filename)

        container = self._service.get_container_client(self.container_name)

        # Si el contenedor ya existe, no pasa nada
        try:
            await container.create_container()
        except ResourceExistsError:
            pass

        blob = container.get_blob_client(blob_name)

        await blob.upload_blob(
            data=pdf_bytes,
            overwrite=False,
            content_type="application/pdf",
        )

        return {
            "container": self.container_name,
            "blob_name": blob_name,
            "blob_url": blob.url,  # OJO: si el container es privado, no abre sin SAS
        }
