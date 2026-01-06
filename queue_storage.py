import os
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from azure.storage.queue.aio import QueueClient
from azure.core.exceptions import ResourceExistsError


class AzureQueueStorageClient:
    def __init__(
        self,
        connection_string: Optional[str] = None,
        queue_name: Optional[str] = None,
    ) -> None:
        self.connection_string = "DefaultEndpointsProtocol=https;AccountName=cuentatesis;AccountKey=iyJbTjmfhvmCdzYy2Tnb7rXp88NWk1sephqm91UV1qPlk1dW7IOMEthHTxPyi5fQA8gQgZc6EVpy+AStbxmwwg==;EndpointSuffix=core.windows.net"
        self.queue_name = queue_name or os.getenv("AZURE_STORAGE_QUEUE_NAME", "documento-nuevo")

        if not self.connection_string:
            raise ValueError("Falta AZURE_STORAGE_CONNECTION_STRING")
        if not self.queue_name:
            raise ValueError("Falta AZURE_STORAGE_QUEUE_NAME")

        self._queue = QueueClient.from_connection_string(
            conn_str=self.connection_string,
            queue_name=self.queue_name,
        )

    async def ensure_queue(self) -> None:
        try:
            await self._queue.create_queue()
        except ResourceExistsError:
            pass

    async def publish_document_event(
        self,
        *,
        id_carga: str,
        blob_url: str,
        content_type: str = "application/pdf",
        trace_id: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        payload = {
            "idCarga": id_carga,
            "blobUrl": blob_url,
            "contentType": content_type,
            "traceId": trace_id,
            "created": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }

        if extra:
            payload.update(extra)

        # Azure Queue espera texto; el SDK se encarga de encoding
        await self._queue.send_message(json.dumps(payload, ensure_ascii=False))

        return payload

    async def close(self) -> None:
        await self._queue.close()