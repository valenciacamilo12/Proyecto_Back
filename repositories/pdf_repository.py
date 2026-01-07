from datetime import datetime, timezone
from typing import Any, Dict, Optional

import logging
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import OperationFailure


class PdfRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db.get_collection("pdf_uploads")
        self._log = logging.getLogger("api")

    async def ensure_indexes(self) -> None:
        """
        Índice único por id_carga.
        Debe ser idempotente (no romper startup en Cosmos DB).
        """
        try:
            await self._col.create_index(
                [("id_carga", ASCENDING)],
                unique=True,
                name="id_carga_unique",
            )
        except OperationFailure as e:
            if getattr(e, "code", None) == 85:  # IndexOptionsConflict
                self._log.info("[INDEX] id_carga index ya existe, se reutiliza")
            else:
                raise

    async def create_upload_record(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Inserta el registro inicial de la carga (lo que usa PdfService.register_upload).
        """
        now = datetime.now(timezone.utc)
        doc.setdefault("created_at", now)
        doc.setdefault("updated_at", now)

        result = await self._col.insert_one(doc)
        created = await self._col.find_one({"_id": result.inserted_id})
        return self._serialize(created)

    async def update_status_by_id_carga(
        self,
        *,
        id_carga: str,
        status: str,
        comment: str,
    ) -> Dict[str, Any]:
        """
        Actualiza el estado de una carga.
        - ERROR      -> guarda error_message = comment
        - PROCESSED  -> elimina error_message si existe
        El comment NO se persiste como campo independiente.
        """
        self._log.info(f"[STATUS] id_carga={id_carga} status={status} comment={comment}")

        update: Dict[str, Any] = {
            "$set": {
                "status": status,
                "updated_at": datetime.now(timezone.utc),
            }
        }

        if status == "ERROR":
            update["$set"]["error_message"] = comment

        elif status == "PROCESSED":
            update["$unset"] = {"error_message": ""}

        updated = await self._col.find_one_and_update(
            {"id_carga": id_carga},
            update,
            return_document=ReturnDocument.AFTER,
        )

        return self._serialize(updated)

    def _serialize(self, doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not doc:
            return {}
        if "_id" in doc and isinstance(doc["_id"], ObjectId):
            doc["_id"] = str(doc["_id"])
        return doc
