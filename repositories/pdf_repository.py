from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import OperationFailure


class PdfRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        # Mantiene compatibilidad con motor/cosmos
        self._col = db.get_collection("pdf_uploads")
        self._log = logging.getLogger("api")

    # -------------------------
    # Índices (v2)
    # -------------------------
    async def ensure_indexes(self) -> None:
        """
        Índice por id_carga.
        OJO: si tu modelo permite múltiples documentos por la misma id_carga (uno por PDF),
        NO debe ser unique=True. Si solo guardas UN documento por id_carga, sí puede ser unique.

        Para no romper tu comportamiento actual (que parece ser "uno por id_carga"),
        lo dejamos como unique=True como en la v2.
        """
        try:
            await self._col.create_index(
                [("id_carga", ASCENDING)],
                unique=True,
                name="id_carga_unique",
            )
        except OperationFailure as e:
            # Cosmos DB puede lanzar IndexOptionsConflict si ya existe con otras opciones
            if getattr(e, "code", None) == 85:
                self._log.info("[INDEX] id_carga index ya existe, se reutiliza")
            else:
                raise

    # -------------------------
    # Crear registro (merge v1/v2)
    # -------------------------
    async def create_upload_record(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Inserta el registro inicial de la carga (usado por PdfService.register_upload).
        - created_at / updated_at en UTC
        - retorna documento serializado
        """
        now = datetime.now(timezone.utc)

        doc.setdefault("created_at", now)
        doc.setdefault("updated_at", now)

        result = await self._col.insert_one(doc)
        created = await self._col.find_one({"_id": result.inserted_id})
        return self._serialize(created)

    # -------------------------
    # Dashboard: listar cargas (v1 mejorada)
    # -------------------------
    async def list_cargas(self) -> List[Dict[str, Any]]:
        """
        Devuelve una fila por id_carga (último estado).
        Incluye: id_carga, status, updated_at, error_message
        """
        pipeline = [
            {"$sort": {"updated_at": -1}},
            {
                "$group": {
                    "_id": "$id_carga",
                    "id_carga": {"$first": "$id_carga"},
                    "status": {"$first": "$status"},
                    "updated_at": {"$first": "$updated_at"},
                    "error_message": {"$first": "$error_message"},
                    # opcional: si te sirve para reintento/descarga
                    "filename": {"$first": "$filename"},
                    "blob": {"$first": "$blob"},
                    "excel_blob_url": {"$first": "$excel_blob_url"},
                }
            },
            {"$sort": {"updated_at": -1}},
        ]

        cursor = self._col.aggregate(pipeline)
        docs = [self._serialize(doc) async for doc in cursor]
        return docs

    # -------------------------
    # Buscar última por id_carga (v1 con serialize v2)
    # -------------------------
    async def find_latest_by_id_carga(self, id_carga: str) -> Optional[Dict[str, Any]]:
        doc = await self._col.find_one(
            {"id_carga": id_carga},
            sort=[("updated_at", -1)],
        )
        return self._serialize(doc) if doc else None

    # -------------------------
    # Actualizar estado (v2)
    # -------------------------
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

    # -------------------------
    # Serialización (v2)
    # -------------------------
    def _serialize(self, doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not doc:
            return {}

        if "_id" in doc and isinstance(doc["_id"], ObjectId):
            doc["_id"] = str(doc["_id"])

        # Si Blob viene con "url" en vez de "blob_url", puedes normalizar aquí si quieres:
        # blob = doc.get("blob")
        # if isinstance(blob, dict) and "blob_url" not in blob and "url" in blob:
        #     blob["blob_url"] = blob["url"]

        return doc
