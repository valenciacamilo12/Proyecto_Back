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
        self._col = db.get_collection("pdf_uploads")
        self._log = logging.getLogger("api")

    async def ensure_indexes(self) -> None:
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
        now = datetime.now(timezone.utc)
        doc.setdefault("created_at", now)
        doc.setdefault("updated_at", now)

        # Inicializa lista de extracciones (por orden / por documento)
        doc.setdefault("extractions", [])

        result = await self._col.insert_one(doc)
        created = await self._col.find_one({"_id": result.inserted_id})
        return self._serialize(created)

    async def list_cargas(self) -> List[Dict[str, Any]]:
        pipeline = [
            {"$sort": {"updated_at": -1}},
            {
                "$group": {
                    "_id": "$id_carga",
                    "id_carga": {"$first": "$id_carga"},
                    "status": {"$first": "$status"},
                    "updated_at": {"$first": "$updated_at"},
                    "error_message": {"$first": "$error_message"},
                    "filename": {"$first": "$filename"},
                    "blob": {"$first": "$blob"},
                    "excel_blob_url": {"$first": "$excel_blob_url"},
                    "extractions": {"$first": "$extractions"},
                }
            },
            {"$sort": {"updated_at": -1}},
        ]

        cursor = self._col.aggregate(pipeline)
        docs = [self._serialize(doc) async for doc in cursor]
        return docs

    async def find_latest_by_id_carga(self, id_carga: str) -> Optional[Dict[str, Any]]:
        doc = await self._col.find_one({"id_carga": id_carga}, sort=[("updated_at", -1)])
        return self._serialize(doc) if doc else None

    async def update_status_by_id_carga(
        self,
        *,
        id_carga: str,
        status: str,
        comment: str,
        extracted: Optional[Dict[str, Any]] = None,
        trace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        - ERROR     -> set error_message y LIMPIA extractions[]
        - PROCESSED -> unset error_message y (si viene extracted) push a extractions[]
        - UPLOADED  -> estado normal

        Guarda lista ordenada en Mongo:
          extractions: [
            { trace_id, created_at, ...campos extraidos... },
            ...
          ]
        """
        self._log.info(
            f"[STATUS] id_carga={id_carga} status={status} comment={comment} trace_id={trace_id}"
        )

        now = datetime.now(timezone.utc)

        update: Dict[str, Any] = {
            "$set": {
                "status": status,
                "updated_at": now,
            }
        }

        if status == "ERROR":
            update["$set"]["error_message"] = comment

            # ✅ Ajuste: en ERROR limpiamos cualquier extracción previa
            update["$set"]["extractions"] = []

            # (Opcional) Si en algún momento se guardó trace_id en raíz y quieres limpiarlo:
            # update.setdefault("$unset", {})["trace_id"] = ""

        elif status == "PROCESSED":
            update["$unset"] = {"error_message": ""}

            # Si el microservicio IA manda extracción, la guardamos como entrada en lista
            if extracted:
                entry = dict(extracted)
                entry["trace_id"] = trace_id
                entry["created_at"] = now

                # Garantiza que exista el arreglo (solo aplica si usas upsert=True; aquí no lo usas)
                update.setdefault("$setOnInsert", {})["extractions"] = []

                # Guardar "una fila por documento" en la lista, en orden de llegada
                update.setdefault("$push", {})["extractions"] = entry

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