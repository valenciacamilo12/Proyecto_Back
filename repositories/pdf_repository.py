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
            if getattr(e, "code", None) == 85:
                self._log.info("[INDEX] id_carga index ya existe, se reutiliza")
            else:
                raise

    async def create_upload_record(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        doc.setdefault("created_at", now)
        doc.setdefault("updated_at", now)
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
        return [self._serialize(doc) async for doc in cursor]

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
        now = datetime.now(timezone.utc)
        update: Dict[str, Any] = {"$set": {"status": status, "updated_at": now}}

        if status == "ERROR":
            update["$set"]["error_message"] = comment
            update["$set"]["extractions"] = []

        elif status == "PROCESSED":
            update["$unset"] = {"error_message": ""}
            if extracted:
                entry = dict(extracted)
                entry["trace_id"] = trace_id
                entry["created_at"] = now
                update.setdefault("$push", {})["extractions"] = entry

        updated = await self._col.find_one_and_update(
            {"id_carga": id_carga},
            update,
            return_document=ReturnDocument.AFTER,
        )
        return self._serialize(updated)

    # ---------------------------------------------------------
    # ✅ NUEVO: filas para Excel (1 fila por documento / id_carga)
    # ---------------------------------------------------------
    async def list_extractions_for_excel(self) -> List[Dict[str, Any]]:
        """
        Retorna filas normalizadas para Excel.
        - Solo documentos PROCESSED con al menos 1 extracción.
        - Toma la ÚLTIMA extracción (si hay varias).
        - Incluye id_carga (para la columna idCarga del Excel).
        """
        query = {"status": "PROCESSED", "extractions.0": {"$exists": True}}
        projection = {"id_carga": 1, "filename": 1, "updated_at": 1, "extractions": 1}

        cursor = self._col.find(query, projection=projection).sort("updated_at", -1)

        rows: List[Dict[str, Any]] = []
        async for doc in cursor:
            extractions = doc.get("extractions") or []
            if not extractions:
                continue

            last = extractions[-1]  # última extracción
            rows.append(
                {
                    "id_carga": doc.get("id_carga") or "",
                    "file": last.get("file") or doc.get("filename") or "",
                    "radicado": last.get("radicado") or "",
                    "demandado": last.get("demandado") or "",
                    "demandante": last.get("demandante") or "",
                    "fecha_de_recibido": last.get("fecha_de_recibido") or "",
                    "fecha_de_sentencia": last.get("fecha_de_sentencia") or "",
                    "tipo_de_proceso": last.get("tipo_de_proceso") or "",
                }
            )

        return rows

    def _serialize(self, doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not doc:
            return {}
        if "_id" in doc and isinstance(doc["_id"], ObjectId):
            doc["_id"] = str(doc["_id"])
        return doc