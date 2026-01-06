from datetime import datetime, timezone
from typing import Any, Dict, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import ASCENDING
from bson import ObjectId


class PdfRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db.get_collection("pdf_uploads")

    async def ensure_indexes(self) -> None:
        await self._col.create_index([("id_carga", ASCENDING)], unique=True)

    async def create_upload_record(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        doc.setdefault("created_at", now)
        doc.setdefault("updated_at", now)

        result = await self._col.insert_one(doc)
        created = await self._col.find_one({"_id": result.inserted_id})
        return self._serialize(created)

    def _serialize(self, doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not doc:
            return {}
        if "_id" in doc and isinstance(doc["_id"], ObjectId):
            doc["_id"] = str(doc["_id"])
        return doc
