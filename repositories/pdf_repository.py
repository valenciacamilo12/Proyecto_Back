from datetime import datetime, timezone
from typing import Any, Dict, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from bson import ObjectId

class PdfRepository:
    def __init__(self, db: AsyncIOMotorDatabase):
        self._col = db.get_collection("pdf_uploads")

    async def create_upload_record(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        doc.setdefault("created_at", now)
        doc.setdefault("updated_at", now)

        result = await self._col.insert_one(doc)
        created = await self._col.find_one({"_id": result.inserted_id})
        return self._serialize(created)

    async def update_status(self, doc_id: str, status: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        update: Dict[str, Any] = {
            "$set": {
                "status": status,
                "updated_at": datetime.now(timezone.utc),
            }
        }
        if extra:
            update["$set"].update(extra)

        _id = ObjectId(doc_id)
        await self._col.update_one({"_id": _id}, update)
        updated = await self._col.find_one({"_id": _id})
        return self._serialize(updated)

    def _serialize(self, doc: Dict[str, Any] | None) -> Dict[str, Any]:
        if not doc:
            return {}
        doc["_id"] = str(doc["_id"])
        return doc
