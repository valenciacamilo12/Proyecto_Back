from datetime import datetime
from typing import List, Dict, Any, Optional


class PdfRepository:
    def __init__(self, db):
        self._col = db["pdf_uploads"]

    async def create_upload_record(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.utcnow()
        doc.update({
            "created_at": now,
            "updated_at": now,
        })

        result = await self._col.insert_one(doc)
        doc["_id"] = result.inserted_id
        return doc

    async def list_cargas(self) -> List[Dict[str, Any]]:
        """
        Devuelve una fila por id_carga (último estado)
        """
        pipeline = [
            {"$sort": {"updated_at": -1}},
            {
                "$group": {
                    "_id": "$id_carga",
                    "id_carga": {"$first": "$id_carga"},
                    "status": {"$first": "$status"},
                    "updated_at": {"$first": "$updated_at"},
                }
            },
            {"$sort": {"updated_at": -1}},
        ]

        cursor = self._col.aggregate(pipeline)
        return [doc async for doc in cursor]

    async def find_latest_by_id_carga(
        self,
        id_carga: str
    ) -> Optional[Dict[str, Any]]:
        doc = await self._col.find_one(
            {"id_carga": id_carga},
            sort=[("updated_at", -1)],
        )
        if doc and "_id" in doc:
            doc["_id"] = str(doc["_id"])
        return doc
