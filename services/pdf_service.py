from typing import Any, Dict
from repositories.pdf_repository import PdfRepository


class PdfService:
    def __init__(self, repo: PdfRepository):
        self._repo = repo

    async def register_upload(self, id_carga: str, filename: str, blob_result: Dict[str, Any]) -> Dict[str, Any]:
        doc = {
            "id_carga": id_carga,
            "filename": filename,
            "blob": blob_result,
            "status": "UPLOADED",
        }
        return await self._repo.create_upload_record(doc)
