from typing import Any, Dict
from repositories.pdf_repository import PdfRepository


class PdfStatusService:
    """
    Servicio para actualizar el estado de una carga por id_carga.
    Unifica éxito y error en un único contrato: (status, comment).
    """

    def __init__(self, repo: PdfRepository):
        self._repo = repo

    async def update_status(
        self,
        *,
        id_carga: str,
        status: str,
        comment: str,
    ) -> Dict[str, Any]:
        return await self._repo.update_status_by_id_carga(
            id_carga=id_carga,
            status=status,
            comment=comment,
        )