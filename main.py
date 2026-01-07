import logging
import os
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional, Literal
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends, Request, Path
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field
from pydantic import BaseModel, Field, model_validator
from blob_storage import AzureBlobStorageClient
from queue_storage import AzureQueueStorageClient
from db.mongo import connect_to_mongo, close_mongo_connection, mongo
from repositories.pdf_repository import PdfRepository
from services.pdf_service import PdfService
import io
from datetime import datetime
from fastapi.responses import StreamingResponse
from openpyxl import Workbook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api")

blob_client: AzureBlobStorageClient | None = None
queue_client: AzureQueueStorageClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global blob_client, queue_client

    logger.info("[LIFESPAN] startup: init blob + queue + connect mongo")
    blob_client = AzureBlobStorageClient()
    queue_client = AzureQueueStorageClient()

    await queue_client.ensure_queue()

    await connect_to_mongo()
    logger.info("[LIFESPAN] mongo conectado OK")

    try:
        repo = PdfRepository(mongo.db)
        await repo.ensure_indexes()
        logger.info("[LIFESPAN] ensure_indexes OK")
    except AttributeError:
        logger.info("[LIFESPAN] ensure_indexes NO existe (se omite)")

    yield

    logger.info("[LIFESPAN] shutdown: close mongo + queue")
    await close_mongo_connection()
    if queue_client:
        await queue_client.close()


app = FastAPI(lifespan=lifespan)


# -------------------------
# Dependencias
# -------------------------
def get_db():
    if mongo.db is None:
        raise RuntimeError("Mongo DB no inicializada")
    return mongo.db


def get_pdf_repo(db=Depends(get_db)) -> PdfRepository:
    return PdfRepository(db)


def get_pdf_service(repo: PdfRepository = Depends(get_pdf_repo)) -> PdfService:
    return PdfService(repo)


# -------------------------
# Modelos
# -------------------------
class GenerateIdCargaResponse(BaseModel):
    id_carga: str


AllowedStatus = Literal["UPLOADED", "PROCESSED", "ERROR"]


class ExtractionPayload(BaseModel):
    # Campos que dijiste que extrae el agente
    file: Optional[str] = Field(None, description="Nombre de archivo (si aplica)")
    radicado: str = Field(..., description="Radicado")
    demandado: str = Field(..., description="Demandado")
    demandante: str = Field(..., description="Demandante")
    fecha_de_recibido: Optional[str] = Field(None, description="Fecha de recibido (string tal como venga del agente)")
    fecha_de_sentencia: Optional[str] = Field(None, description="Fecha de sentencia (string tal como venga del agente)")
    tipo_de_proceso: str = Field(..., description="Tipo de proceso")


class UpdateStatusRequest(BaseModel):
    status: AllowedStatus = Field(..., description="Nuevo estado del registro")
    comment: str = Field(..., min_length=1, description="Comentario simple (éxito o error)")

    # NUEVO: datos del agente cuando PROCESSED
    extracted: Optional[ExtractionPayload] = Field(
        None,
        description="Datos extraídos por el agente (obligatorio cuando status=PROCESSED)",
    )

    # Opcional pero muy útil para trazabilidad/idempotencia
    trace_id: Optional[str] = Field(None, description="traceId del procesamiento en IA")

    @model_validator(mode="after")
    def validate_extracted_when_processed(self):
        if self.status == "PROCESSED" and self.extracted is None:
            raise ValueError("Cuando status=PROCESSED, debes enviar extracted.")
        return self


# -------------------------
# Diagnóstico
# -------------------------
@app.get("/health")
async def health():
    return {
        "azure_conn_string_present": bool(os.getenv("AZURE_STORAGE_CONNECTION_STRING")),
        "azure_queue_present": bool(os.getenv("AZURE_STORAGE_QUEUE_NAME")),
        "mongo_uri_present": bool(os.getenv("MONGO_URI")),
        "mongo_db": os.getenv("MONGO_DB"),
        "mongo_connected": mongo.db is not None,
        "blob_client_initialized": blob_client is not None,
        "queue_client_initialized": queue_client is not None,
    }


@app.post("/debug/echo")
async def debug_echo(request: Request):
    body = await request.body()
    return {
        "method": request.method,
        "path": str(request.url.path),
        "headers_has_content_type": "content-type" in request.headers,
        "content_type": request.headers.get("content-type"),
        "body_len": len(body),
    }


# -------------------------
# Endpoints
# -------------------------
@app.get("/")
async def read_root():
    return {"Hello": "World Back!"}


@app.get("/dashboard/id-carga", response_model=GenerateIdCargaResponse)
async def generate_id_carga():
    id_carga = f"UPL-{uuid.uuid4().hex[:8]}"
    logger.info(f"[ID] generado id_carga={id_carga}")
    return GenerateIdCargaResponse(id_carga=id_carga)


# -------------------------
# Upload PDF (Blob + Mongo + Queue)
# -------------------------
@app.post("/storage/pdf")
async def upload_pdf_to_blob(
    file: UploadFile = File(...),
    id_carga: str = Form(...),
    pdf_service: PdfService = Depends(get_pdf_service),
):
    logger.info(f"[UPLOAD] ENTRO endpoint /storage/pdf id_carga={id_carga} filename={file.filename}")

    if blob_client is None:
        raise HTTPException(status_code=500, detail="Blob client no inicializado")
    if queue_client is None:
        raise HTTPException(status_code=500, detail="Queue client no inicializado")

    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Debe enviar un archivo .pdf")

    pdf_bytes = await file.read()
    if not pdf_bytes:
        raise HTTPException(status_code=400, detail="Archivo vacío")

    trace_id = str(uuid.uuid4())

    try:
        logger.info("[UPLOAD] 1/3 Subiendo a Blob...")
        blob_result = await blob_client.upload_pdf(pdf_bytes, file.filename)
        logger.info(f"[UPLOAD] blob_result={blob_result}")

        logger.info("[UPLOAD] 2/3 Guardando en Mongo...")
        db_record = await pdf_service.register_upload(
            id_carga=id_carga,
            filename=file.filename,
            blob_result=blob_result,
        )

        logger.info("[UPLOAD] 3/3 Publicando mensaje en cola...")
        queue_payload = await queue_client.publish_document_event(
            id_carga=id_carga,
            blob_url=blob_result["blob_url"],
            content_type="application/pdf",
            trace_id=trace_id,
            extra={
                "container": blob_result.get("container"),
                "blobName": blob_result.get("blob_name"),
                "filename": file.filename,
            },
        )

        return JSONResponse(
            status_code=201,
            content=jsonable_encoder({"blob": blob_result, "mongo": db_record, "queue": queue_payload}),
        )

    except Exception as e:
        logger.exception("[UPLOAD] FALLO")
        raise HTTPException(
            status_code=500,
            detail=f"Error subiendo a Blob, guardando en Mongo o enviando a cola: {e}",
        )


# ----------------------------------------------------
# Endpoint para que el microservicio IA actualice estado + guarde extracción
# ----------------------------------------------------
@app.patch("/cargas/{id_carga}/status")
async def update_status(
    payload: UpdateStatusRequest,
    id_carga: str = Path(..., description="Identificador de la carga (UPL-xxxx)"),
    repo: PdfRepository = Depends(get_pdf_repo),
):
    updated = await repo.update_status_by_id_carga(
        id_carga=id_carga,
        status=payload.status,
        comment=payload.comment,
        extracted=payload.extracted.model_dump() if payload.extracted else None,
        trace_id=payload.trace_id,
    )

    if not updated:
        raise HTTPException(status_code=404, detail=f"No existe id_carga={id_carga}")

    return JSONResponse(status_code=200, content=jsonable_encoder(updated))


# ----------------------------------------------------
# Dashboard: listado
# ----------------------------------------------------
@app.get("/dashboard/cargas")
async def list_dashboard_cargas(repo: PdfRepository = Depends(get_pdf_repo)):
    cargas = await repo.list_cargas()

    response = []
    for c in cargas:
        status = (c.get("status") or "UNKNOWN").upper()
        response.append(
            {
                "id_carga": c.get("id_carga"),
                "updated_at": c.get("updated_at"),
                "status": status,
                "error_message": c.get("error_message"),
                "extractions_count": len(c.get("extractions") or []),
            }
        )

    return response


# ----------------------------------------------------
# Reintentar carga: re-publica en la cola
# ----------------------------------------------------
@app.post("/dashboard/cargas/{id_carga}/retry")
async def retry_carga(
    id_carga: str = Path(..., description="Identificador de la carga (UPL-xxxx)"),
    repo: PdfRepository = Depends(get_pdf_repo),
):
    if queue_client is None:
        raise HTTPException(status_code=500, detail="Queue client no inicializado")

    doc = await repo.find_latest_by_id_carga(id_carga)
    if not doc:
        raise HTTPException(status_code=404, detail=f"No existe id_carga={id_carga}")

    blob = doc.get("blob") or {}
    blob_url = blob.get("blob_url") or blob.get("url") or doc.get("blob_url")
    if not blob_url:
        raise HTTPException(status_code=400, detail="La carga no tiene blob_url para reintentar")

    trace_id = str(uuid.uuid4())
    queue_payload = await queue_client.publish_document_event(
        id_carga=id_carga,
        blob_url=blob_url,
        content_type="application/pdf",
        trace_id=trace_id,
        extra={
            "container": blob.get("container"),
            "blobName": blob.get("blob_name") or blob.get("blobName"),
            "filename": doc.get("filename"),
        },
    )

    updated = await repo.update_status_by_id_carga(
        id_carga=id_carga,
        status="UPLOADED",
        comment="Reintento solicitado desde Dashboard",
        extracted=None,
        trace_id=trace_id,
    )

    return JSONResponse(
        status_code=200,
        content=jsonable_encoder({"ok": True, "id_carga": id_carga, "queue": queue_payload, "mongo": updated}),
    )


@app.get("/dashboard/extractions/excel")
async def download_extractions_excel(repo: PdfRepository = Depends(get_pdf_repo)):
    rows = await repo.list_extractions_for_excel()

    wb = Workbook()
    ws = wb.active
    ws.title = "Extracciones"

    headers = [
        "idCarga",
        "file",
        "Radicado",
        "Demandado",
        "Demandante",
        "Fecha De Recibido",
        "Fecha De Sentencia",
        "Tipo De Proceso",
    ]
    ws.append(headers)

    for r in rows:
        ws.append(
            [
                r.get("id_carga", ""),
                r.get("file", ""),
                r.get("radicado", ""),
                r.get("demandado", ""),
                r.get("demandante", ""),
                r.get("fecha_de_recibido", ""),
                r.get("fecha_de_sentencia", ""),
                r.get("tipo_de_proceso", ""),
            ]
        )

    for col in ws.columns:
        max_len = 0
        col_letter = col[0].column_letter
        for cell in col:
            v = "" if cell.value is None else str(cell.value)
            max_len = max(max_len, len(v))
        ws.column_dimensions[col_letter].width = min(max_len + 2, 60)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    filename = f"extracciones_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
        },
    )