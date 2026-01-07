import logging
import os
import uuid
import io
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import (
    FastAPI,
    UploadFile,
    File,
    Form,
    HTTPException,
    Depends,
    Request,
    Path,
)
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field

from blob_storage import AzureBlobStorageClient
from queue_storage import AzureQueueStorageClient
from db.mongo import connect_to_mongo, close_mongo_connection, mongo
from repositories.pdf_repository import PdfRepository
from services.pdf_service import PdfService

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


# ✅ IMPORTANTE: ahora incluimos UPLOADED
AllowedStatus = Literal["UPLOADED", "PROCESSED", "ERROR"]


class UpdateStatusRequest(BaseModel):
    status: AllowedStatus = Field(..., description="Nuevo estado del registro")
    comment: str = Field(..., min_length=1, description="Comentario simple (éxito o error)")


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
# Endpoints base
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
        raise HTTPException(status_code=500, detail=f"Error subiendo a Blob, guardando en Mongo o enviando a cola: {e}")


# ----------------------------------------------------
# Endpoint para que el microservicio IA actualice estado
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
    )

    if not updated:
        raise HTTPException(status_code=404, detail=f"No existe id_carga={id_carga}")

    return JSONResponse(status_code=200, content=jsonable_encoder(updated))


# ----------------------------------------------------
# ✅ Endpoint que te faltaba: listado para Dashboard
# ----------------------------------------------------
@app.get("/dashboard/cargas")
async def list_dashboard_cargas(
    repo: PdfRepository = Depends(get_pdf_repo),
):
    """
    Devuelve una fila por id_carga (último estado), para pintar el Dashboard.
    """
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

    # Marcar como UPLOADED de nuevo (reintento)
    updated = await repo.update_status_by_id_carga(
        id_carga=id_carga,
        status="UPLOADED",
        comment="Reintento solicitado desde Dashboard",
    )

    return JSONResponse(
        status_code=200,
        content=jsonable_encoder(
            {
                "ok": True,
                "id_carga": id_carga,
                "queue": queue_payload,
                "mongo": updated,
            }
        ),
    )


# ----------------------------------------------------
# Descargar Excel (placeholder)
# ----------------------------------------------------
@app.get("/dashboard/cargas/{id_carga}/excel")
async def download_excel(
    id_carga: str = Path(...),
    repo: PdfRepository = Depends(get_pdf_repo),
):
    doc = await repo.find_latest_by_id_carga(id_carga)
    if not doc:
        raise HTTPException(status_code=404, detail=f"No existe id_carga={id_carga}")

    if (doc.get("status") or "").upper() != "PROCESSED":
        raise HTTPException(status_code=409, detail="La carga aún no está PROCESSED")

    excel_blob_url = doc.get("excel_blob_url")
    if not excel_blob_url:
        raise HTTPException(status_code=404, detail="No hay excel_blob_url asociado a esta carga")

    # Aquí debes implementar descarga real desde Blob.
    # Dejo explícito para que no “parezca que descargó” sin hacerlo.
    raise HTTPException(status_code=501, detail="Falta implementar descarga desde Blob (excel_blob_url)")
