import logging
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Depends, Request
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

from blob_storage import AzureBlobStorageClient
from db.mongo import connect_to_mongo, close_mongo_connection, mongo
from repositories.pdf_repository import PdfRepository
from services.pdf_service import PdfService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api")

blob_client: AzureBlobStorageClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global blob_client

    logger.info("[LIFESPAN] startup: init blob + connect mongo")
    blob_client = AzureBlobStorageClient()

    await connect_to_mongo()
    logger.info("[LIFESPAN] mongo conectado OK")

    # Índices (si tu repo los tiene)
    try:
        repo = PdfRepository(mongo.db)
        await repo.ensure_indexes()
        logger.info("[LIFESPAN] ensure_indexes OK")
    except AttributeError:
        logger.info("[LIFESPAN] ensure_indexes NO existe (se omite)")

    yield

    logger.info("[LIFESPAN] shutdown: close mongo")
    await close_mongo_connection()


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
# Diagnóstico
# -------------------------
@app.get("/health")
async def health():
    return {
        "azure_conn_string_present": bool(os.getenv("AZURE_STORAGE_CONNECTION_STRING")),
        "mongo_uri_present": bool(os.getenv("MONGO_URI")),
        "mongo_db": os.getenv("MONGO_DB"),
        "mongo_connected": mongo.db is not None,
        "blob_client_initialized": blob_client is not None,
    }


@app.post("/debug/echo")
async def debug_echo(request: Request):
    """
    Para confirmar si el front realmente está pegándole a ESTE backend.
    """
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


@app.post("/storage/pdf")
async def upload_pdf_to_blob(
    file: UploadFile = File(...),
    id_carga: str = Form(...),
    pdf_service: PdfService = Depends(get_pdf_service),
):
    # Evidencia dura: si esto no aparece en consola, el endpoint no se está llamando
    logger.info(f"[UPLOAD] ENTRO endpoint /storage/pdf id_carga={id_carga} filename={file.filename}")

    if blob_client is None:
        logger.error("[UPLOAD] blob_client is None")
        raise HTTPException(status_code=500, detail="Blob client no inicializado")

    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Debe enviar un archivo .pdf")

    pdf_bytes = await file.read()
    logger.info(f"[UPLOAD] bytes_leidos={len(pdf_bytes)}")

    if not pdf_bytes:
        raise HTTPException(status_code=400, detail="Archivo vacío")

    try:
        logger.info("[UPLOAD] 1/2 Subiendo a Blob...")
        blob_result = await blob_client.upload_pdf(pdf_bytes, file.filename)
        logger.info(f"[UPLOAD] blob_result={blob_result}")

        logger.info("[UPLOAD] 2/2 Guardando en Mongo...")
        db_record = await pdf_service.register_upload(
            id_carga=id_carga,
            filename=file.filename,
            blob_result=blob_result,
        )
        logger.info(f"[UPLOAD] mongo_record={db_record.get('_id')}")

        return JSONResponse(
            status_code=201,
            content=jsonable_encoder({"blob": blob_result, "mongo": db_record}),
        )

    except Exception as e:
        logger.exception("[UPLOAD] FALLO")
        raise HTTPException(status_code=500, detail=f"Error subiendo a Blob o guardando en Mongo: {e}")


# -----------------------------
# MODELO RESPONSE
# -----------------------------
class GenerateIdCargaResponse(BaseModel):
    id_carga: str


@app.get("/dashboard/id-carga", response_model=GenerateIdCargaResponse)
async def generate_id_carga():
    id_carga = f"UPL-{uuid.uuid4().hex[:8]}"
    logger.info(f"[ID] generado id_carga={id_carga}")
    return GenerateIdCargaResponse(id_carga=id_carga)
