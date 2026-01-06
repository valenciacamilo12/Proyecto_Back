from contextlib import asynccontextmanager
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

from blob_storage import AzureBlobStorageClient
from db.mongo import connect_to_mongo, close_mongo_connection, mongo
from repositories.pdf_repository import PdfRepository
from services.pdf_service import PdfService


blob_client = AzureBlobStorageClient()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect_to_mongo()
    yield
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
# Endpoints
# -------------------------
@app.get("/")
async def read_root():
    return {"Hello": "World IA!!!"}

@app.post("/storage/pdf")
async def upload_pdf_to_blob(
    file: UploadFile = File(...),
    pdf_service: PdfService = Depends(get_pdf_service),
):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Debe enviar un archivo .pdf")

    pdf_bytes = await file.read()
    if not pdf_bytes:
        raise HTTPException(status_code=400, detail="Archivo vacío")

    try:
        # 1) Subir a Azure Blob
        blob_result = await blob_client.upload_pdf(pdf_bytes, file.filename)

        # 2) Registrar en Mongo (async)
        db_record = await pdf_service.register_upload(file.filename, blob_result)

        # 3) Respuesta unificada
        return JSONResponse(
            status_code=201,
            content=jsonable_encoder({
                "blob": blob_result,
                "mongo": db_record,
            })
        )
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error subiendo a Blob o guardando en Mongo: {e}")
