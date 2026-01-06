from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from blob_storage import AzureBlobStorageClient
import uuid
from pydantic import BaseModel

app = FastAPI()

blob_client = AzureBlobStorageClient()

@app.get("/")
async def read_root():
    return {"Hello": "World IA!!!"}


@app.post("/storage/pdf")
async def upload_pdf_to_blob(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Debe enviar un archivo .pdf")

    pdf_bytes = await file.read()
    if not pdf_bytes:
        raise HTTPException(status_code=400, detail="Archivo vacío")

    try:
        result = await blob_client.upload_pdf(pdf_bytes, file.filename)
        return JSONResponse(status_code=201, content=result)
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error subiendo a Blob: {e}")
    
# -----------------------------
# MODELO RESPONSE
# -----------------------------
class GenerateIdCargaResponse(BaseModel):
    id_carga: str


# -----------------------------
# ENDPOINT GENERAR ID CARGA
# -----------------------------
@app.get("/dashboard/id-carga", response_model=GenerateIdCargaResponse)
async def generate_id_carga():
    id_carga = f"UPL-{uuid.uuid4().hex[:8]}"
    return GenerateIdCargaResponse(id_carga=id_carga)