from fastapi import APIRouter, File, UploadFile

from app.services.file_service import analyze_csv
from app.services.storage_service import convert_to_parquet, save_csv

router = APIRouter()


@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    csv_path = save_csv(file, file.filename)

    parquet_path = convert_to_parquet(csv_path)

    analysis = analyze_csv(csv_path)

    return {
        "filename": file.filename,
        "csv_path": csv_path,
        "parquet_path": parquet_path,
        "analysis": analysis,
    }
