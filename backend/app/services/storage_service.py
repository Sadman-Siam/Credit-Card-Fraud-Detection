import os

import duckdb
import pandas as pd

RAW_DIR = "data/raw"
PARQUET_DIR = "data/parquet"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PARQUET_DIR, exist_ok=True)


def save_csv(file, filename):
    file_path = os.path.join(RAW_DIR, filename)

    with open(file_path, "wb") as buffer:
        buffer.write(file.file.read())

    return file_path


def convert_to_parquet(csv_path):
    df = pd.read_csv(csv_path)

    parquet_name = os.path.basename(csv_path).replace(".csv", ".parquet")
    parquet_path = os.path.join(PARQUET_DIR, parquet_name)

    df.to_parquet(parquet_path)

    return parquet_path


def run_query(query):
    con = duckdb.connect()
    result = con.execute(query).fetchdf()
    return result
