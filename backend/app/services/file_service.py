import pandas as pd


def analyze_csv(file_path: str):
    df = pd.read_csv(file_path)

    analysis = {
        "columns": list(df.columns),
        "shape": df.shape,
        "missing_values": df.isnull().sum().to_dict(),
        "describe": df.describe().to_dict(),
    }

    return analysis
