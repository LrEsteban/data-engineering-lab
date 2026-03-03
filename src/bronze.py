def main():
    import pandas as pd
    import os
    from config import BRONZE_PATH

 

    files = [f for f in os.listdir(BRONZE_PATH) if f.endswith(".parquet")]

    if not files:
        raise Exception("No se encontró ningún parquet en data/bronze")

    file_path = os.path.join(BRONZE_PATH, files[0])

    print(f"Leyendo archivo: {file_path}")

    df = pd.read_parquet(file_path)

    print("\nColumnas:")
    print(df.columns)

    print("\nPrimeras filas:")
    print(df.head())

    print(f"\nTotal registros: {len(df)}")
if __name__ == "__main__":
    main()