def main():
    import pandas as pd
    import os
    from deltalake import write_deltalake
    from config import BRONZE_PATH, SILVER_PATH



    os.makedirs("data/silver", exist_ok=True)

    # Leer parquet
    files = [f for f in os.listdir(BRONZE_PATH) if f.endswith(".parquet")]
    file_path = os.path.join(BRONZE_PATH, files[0])

    df = pd.read_parquet(file_path)

    # 🔹 Normalizar nombres de columnas
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # 🔹 Eliminar columnas irrelevantes
    columns_to_drop = ["cabin", "ticket"]
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    # 🔹 Tratar nulos
    df["age"] = df["age"].fillna(df["age"].median())
    df["embarked"] = df["embarked"].fillna("Unknown")

    # 🔹 Convertir tipos
    df["survived"] = df["survived"].astype(int)
    df["pclass"] = df["pclass"].astype(int)

    # 🔹 Data Quality Checks

    if df["survived"].isnull().sum() > 0:
        raise Exception("Hay valores nulos en 'survived'")

    if len(df) == 0:
        raise Exception("El dataframe está vacío")

    print("Data Quality OK ✅")

    # Guardar como Delta
    write_deltalake(SILVER_PATH, df, mode="overwrite")

    print("Silver generado correctamente ✅")
    print(f"Registros: {len(df)}")
if __name__ == "__main__":
    main()