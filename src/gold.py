def main():
    import os
    import pandas as pd
    from deltalake import DeltaTable
    from config import SILVER_PATH, GOLD_PATH

    print("🚀 Iniciando capa GOLD...")

    # 🔹 1. Leer datos desde Silver (Delta)
    SILVER_PATH = "data/silver/titanic_silver"

    if not os.path.exists(SILVER_PATH):
        raise Exception("La capa Silver no existe. Ejecuta primero silver.py")

    dt = DeltaTable(SILVER_PATH)
    df = dt.to_pandas()

    print("Datos cargados desde Silver ✅")

    # 🔹 2. Data Quality básica
    if len(df) == 0:
        raise Exception("El dataframe está vacío")

    # 🔹 3. Métricas por clase
    metrics_class = df.groupby("pclass").agg(
        total_passengers=("passengerid", "count"),
        survival_rate=("survived", "mean"),
        avg_age=("age", "mean"),
        avg_fare=("fare", "mean")
    ).reset_index()

    # 🔹 4. Métricas por género
    metrics_gender = df.groupby("sex").agg(
        total_passengers=("passengerid", "count"),
        survival_rate=("survived", "mean")
    ).reset_index()

    # 🔹 5. Crear carpeta gold si no existe
    
    os.makedirs(GOLD_PATH, exist_ok=True)

    # 🔹 6. Guardar resultados en Parquet
    metrics_class.to_parquet(f"{GOLD_PATH}/metrics_by_class.parquet", index=False)
    metrics_gender.to_parquet(f"{GOLD_PATH}/metrics_by_gender.parquet", index=False)

    print("Capa GOLD generada correctamente 🎯")


if __name__ == "__main__":
    main()