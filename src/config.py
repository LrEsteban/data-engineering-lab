import os

BASE_DATA_PATH = "data"

BRONZE_PATH = os.path.join(BASE_DATA_PATH, "bronze")
SILVER_PATH = os.path.join(BASE_DATA_PATH, "silver", "titanic_silver")
GOLD_PATH = os.path.join(BASE_DATA_PATH, "gold")

LOG_PATH = "pipeline.log"