import logging
from config import LOG_PATH
from bronze import main as bronze_main
from silver import main as silver_main
from gold import main as gold_main

def setup_logging():
    logging.basicConfig(
        filename=LOG_PATH,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def main():

    setup_logging()
    logging.info("Pipeline iniciado")

    try:
        logging.info("Ejecutando Bronze")
        bronze_main()
    except Exception as e:
        logging.error(f"Error en Bronze: {e}")
        return

    try:
        logging.info("Ejecutando Silver")
        silver_main()
    except Exception as e:
        logging.error(f"Error en Silver: {e}")
        return

    try:
        logging.info("Ejecutando Gold")
        gold_main()
    except Exception as e:
        logging.error(f"Error en Gold: {e}")
        return

    logging.info("Pipeline completado correctamente")
    print("Pipeline completado ✅")

if __name__ == "__main__":
    main()