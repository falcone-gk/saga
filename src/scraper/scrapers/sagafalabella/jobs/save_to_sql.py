import pandas as pd

from core.logging import get_logger
from core.schemas import PostgresConfig
from core.settings import settings
from services.postgres import PostgresManager

logger = get_logger(__name__)


def main():
    logger.info("Guardando datos scrapeados a SQL")

    # Obtener el dataframe
    parquet_path = settings.TMP_DIR / "saga_falabella_updated.parquet"
    data = pd.read_parquet(parquet_path, engine="pyarrow").drop(columns=["url"])

    config = PostgresConfig(
        host="localhost",
        port=5432,
        user="postgres",
        password="root",
        database="test_biomont",
    )
    db = PostgresManager(config)
    db.save_dataframe(data, "webscrapping_sagafalabella2")

    logger.info("Datos scrapeados a SQL guardados de manera exitosa")


if __name__ == "__main__":
    main()
