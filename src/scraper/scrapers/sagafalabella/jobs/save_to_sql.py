import pandas as pd

from core.logging import get_logger
from core.schemas import PostgresConfig
from core.settings import settings
from services.datalake import DataLakeManager
from services.postgres import PostgresManager

logger = get_logger(__name__)


def main():
    logger.info("Guardando datos scrapeados a SQL")

    # Obtener el dataframe
    parquet_path = settings.TMP_DIR / "saga_falabella_updated.parquet"
    datalake = DataLakeManager(connection_type="local")
    data = datalake.read_data(parquet_path, fmt="parquet")

    if not isinstance(data, pd.DataFrame):
        logger.error("No se pudo leer el archivo de datos")
        return

    config = PostgresConfig(
        host="localhost",
        port=5432,
        user="postgres",
        password="root",
        database="test_biomont",
    )
    db = PostgresManager(config)
    db.save_dataframe(data.drop(columns=["url"]), "webscrapping_sagafalabella2")

    logger.info("Datos scrapeados a SQL guardados de manera exitosa")


if __name__ == "__main__":
    main()
