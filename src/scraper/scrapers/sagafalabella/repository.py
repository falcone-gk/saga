import pandas as pd

from scraper.core.logging import get_logger
from scraper.core.settings import settings
from scraper.database.session import SessionLocal

from .models import WebScrappingSagaFalabella

logger = get_logger(__name__)


def bulk_insert_falabella(products):
    session = SessionLocal()
    try:
        session.bulk_insert_mappings(WebScrappingSagaFalabella, products)
        session.commit()
    finally:
        session.close()


def guardar_parsed_temporal(parsed_lote):
    """
    Guarda un lote de productos en un archivo parquet temporal.
    Si el archivo existe, agrega (append).
    """
    if not parsed_lote:
        logger.warning("No hay datos en el dataframe")
        return

    tmp_file = settings.TMP_DIR / "saga_falabella.parquet"

    df = pd.DataFrame(parsed_lote)

    # Si el archivo existe, agrega (append)
    df.to_parquet(
        tmp_file,
        engine="fastparquet",
        index=False,
    )

    logger.info("Archivo temporal de saga_falabella.parquet generado")


def save_parsed_updated(data):
    """
    Guarda un lote de productos en un archivo parquet temporal.
    Si el archivo existe, actualiza (overwrite).
    """
    if data is None or data.empty:
        logger.warning("No hay datos en el dataframe")
        return

    tmp_file = settings.TMP_DIR / "saga_falabella_updated.parquet"

    # Si el archivo existe, actualiza (overwrite)
    data.to_parquet(tmp_file, engine="fastparquet", index=False)

    logger.info(
        "Archivo temporal de saga_falabella_updated.parquet actualizado"
    )
