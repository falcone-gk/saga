from scraper.core.logging import get_logger
from scraper.core.settings import settings
from scraper.scrapers.sagafalabella.repository import (
    bulk_insert_falabella_from_parquet,
)

logger = get_logger(__name__)


def save_to_sql():
    tmp_file = settings.TMP_DIR / "saga_falabella_updated.parquet"
    bulk_insert_falabella_from_parquet(tmp_file)


def main():
    logger.info("Guardando datos scrapeados a SQL")
    save_to_sql()
    logger.info("Datos scrapeados a SQL guardados de manera exitosa")


if __name__ == "__main__":
    main()
