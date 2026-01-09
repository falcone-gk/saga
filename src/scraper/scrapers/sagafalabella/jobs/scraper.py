import sys
from collections import defaultdict
from typing import DefaultDict, TypedDict

from core.logging import get_logger
from scraper.scrapers.sagafalabella.client import fetch_products_page
from scraper.scrapers.sagafalabella.constants import (
    CATEGORY_LOOKUP,
)
from scraper.scrapers.sagafalabella.parser import get_product_data
from scraper.scrapers.sagafalabella.schemas import (
    CategoryMetadata,
    ScrapedProduct,
)

logger = get_logger(__name__)


class ProductDict(TypedDict):
    skuId: str


def get_new_products(
    products: list[ProductDict],
    skus_stored: set[str],
) -> list[ProductDict]:
    new_products: list[ProductDict] = []

    for producto in products:
        sku = producto["skuId"]
        if sku not in skus_stored:
            skus_stored.add(sku)
            new_products.append(producto)

    return new_products


def scrape() -> list[ScrapedProduct]:
    all_scraped_data: list[ScrapedProduct] = []

    categories_by_animal: DefaultDict[
        str, list[tuple[str, CategoryMetadata]]
    ] = defaultdict(list)

    for cat_id, metadata in CATEGORY_LOOKUP.items():
        categories_by_animal[metadata["animal"]].append((cat_id, metadata))

    for animal, categories in categories_by_animal.items():
        logger.info(f"=== Iniciando scraping para el animal: {animal} ===")

        skus_stored_for_animal: set[str] = set()

        for cat_id, metadata in categories:
            category_label = metadata["category_label"]
            category_name = metadata["category_url"]

            page = 1
            category_counter = 0
            logger.info(f"Iniciando scraping de {animal} -> {category_label}")

            while True:
                logger.info(f"Scrapeando pagina {page}")
                products: list[ProductDict] = fetch_products_page(
                    page, cat_id, category_name
                )

                if not products:
                    break

                new_products = get_new_products(
                    products, skus_stored_for_animal
                )

                parsed = [
                    get_product_data(animal, p, category_label)
                    for p in new_products
                ]

                all_scraped_data.extend(parsed)
                category_counter += len(parsed)

                logger.info(
                    f"Total de productos scrapeados de {animal} -> {category_label}: (pagina {page}): {len(parsed)}",
                )

                page += 1

            logger.info(
                f"Finalizado {category_label}: "
                f"{category_counter} productos nuevos para {animal}."
            )

    logger.info(f"Scraping completado. Total global: {len(all_scraped_data)}")
    return all_scraped_data


def main():
    import pandas as pd

    from core.settings import settings
    from scraper.scrapers.sagafalabella.schemas import SCRAPED_PRODUCT_SCHEMA

    try:
        logger.info("=== INICIANDO SCRAPER SAGA FALABELLA ===")

        data = scrape()
        data_dicts = [p.model_dump() for p in data]

        if not data:
            logger.warning("No se obtuvieron datos del scrapeo.")
            return

        # Guardar los datos en un parquet
        tmp_file = settings.TMP_DIR / "saga_falabella.parquet"
        df = pd.DataFrame(data_dicts)
        df.to_parquet(
            tmp_file,
            engine="pyarrow",
            index=False,
            schema=SCRAPED_PRODUCT_SCHEMA,
        )

        logger.info(f"Archivo temporal generado en: {tmp_file}")
        logger.info("=== PROCESO FINALIZADO CON ÉXITO ===")

    except KeyboardInterrupt:
        logger.warning(
            "\nCapturado: Scrapeo interrumpido por el usuario (Ctrl+C)"
        )
        sys.exit(0)
    except Exception as e:
        logger.exception(f"Error crítico durante la ejecución: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
