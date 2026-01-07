import sys

from core.logging import get_logger
from scraper.scrapers.sagafalabella.client import fetch_products_page
from scraper.scrapers.sagafalabella.constants import STRUCTURE_DATA
from scraper.scrapers.sagafalabella.parser import get_product_data

logger = get_logger(__name__)


def get_new_products(products, skus_stored):
    productos_nuevos = []
    for producto in products:
        sku = producto.get("skuId")
        if sku not in skus_stored:
            skus_stored.add(sku)
            productos_nuevos.append(producto)
    return productos_nuevos


def scrape():
    # Iteramos sobre la estructura definida en constantes
    for animal, info in STRUCTURE_DATA.items():
        scraped_data = []
        skus_stored = set()
        for dict_category in info["categorias"]:
            page = 1
            # Extraemos la info de la categoría (asumiendo estructura de tu dict)
            category_label = list(dict_category.keys())[0]
            category_data = dict_category[category_label]

            category_id = category_data["id"]
            category_name = category_data["category_name"]

            logger.info(
                "Iniciando scraping de %s -> %s", animal, category_label
            )

            # Iteramos sobre los productos de la categoría
            category_counter = 0
            while True:
                logger.info("Scrapeando pagina %s", page)
                products = fetch_products_page(page, category_id, category_name)

                if products is None:
                    logger.info("No hay mas productos para scrapear")
                    break

                # No consideramos productos repetidos dentro de una misma categoria
                new_products = get_new_products(products, skus_stored)

                parsed = [
                    get_product_data(animal, p, category_label)
                    for p in new_products
                ]
                scraped_data.extend(parsed)

                logger.info(
                    "Total de productos scrapeados de %s -> %s (pagina %s): %s",
                    animal,
                    category_label,
                    page,
                    len(parsed),
                )
                page += 1
                category_counter += len(parsed)

            # Insertamos los datos de la categoria
            logger.info(
                "Total de productos scrapeados (%s -> %s): %s",
                animal,
                category_label,
                category_counter,
            )

    # guardar_parsed_temporal(scraped_data)
    logger.info("Total de productos procesados: %s", len(scraped_data))
    return scraped_data


def main():
    """Punto de entrada para el script"""
    logger.info("=== INICIANDO SCRAPER SAGA FALABELLA ===")

    try:
        scrape()
        logger.info("=== PROCESO FINALIZADO ===")

    except KeyboardInterrupt:
        logger.warning("Scrapeo interrumpido por el usuario")
        sys.exit(0)

    except Exception:
        # incluye stacktrace completo
        logger.exception("Error crítico durante la ejecución")
        sys.exit(1)


if __name__ == "__main__":
    main()
