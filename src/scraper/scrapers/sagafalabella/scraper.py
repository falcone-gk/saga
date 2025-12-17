import sys

from scraper.core.logging import get_logger

from .client import fetch_products_page
from .constants import STRUCTURE_DATA
from .parser import extraer_producto
from .repository import bulk_insert_falabella

logger = get_logger(__name__)


def scrape():
    total_general = 0
    # Iteramos sobre la estructura definida en constantes
    for animal, info in STRUCTURE_DATA.items():
        for categoria_dict in info["categorias"]:
            page = 1
            # Extraemos la info de la categoría (asumiendo estructura de tu dict)
            nombre_categoria = list(categoria_dict.keys())[0]
            datos_cat = categoria_dict[nombre_categoria]

            categoria_id = datos_cat["id"]
            category_name = datos_cat["category_name"]

            logger.info(
                "Iniciando scraping de %s > %s", animal, nombre_categoria
            )

            while True:
                products = fetch_products_page(
                    page, categoria_id, category_name
                )

                if products is None:  # Maneja None o lista vacía
                    break

                parsed = [
                    extraer_producto(animal, p, category_name) for p in products
                ]

                bulk_insert_falabella(parsed)
                logger.info(
                    "Página %s: %s productos guardados.", page, len(parsed)
                )

                page += 1
                total_general += len(parsed)

    logger.info("Total de productos procesados: %s", total_general)


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
