import sys

from core.logging import get_logger
from scraper.scrapers.sagafalabella.client import fetch_products_page
from scraper.scrapers.sagafalabella.constants import STRUCTURE_DATA
from scraper.scrapers.sagafalabella.parser import extraer_producto
from scraper.scrapers.sagafalabella.repository import guardar_parsed_temporal

logger = get_logger(__name__)


def filtrar_productos_nuevos(products, skus_vistos):
    productos_nuevos = []
    for producto in products:
        sku = producto.get("skuId")
        if sku not in skus_vistos:
            skus_vistos.add(sku)
            productos_nuevos.append(producto)
    return productos_nuevos


def scrape():
    # counter = 0
    # Iteramos sobre la estructura definida en constantes
    for animal, info in STRUCTURE_DATA.items():
        parsed_total = []
        skus_vistos = set()
        for categoria_dict in info["categorias"]:
            page = 1
            # Extraemos la info de la categoría (asumiendo estructura de tu dict)
            nombre_categoria = list(categoria_dict.keys())[0]
            datos_categoria = categoria_dict[nombre_categoria]

            categoria_id = datos_categoria["id"]
            category_name = datos_categoria["category_name"]

            logger.info(
                "Iniciando scraping de %s -> %s", animal, nombre_categoria
            )

            # Iteramos sobre los productos de la categoría
            counter_categoria = 0
            while True:
                logger.info("Scrapeando pagina %s", page)
                productos = fetch_products_page(
                    page, categoria_id, category_name
                )

                if productos is None:
                    logger.info("No hay mas productos para scrapear")
                    break

                # No consideramos productos repetidos dentro de una misma categoria
                productos_nuevos = filtrar_productos_nuevos(
                    productos, skus_vistos
                )

                # TODO: El extraer_producto no tomará el campo "categoria_producto" porque se llenará en el siguiente
                # job cuando se extraiga el detalle del producto.
                parsed = [
                    extraer_producto(animal, p, category_name)
                    for p in productos_nuevos
                ]
                parsed_total.extend(parsed)

                logger.info(
                    "Total de productos scrapeados de %s -> %s (pagina %s): %s",
                    animal,
                    nombre_categoria,
                    page,
                    len(parsed),
                )
                page += 1
                counter_categoria += len(parsed)

            # Insertamos los datos de la categoria
            logger.info(
                "Total de productos scrapeados (%s -> %s): %s",
                animal,
                nombre_categoria,
                counter_categoria,
            )

    guardar_parsed_temporal(parsed_total)
    logger.info("Total de productos procesados: %s", len(parsed_total))


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
