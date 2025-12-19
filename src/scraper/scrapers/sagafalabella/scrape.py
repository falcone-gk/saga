import sys

from scraper.core.logging import get_logger
from scraper.scrapers.sagafalabella.client import fetch_products_page
from scraper.scrapers.sagafalabella.constants import STRUCTURE_DATA
from scraper.scrapers.sagafalabella.parser import extraer_producto
from scraper.scrapers.sagafalabella.repository import bulk_insert_falabella

logger = get_logger(__name__)


def scrape():
    # Se realiza este diccionario sabiendo que solo extraremos perros y gatos
    animales_dict = {
        "perro": set(),
        "gato": set(),
    }
    parsed_total = []
    sku_index = {}

    # Iteramos sobre la estructura definida en constantes
    for animal, info in STRUCTURE_DATA.items():
        # skus_vistos = set()
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
                products = fetch_products_page(
                    page, categoria_id, category_name
                )
                insertados_pagina = 0

                if products is None:
                    logger.info("No hay mas productos para scrapear")
                    break

                # No consideramos productos repetidos dentro de una misma categoria
                productos_nuevos = []
                for producto in products:
                    sku = producto.get("skuId")
                    if sku not in animales_dict[animal]:
                        animales_dict[animal].add(sku)
                        productos_nuevos.append(producto)

                # TODO: En extraer_producto el campo "categoria_producto" se tomará del detalle del producto
                # Al relizar el TODO, se fixea los casos de duplicados mismo animal distinto categoria producto
                # parsed = [
                #     extraer_producto(animal, p, category_name)
                #     for p in productos_nuevos
                # ]
                # parsed_total.extend(parsed)

                # Nueva logica para casos de duplicados en distintos animales
                for p in productos_nuevos:
                    sku = p.get("skuId")

                    if sku in sku_index:
                        # El SKU ya existe para el otro animal
                        idx = sku_index[sku]
                        parsed_total[idx]["categoria_animal"] = "perro-gato"
                        logger.info(
                            "Se sobreescribio el producto %s -> %s",
                            parsed_total[idx]["sku"],
                            parsed_total[idx]["nombre"],
                        )
                    else:
                        producto_parseado = extraer_producto(
                            animal, p, category_name
                        )
                        sku_index[sku] = len(parsed_total)
                        parsed_total.append(producto_parseado)

                        counter_categoria += 1
                        insertados_pagina += 1

                logger.info(
                    "Total de productos scrapeados (pagina %s): %s",
                    page,
                    insertados_pagina,
                )
                page += 1

            # Insertamos los datos de la categoria
            logger.info(
                "Total de productos scrapeados (%s -> %s): %s",
                animal,
                nombre_categoria,
                counter_categoria,
            )

    bulk_insert_falabella(parsed_total)
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
