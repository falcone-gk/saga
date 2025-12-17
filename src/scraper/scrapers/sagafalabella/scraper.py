# sites/falabella/scraper.py

from .client import fetch_products_page
from .constants import STRUCTURE_DATA
from .parser import extraer_producto
from .repository import bulk_insert


def scrape():
    total = 0

    for animal, info in STRUCTURE_DATA.items():
        for categoria in info["categorias"]:
            page = 1

            nombre_categoria = list(categoria.keys())[0]
            categoria_id = categoria[nombre_categoria]["id"]
            category_name = categoria[nombre_categoria]["category_name"]

            while True:
                # Extraea el json completo de cada producto
                products = fetch_products_page(
                    page, categoria_id, category_name
                )

                # Si no hay productos, se sale del bucle porque ya no hay mas paginas
                # en la categoria
                if products is None:
                    break

                # Extrae solo los campos que deseamos de cada producto
                parsed = [
                    extraer_producto(animal, p, category_name) for p in products
                ]
                bulk_insert(parsed)
                page += 1

            total += len(parsed)

    return total
