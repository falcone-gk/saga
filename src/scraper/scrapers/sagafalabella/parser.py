import json
from typing import Any

import pendulum
from bs4 import BeautifulSoup

from core.logging import get_logger
from scraper.scrapers.sagafalabella.client import (
    fetch_html_product_extra_details,
)
from scraper.scrapers.sagafalabella.schemas import RawProduct, ScrapedProduct
from scraper.utils.text import clean_html, get_weight_from_text

logger = get_logger(__name__)


def get_prices(
    product: RawProduct,
) -> tuple[float | None, float | None, float | None]:
    normal_price: float | None = None
    discounted_price: float | None = None
    precio_cmr: float | None = None

    for p in product.prices:
        _type = p.type
        crossed = p.crossed
        list_prices = p.price

        if not list_prices:
            continue

        try:
            price = float(list_prices[0])
        except (ValueError, TypeError, IndexError):
            continue

        if _type == "normalPrice" and crossed:
            normal_price = price
        elif _type == "cmrPrice" and not crossed:
            precio_cmr = price
        elif _type in ("eventPrice", "internetPrice") and not crossed:
            discounted_price = price

        if discounted_price is None and not crossed and _type != "cmrPrice":
            normal_price = price

    return normal_price, discounted_price, precio_cmr


def get_product_data(
    animal: str, product_dict: dict[str, Any], category_name: str
) -> ScrapedProduct:
    start_date = pendulum.now("America/Lima").to_iso8601_string()

    # Validamos el diccionario de entrada
    product = RawProduct(**product_dict)
    normal_price, discounted_price, precio_cmr = get_prices(product)

    peso = (
        get_weight_from_text(product.displayName)
        if category_name == "Alimentos"
        else None
    )

    # Construimos el objeto final usando el modelo para asegurar consistencia
    result = ScrapedProduct(
        categoria_animal=animal,
        categoria_producto=None,
        marca=product.brand,
        nombre=product.displayName,
        vendido_por=product.sellerName,
        descripcion_producto=None,
        peso_considerado=peso,
        precio_sin_descuento=normal_price,
        precio_publico=discounted_price,
        precio_cmr=precio_cmr,
        fecha_extraccion_inicio=start_date,
        fecha_extraccion_final=None,
        product_id=product.productId,
        sku=product.skuId,
        url=product.url,
    )

    result.fecha_extraccion_final = pendulum.now(
        "America/Lima"
    ).to_iso8601_string()

    return result


def get_breadcrumb_categories(
    soup: BeautifulSoup,
) -> tuple[str | None, str | None]:
    """
    Extrae los dos siguientes niveles después de omitir los dos primeros.
    Ejemplo: [Home, Mascotas, Higiene, Cepillos, Madera]
    -> Cat: Higiene, Sub: Cepillos
    """
    # Buscamos la lista de breadcrumbs
    ol = soup.find("ol", class_="Breadcrumbs-module_breadcrumb__b47ha")
    if not ol:
        return None, None

    # Obtenemos todos los textos de los enlaces
    links = [a.get_text(strip=True) for a in ol.find_all("a")]

    # Cortamos para ignorar los dos primeros (Home y Raíz)
    # links_to_use contendrá todo lo que viene después
    links_to_use = links[2:]

    # Inicializamos variables
    category = None
    sub_category = None

    # Tomamos el primero de la lista restante como categoría
    if len(links_to_use) >= 1:
        category = links_to_use[0]

    # Tomamos el segundo de la lista restante como subcategoría (si existe)
    if len(links_to_use) >= 2:
        sub_category = links_to_use[1]

    return category, sub_category


def get_product_detail(
    sku: str, url: str
) -> tuple[str | None, str | None, str | None]:
    try:
        content = fetch_html_product_extra_details(url)

        if content is None:
            logger.warning(f"No se pudo extraer detalle del sku {sku}")
            return None, None, None

        soup = BeautifulSoup(content, "html.parser")

        # Extraer Categorías (Nueva lógica integrada)
        category, sub_category = get_breadcrumb_categories(soup)

        # Extraer Descripción (Tu lógica original de JSON)
        next_data_script = soup.find("script", id="__NEXT_DATA__")
        description = None

        if next_data_script and next_data_script.string:
            data_json = json.loads(next_data_script.string)
            product_info = (
                data_json.get("props", {})
                .get("pageProps", {})
                .get("productData", {})
            )
            raw_description = product_info.get(
                "longDescription"
            ) or product_info.get("description")
            description = (
                clean_html(raw_description)
                if isinstance(raw_description, str)
                else None
            )

        if not description:
            logger.warning(f"No se pudo extraer descripción del sku {sku}")

        # Ahora retornamos 3 valores
        return category, sub_category, description

    except Exception as e:
        logger.error(f"Error en sku {sku}: {e}")
        return None, None, None
