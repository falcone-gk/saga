import json
import re
from typing import Any, Optional

import pendulum
from bs4 import BeautifulSoup

from core.logging import get_logger
from scraper.scrapers.sagafalabella.client import (
    fetch_html_product_extra_details,
)
from scraper.scrapers.sagafalabella.constants import CATEGORY_LOOKUP
from scraper.scrapers.sagafalabella.schemas import RawProduct, ScrapedProduct
from scraper.utils.text import extraer_peso, limpiar_html

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
        extraer_peso(product.displayName)
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


def get_category_name_by_id(category_id: Optional[str]) -> Optional[str]:
    if not category_id:
        return None

    category_info = CATEGORY_LOOKUP.get(category_id.upper())
    if category_info:
        return category_info.get("category_label")

    return None


def get_product_detail(sku: str, url: str) -> tuple[str | None, str | None]:
    try:
        content = fetch_html_product_extra_details(url)

        if content is None:
            logger.warning(f"No se pudo extraer detalle del sku {sku}")
            return None, None

        soup = BeautifulSoup(content, "html.parser")

        next_data_script = soup.find("script", id="__NEXT_DATA__")
        if not next_data_script or not next_data_script.string:
            return None, None

        data_json = json.loads(next_data_script.string)
        product_info = (
            data_json.get("props", {})
            .get("pageProps", {})
            .get("productData", {})
        )

        # ---- descripción ----
        raw_description = product_info.get(
            "longDescription"
        ) or product_info.get("description")
        description = (
            limpiar_html(raw_description)
            if isinstance(raw_description, str)
            else None
        )

        if not description:
            logger.warning(f"No se pudo extraer descripción del sku {sku}")

        # ---- categoría ----
        category: str | None = None
        breadcrumb_link = soup.select_one(
            "a.Breadcrumbs-module_selected-bread-crumb__ZPj02"
        )

        if breadcrumb_link:
            href = breadcrumb_link.get("href")
            if isinstance(href, str):
                match = re.search(r"/category/([^/]+)", href)
                if match:
                    category_id = match.group(1)
                    category = get_category_name_by_id(category_id)

        return category, description

    except Exception as e:
        logger.error(f"Error en sku {sku}: {e}")
        return None, None
