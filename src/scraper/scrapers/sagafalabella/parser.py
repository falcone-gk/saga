import json
import re

import pendulum
from bs4 import BeautifulSoup

from core.logging import get_logger
from scraper.scrapers.sagafalabella.client import (
    fetch_html_product_extra_details,
)
from scraper.scrapers.sagafalabella.constants import STRUCTURE_DATA
from scraper.utils.text import extraer_peso, limpiar_html

logger = get_logger(__name__)


def extraer_precio(product):
    prices = product.get("prices", [])
    if not prices:
        return None, None, None

    precio_antes = None
    precio_descuento = None
    precio_cmr = None

    for p in prices:
        tipo = p.get("type", "")
        crossed = p.get("crossed", False)
        precio_lista = p.get("price", [])

        if not precio_lista:
            continue

        try:
            precio = float(precio_lista[0])
        except (ValueError, TypeError, IndexError):
            # El 'continue' hace que se salte el producto al no tener un precio con formato correcto
            # pasaria al siguiente de la lista
            continue

        # Precio normal (tachado)
        if tipo == "normalPrice" and crossed:
            precio_antes = precio

        # Precio CMR
        if tipo == "cmrPrice" and not crossed:
            precio_cmr = precio

        # Precio con descuento
        if tipo in ("eventPrice", "internetPrice") and not crossed:
            precio_descuento = precio

        # Si no tiene eventPrice/internetPrice, usar el único sin crossed
        if (
            precio_descuento is None
            and not crossed
            and tipo not in ("cmrPrice",)
        ):
            precio_antes = precio

    return precio_antes, precio_descuento, precio_cmr


def extraer_producto(animal, product, category_name):
    fecha_inicio = pendulum.now("America/Lima").to_iso8601_string()
    precio_antes, precio_despues, precio_cmr = extraer_precio(product)
    nombre = product.get("displayName")
    sku = product.get("skuId")
    product_id = product.get("productId")

    # No tiene sentido obtener el peso de productos que no son alimentos
    peso = None
    if category_name == "Alimentos":
        peso = extraer_peso(nombre)

    logger.info("Extrayendo datos del producto con sku: %s", sku)

    result = {
        "categoria_animal": animal,
        "categoria_producto": None,
        "sub_categoria_producto": None,
        "marca": product.get("brand"),
        "nombre": nombre,
        "vendido_por": product.get("sellerName"),
        "titulo_promocion": None,
        "descripcion_promocion": None,
        "descripcion_producto": None,
        "peso_considerado": peso,
        "precio_sin_descuento": precio_antes,
        "precio_publico": precio_despues,
        "precio_cmr": precio_cmr,
        "fecha_extraccion_inicio": fecha_inicio,
        "fecha_extraccion_final": None,
        "product_id": product_id,
        "sku": sku,
        "url": product.get("url"),
    }

    result["fecha_extraccion_final"] = pendulum.now(
        "America/Lima"
    ).to_iso8601_string()

    return result


def obtener_categoria_por_category_id(category_id):
    for data in STRUCTURE_DATA.values():
        for categoria in data.get("categorias", []):
            for nombre_categoria, info in categoria.items():
                if info.get("id", "").upper() == category_id.upper():
                    return nombre_categoria

    return None


def extraer_extra_detalle_producto(sku, url):
    logger.info("Extrayendo detalle del producto con sku: %s", sku)
    try:
        content = fetch_html_product_extra_details(url)
        soup = BeautifulSoup(content, "html.parser")

        next_data_script = soup.find("script", id="__NEXT_DATA__")
        if (not next_data_script) or (not next_data_script.string):
            return None

        data = json.loads(next_data_script.string)

        # Extraccion de la descripcion del producto
        data = data.get("props", {}).get("pageProps", {}).get("productData", {})
        raw_description = data.get("longDescription", None)

        # Se vio casos en el que longDescription es empty string
        # por lo tanto se toma la descripcion si no es None
        long_desc = data.get("longDescription")
        raw_description = long_desc if long_desc else data.get("description")

        description = limpiar_html(raw_description)
        if description is None:
            logger.warning(
                "No se pudo extraer la descripcón del sku %s de la url %s",
                sku,
                url,
            )

        # Extraccion de la categoria del producto
        a = soup.select_one("a.Breadcrumbs-module_selected-bread-crumb__ZPj02")
        url_category = a["href"]

        match = re.search(r"/category/([^/]+)", url_category)
        if match is None:
            logger.warning(
                "No se pudo extraer la categoria del producto con sku %s de la url %s",
                sku,
                url,
            )
            category_id = None
        else:
            category_id = match.group(1)

        category = obtener_categoria_por_category_id(category_id)

        return category, description

    except Exception as e:
        logger.error(
            "Error obteniendo detalle del producto con sku %s de la url %s: %s",
            sku,
            url,
            e,
        )
        return None, None
