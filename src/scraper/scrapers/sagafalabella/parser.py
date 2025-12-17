import pendulum

from scraper.utils.text import extraer_peso


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

    print("Extrayendo datos del producto con sku:", sku)

    result = {
        "categoria_animal": animal,
        "categoria_producto": category_name,
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
    }

    # TODO: Verificar si es necesario obtener la descripcion ya que aumenta el tiempo de scraping
    # considerablemente
    # descripcion = obtener_descripcion_producto(product_id)
    # result["descripcion_producto"] = descripcion

    result["fecha_extraccion_final"] = pendulum.now(
        "America/Lima"
    ).to_iso8601_string()

    # print(
    #     "Extrayendo de datos del producto con sku:",
    #     sku,
    #     "finalizada correctamente",
    # )

    return result
