import pendulum
import requests

from constants import STRUCTURE_DATA
from db.models import WebScrappingSagaFalabella
from db.session import SessionLocal
from utils.text import extraer_peso, limpiar_html

# API publica de saga que entrega una lista con paginacion de los productos
BASE_URL = (
    "https://www.falabella.com.pe/s/browse/v1/listing/pe"
    "?page={page}&categoryId={category_id}&categoryName={category_name}&pid=799c102f-9b4c-44be-a421-23e366a63b82"
)


# API publica de productos de saga utilizado solo para obtener la descripcion
PRODUCT_URL = "https://www.falabella.com.pe/s/browse/v3/product/pe?productId={}"


def obtener_descripcion_producto(product_id):
    url = PRODUCT_URL.format(product_id)

    try:
        response = requests.get(url, timeout=10)
    except Exception as e:
        print(f"ERROR: Falló conexión: {e}")
        return None

    if response.status_code != 200:
        print(f"ERROR status {response.status_code} {url} → Fin.")
        return None

    data = response.json()
    descripcion_raw = data.get("data", {}).get("description", None)

    if descripcion_raw is None:
        return None

    descripcion = limpiar_html(descripcion_raw)

    return descripcion


def fetch_data(page, category_id, category_name):
    """Descarga una página de productos según page y category_id."""
    url = BASE_URL.format(
        page=page, category_id=category_id, category_name=category_name
    )
    print(f"Descargando página {page}: {url}")

    try:
        response = requests.get(url, timeout=10)
    except Exception as e:
        print(f"ERROR Falló conexión: {e}")
        return None

    if response.status_code != 200:
        print(f"Status {response.status_code} {url} → Fin.")
        return None

    data = response.json()
    results = data.get("data", {}).get("results", [])

    if not results:
        print("Página sin resultados → Fin.")
        return None

    return results


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


def insert_webscrapping(data):
    db = SessionLocal()
    try:
        db.bulk_insert_mappings(
            WebScrappingSagaFalabella,
            data,
        )
        db.commit()
    finally:
        db.close()


def scrape_all():
    all_products = []
    counter = 0

    for animal, info in STRUCTURE_DATA.items():
        for categoria in info["categorias"]:
            nombre_categoria = list(categoria.keys())[0]
            categoria_id = categoria[nombre_categoria]["id"]
            category_name = categoria[nombre_categoria]["category_name"]

            print(
                f"\n--- Iniciando scraping de {animal} / {nombre_categoria} "
                f"(ID={categoria_id}) ---\n"
            )

            page = 1
            while True:
                # for _ in range(1):
                fecha_inicio = pendulum.now("America/Lima").to_iso8601_string()
                products = fetch_data(page, categoria_id, category_name)
                if products is None:
                    break

                for product in products:
                    precio_antes, precio_despues, precio_cmr = extraer_precio(
                        product
                    )
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
                        "categoria_producto": nombre_categoria,
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

                    all_products.append(result)

                page += 1

            # Insertamos los datos de una categoria
            insert_webscrapping(all_products)
            print(
                f"\nINFO: Se realizó el scraping de {len(all_products)} productos {animal} / {nombre_categoria}."
            )
            # Añadimos al counter total
            counter += len(all_products)

            # Limpiamos memoria
            all_products = []

    return counter
