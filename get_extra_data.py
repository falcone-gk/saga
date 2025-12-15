import json

import pendulum
import requests

from utils.text import limpiar_html

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
        print(f"INFO: status {response.status_code} → Fin.")
        return None

    data = response.json()
    descripcion_raw = data.get("data", {}).get("description", None)

    if descripcion_raw is None:
        return None

    descripcion = limpiar_html(descripcion_raw)

    return descripcion


# Función para actualizar el JSON con la información del vendedor
def actualizar_json_con_descripcion(archivo_json):
    # Leer el archivo JSON original
    with open(archivo_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Iterar sobre los productos y obtener la descripcion de cada producto
    for producto in data:
        producto_id = producto.get("product_id")
        if producto_id:
            descripcion = obtener_descripcion_producto(producto_id)
            producto["descripcion_producto"] = descripcion
            producto["fecha_extraccion_final"] = pendulum.now(
                "America/Lima"
            ).to_iso8601_string()

            if descripcion is None:
                print(
                    f"ERROR: El producto de sku '{producto['sku']}' no se encontró su descripcion"
                )
            print(f"INFO: Producto con sku '{producto['sku']}' actualizado")

    # Guardar los datos actualizados en el mismo archivo JSON
    new_json = "data/full_productos.json"
    with open(new_json, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\nINFO: El archivo {new_json} ha sido creado exitosamente")


if __name__ == "__main__":
    archivo_json = "data/productos.json"
    actualizar_json_con_descripcion(archivo_json)
