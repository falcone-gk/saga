import html
import io
import json
import os
import re
from datetime import datetime

import pandas as pd
import pendulum
import requests
from bs4 import BeautifulSoup
from hdfs import InsecureClient
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Numeric,
    String,
    create_engine,
)
from sqlalchemy.orm import declarative_base, sessionmaker

# conexion a bd

host_psql = "192.168.1.208"
base_ddatos = "biomont"
usuario_psql = os.environ.get("POSTGRES_USER")
clave_psql = os.environ.get("POSTGRES_PASSWORD")

db_url = "postgresql+psycopg2://{}:{}@{}:5432/{}".format(
    usuario_psql, clave_psql, host_psql, base_ddatos
)

engine = create_engine(db_url)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
)

# Modelo de bd
Base = declarative_base()


class WebScrappingSagaFalabella(Base):
    __tablename__ = "webscrapping_sagafalabella"

    id = Column(BigInteger, primary_key=True)

    categoria_animal = Column(String)
    categoria_producto = Column(String)
    sub_categoria_producto = Column(String)
    marca = Column(String)
    nombre = Column(String)
    vendido_por = Column(String)

    titulo_promocion = Column(String)
    descripcion_promocion = Column(String)
    descripcion_producto = Column(String)

    peso_considerado = Column(String)

    precio_sin_descuento = Column(Numeric)
    precio_publico = Column(Numeric)
    precio_cmr = Column(Numeric)

    fecha_extraccion_inicio = Column(DateTime)
    fecha_extraccion_final = Column(DateTime)

    sku = Column(String)
    product_id = Column(String)


STRUCTURE_DATA = {
    "perro": {
        "categorias": [
            {
                "Alimentos": {
                    "id": "CATG15475",
                    "category_name": "Alimento-para-perros",
                }
            },
            {
                "Platos": {
                    "id": "cat14110477",
                    "category_name": "Platos--dispensadores-y-botellas",
                }
            },
            {
                "Antiparasitarios": {
                    "id": "CATG15478",
                    "category_name": "Antiparasitarios",
                }
            },
            {
                "Camas": {
                    "id": "CATG15476",
                    "category_name": "Camas-y-colchones-para-perro",
                }
            },
            {
                "Casas": {
                    "id": "CATG15477",
                    "category_name": "Casas-para-perros",
                }
            },
            {
                "Jaulas y transporte": {
                    "id": "cat12500467",
                    "category_name": "Jaulas-y-transporte-para-perros",
                }
            },
            {
                "Arnes": {
                    "id": "cat14110481",
                    "category_name": "Arnes--Collares-y-Correas",
                }
            },
            {
                "Peluqueria canina": {
                    "id": "CATG15481",
                    "category_name": "Peluqueria-canina",
                }
            },
            {
                "Juguetes y entrenamiento": {
                    "id": "cat12500465",
                    "category_name": "Juguetes-y-entrenamiento-para-perro",
                }
            },
            {
                "Ropa y accesorios": {
                    "id": "cat12500466",
                    "category_name": "Ropa-y-accesorios-para-perros",
                }
            },
            # Es el mismo que Antiparasitarios
            # {
            #     "Higiene y salud canina": {
            #         "id": "CATG15478",
            #         "category_name": "Antiparasitarios",
            #     }
            # },
        ]
    },
    "gato": {
        "categorias": [
            {
                "Alimentos": {
                    "id": "CATG15470",
                    "category_name": "Alimento-para-Gatos",
                }
            },
            {
                "Platos": {
                    "id": "CATG33635",
                    "category_name": "Platos-para-gatos",
                }
            },
            {
                "Arena": {
                    "id": "CATG15472",
                    "category_name": "Arena",
                }
            },
            {
                "Areneros": {
                    "id": "CATG33754",
                    "category_name": "Areneros",
                }
            },
            {
                "Camas": {
                    "id": "CATG14641",
                    "category_name": "Camas-para-gatos",
                }
            },
            {
                "Rascadores": {
                    "id": "CATG33636",
                    "category_name": "Rascadores-para-gato",
                }
            },
            {
                "Juguetes": {
                    "id": "CATG14643",
                    "category_name": "Juguetes-y-entretencion-para-gatos",
                }
            },
            {
                "Arneses y collares": {
                    "id": "CATG14640",
                    "category_name": "Arneses-y-collares",
                }
            },
            {
                "Higiene y cuidado": {
                    "id": "CATG14642",
                    "category_name": "Higiene-y-cuidados-para-gatos",
                }
            },
        ]
    },
}


# API publica de saga que entrega una lista con paginacion de los productos
BASE_URL = (
    "https://www.falabella.com.pe/s/browse/v1/listing/pe"
    "?page={page}&categoryId={category_id}&categoryName={category_name}&pid=799c102f-9b4c-44be-a421-23e366a63b82"
)


# API publica de productos de saga utilizado solo para obtener la descripcion
PRODUCT_URL = "https://www.falabella.com.pe/s/browse/v3/product/pe?productId={}"


# HDFS paths
fecha_actual = datetime.now().strftime("%Y-%m-%d")
path_hdfs_raw = (
    "/biomont/raw/bi_webscraping_sagafalabella_raw_{}.parquet".format(
        fecha_actual
    )
)
path_hdfs_complete = (
    "/biomont/trusted/bi_webscraping_sagafalabella_complete_{}.parquet".format(
        fecha_actual
    )
)

############# Utilidades generales #############


def limpiar_html(texto: str) -> str | None:
    """
    Limpia HTML (normal o escapado) y devuelve solo texto legible.
    """
    if not texto or not isinstance(texto, str):
        return None

    # Desescapar HTML (&lt;div&gt; -> <div>)
    texto = html.unescape(texto)

    # Parsear HTML
    soup = BeautifulSoup(texto, "html.parser")

    # Eliminar tags basura
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    # Obtener texto con saltos lógicos
    texto_limpio = soup.get_text(separator="\n", strip=True)

    # Normalizar espacios
    texto_limpio = re.sub(r"[ \t]+", " ", texto_limpio)

    # Limpiar saltos de línea múltiples
    texto_limpio = re.sub(r"\n{2,}", "\n", texto_limpio)

    # Quitar líneas vacías
    texto_limpio = " ".join(
        line.strip() for line in texto_limpio.splitlines() if line.strip()
    )

    return texto_limpio or None


def extraer_peso(nombre):
    """
    Extrae valores como '7kg', '7 Kg', '7gr', '7 g' y devuelve '7 kg', '7 gr', etc.
    """
    patron = re.compile(r"(\d+(?:[\.,]\d+)?)\s*(kg|g|gr)", flags=re.IGNORECASE)

    m = patron.search(nombre)
    if not m:
        return None

    valor = m.group(1).replace(",", ".")  # Normaliza decimales
    unidad = m.group(2).lower()  # Unidad en minúsculas

    # Normalizar unidades equivalentes
    if unidad == "gr":
        unidad = "g"

    return f"{valor} {unidad}"


############# Utilidades para obtener informacion de productos #############

# Parsers


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

    print("Extrayendo datos del producto con sku: %s", sku)

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
    print("Extrayendo detalle del producto con sku: %s", sku)
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
            print(
                "No se pudo extraer la descripcón del sku %s de la url %s",
                sku,
                url,
            )

        # Extraccion de la categoria del producto
        a = soup.select_one("a.Breadcrumbs-module_selected-bread-crumb__ZPj02")
        url_category = a["href"]

        match = re.search(r"/category/([^/]+)", url_category)
        if match is None:
            print(
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
        print(
            "Error obteniendo detalle del producto con sku %s de la url %s: %s",
            sku,
            url,
            e,
        )
        return None, None


# Fetchs


def fetch_products_page(page, category_id, category_name):
    url = BASE_URL.format(
        page=page,
        category_id=category_id,
        category_name=category_name,
    )
    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        return None
    return response.json().get("data", {}).get("results", [])


def fetch_html_product_extra_details(product_url):
    """
    Se hace un request a la url de un producto para obtener su descripcion
    y categoria de producto.
    """
    response = requests.get(product_url, timeout=10)
    if response.status_code != 200:
        return None
    return response.text


# Saving data


def bulk_insert_falabella_from_parquet(parquet_path):
    session = SessionLocal()

    # Conectar con WebHDFS
    client = InsecureClient("http://192.168.1.206:9870", user="aramos")

    # Traer el archivo desde el HDFS para trabajarlo
    with client.read(path_hdfs_complete) as reader:
        parquet_bytes = io.BytesIO(reader.read())
        df = pd.read_parquet(parquet_bytes, engine="fastparquet")

    try:
        records = df.to_dict(orient="records")

        session.bulk_insert_mappings(WebScrappingSagaFalabella, records)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def guardar_parsed_temporal(parsed_lote):
    """
    Guarda un lote de productos en un archivo parquet temporal.
    Si el archivo existe, agrega (append).
    """
    if not parsed_lote:
        print("No hay datos en el dataframe")
        return

    parquet_buffer = io.BytesIO()
    df = pd.DataFrame(parsed_lote)
    df.to_parquet(
        parquet_buffer,
        engine="fastparquet",
        index=False,
    )

    # Regresar puntero al inicio
    parquet_buffer.seek(0)

    # Subir a HDFS
    client = InsecureClient("http://192.168.1.206:9870", user="aramos")
    client.write(path_hdfs_raw, data=parquet_buffer, overwrite=True)

    print("Archivo temporal de saga_falabella.parquet generado")


def save_parsed_updated(data):
    """
    Guarda un lote de productos en un archivo parquet temporal.
    Si el archivo existe, actualiza (overwrite).
    """
    if data is None or data.empty:
        print("No hay datos en el dataframe")
        return

    parquet_buffer = io.BytesIO()
    data.to_parquet(parquet_buffer, engine="fastparquet", index=False)

    # Regresar puntero al inicio
    parquet_buffer.seek(0)

    # Subir a HDFS
    client = InsecureClient("http://192.168.1.206:9870", user="aramos")
    client.write(path_hdfs_complete, data=parquet_buffer, overwrite=True)

    print("Archivo temporal de saga_falabella_updated.parquet actualizado")


# Jobs


def filtrar_productos_nuevos(products, skus_vistos):
    productos_nuevos = []
    for producto in products:
        sku = producto.get("skuId")
        if sku not in skus_vistos:
            skus_vistos.add(sku)
            productos_nuevos.append(producto)
    return productos_nuevos


# job 1
def scrape():
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

            print("Iniciando scraping de %s -> %s", animal, nombre_categoria)

            # Iteramos sobre los productos de la categoría
            counter_categoria = 0
            while True:
                print("Scrapeando pagina %s", page)
                productos = fetch_products_page(
                    page, categoria_id, category_name
                )

                if productos is None:
                    print("No hay mas productos para scrapear")
                    break

                # No consideramos productos repetidos dentro de una misma categoria
                productos_nuevos = filtrar_productos_nuevos(
                    productos, skus_vistos
                )

                parsed = [
                    extraer_producto(animal, p, category_name)
                    for p in productos_nuevos
                ]
                parsed_total.extend(parsed)

                print(
                    "Total de productos scrapeados de %s -> %s (pagina %s): %s",
                    animal,
                    nombre_categoria,
                    page,
                    len(parsed),
                )
                page += 1
                counter_categoria += len(parsed)

            # Insertamos los datos de la categoria
            print(
                "Total de productos scrapeados (%s -> %s): %s",
                animal,
                nombre_categoria,
                counter_categoria,
            )

    guardar_parsed_temporal(parsed_total)
    print("Total de productos procesados: %s", len(parsed_total))


# job 2
def combinar_categoria_animal(series):
    orden = ["perro", "gato"]
    valores = set(series.dropna().unique())

    resultado = [x for x in orden if x in valores]
    return "-".join(resultado)


def actualizar_categoria_y_descripcion(row):
    try:
        cat, desc = extraer_extra_detalle_producto(row["sku"], row["url"])
        return pd.Series([cat, desc])
    except Exception as e:
        print(
            f"Error extrayendo detalle del producto (SKU {row.get('sku')}): {e}"
        )
        return pd.Series([None, None])


# Main function
def update_product_data():
    # Conectar con WebHDFS
    client = InsecureClient("http://192.168.1.206:9870", user="aramos")
    # Traer el archivo desde el HDFS para trabajarlo
    with client.read(path_hdfs_raw) as reader:
        parquet_bytes = io.BytesIO(reader.read())
        df = pd.read_parquet(parquet_bytes, engine="fastparquet")

    # Combinar categoria_animal por SKU
    categoria_por_sku = df.groupby("sku")["categoria_animal"].apply(
        combinar_categoria_animal
    )

    df["categoria_animal"] = df["sku"].map(categoria_por_sku)
    df = df.drop_duplicates(subset="sku").reset_index(drop=True)

    # Extraer categoria y descripcion del producto
    df[["categoria_producto", "descripcion_producto"]] = df.apply(
        actualizar_categoria_y_descripcion, axis=1
    )

    # Guardando el archivo temporal actualizado
    save_parsed_updated(df)


# Job 3
def save_to_sql():
    # TODO: Tener la ruta del archivo temporal
    tmp_file = ""
    bulk_insert_falabella_from_parquet(tmp_file)
