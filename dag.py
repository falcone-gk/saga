import html
import os
import re

import pendulum
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Numeric,
    String,
    create_engine,
)
from sqlalchemy.orm import declarative_base, sessionmaker

# Configuracion de la Base de Datos

host_psql = "192.168.1.208"
base_ddatos = "biomont"
usuario_psql = os.environ.get("POSTGRES_USER")
clave_psql = os.environ.get("POSTGRES_PASSWORD")

DATABASE_URL = "postgresql+psycopg2://{}:{}@{}:5432/{}".format(
    usuario_psql, clave_psql, host_psql, base_ddatos
)

engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    echo=False,
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
)

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


# Setup para el scrapeo

# Utilidades


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


# API publica de saga que entrega una lista con paginacion de los productos
BASE_URL = (
    "https://www.falabella.com.pe/s/browse/v1/listing/pe"
    "?page={page}&categoryId={category_id}&categoryName={category_name}&pid=799c102f-9b4c-44be-a421-23e366a63b82"
)


# API publica de productos de saga utilizado solo para obtener la descripcion
PRODUCT_URL = "https://www.falabella.com.pe/s/browse/v3/product/pe?productId={}"

STRUCTURE_DATA = {
    "perros": {
        "categorias": [
            {
                "Alimentos": {
                    "id": "CATG15475",
                    "category_name": "Comida-para-perros",
                }
            },
            # Higiene y Antipulgas tiene la misma categoria (id y nombre)
            {
                "Antiparasitarios": {
                    "id": "CATG15478",
                    "category_name": "Antiparasitarios",
                }
            },
        ]
    },
    "gatos": {
        "categorias": [
            {
                "Alimentos": {
                    "id": "CATG15470",
                    "category_name": "Alimento-para-Gatos",
                }
            },
            {
                "Antiparasitarios": {
                    "id": "CATG14642",
                    "category_name": "Higiene-y-cuidado-para-gatos",
                }
            },
        ]
    },
}


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


def scrape_all():
    all_products = []

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
            # while True:
            for _ in range(1):
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
                    descripcion = obtener_descripcion_producto(product_id)
                    result["descripcion_producto"] = descripcion

                    result["fecha_extraccion_final"] = pendulum.now(
                        "America/Lima"
                    ).to_iso8601_string()

                    print(
                        "Extrayendo de datos del producto con sku:",
                        sku,
                        "finalizada correctamente",
                    )

                    all_products.append(result)

                page += 1

    return all_products


def insert_webscrapping(db_session, data: list[dict]):
    db_session.bulk_insert_mappings(
        WebScrappingSagaFalabella,
        data,
    )


def main():
    print("Iniciando scraping...\n")
    data = scrape_all()
    db = SessionLocal()

    try:
        insert_webscrapping(db, data)
        db.commit()
    finally:
        db.close()

    print(f"\nSe realizó el scraping de {len(data)} productos.")


default_args = {
    "owner": "airflow",
    "email": ["BI-TI@biomont.com.pe"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="scrape_saga",
    default_args=default_args,
    description="Scrapeo de la pagina de Saga Falabella",
    start_date=pendulum.datetime(
        2025, 9, 17, 8, 0, 0, tz=pendulum.timezone("America/Lima")
    ),
    schedule_interval="0 * * * *",
    catchup=False,
) as dag:
    task_extract = PythonOperator(
        task_id="scrape_sagafalabella", python_callable=main
    )

    (task_extract)
