import sys

import pandas as pd

from core.logging import get_logger
from core.settings import settings
from scraper.scrapers.sagafalabella.parser import get_product_detail
from scraper.scrapers.sagafalabella.repository import save_parsed_updated

logger = get_logger(__name__)


def merge_animal_name(series):
    orden = ["perro", "gato"]
    valores = set(series.dropna().unique())

    result = [x for x in orden if x in valores]
    return "-".join(result)


def get_category_and_description(row):
    try:
        cat, desc = get_product_detail(row["sku"], row["url"])
        return pd.Series([cat, desc])
    except Exception as e:
        logger.error(
            f"Error extrayendo detalle del producto (SKU {row.get('sku')}): {e}"
        )
        return pd.Series([None, None])


# Main function
def update_product_data():
    tmp_file = settings.TMP_DIR / "saga_falabella.parquet"
    df = pd.read_parquet(tmp_file, engine="fastparquet")

    # Combinar categoria_animal (perro-gato) por SKU presente en ambas categorias
    # de animal
    new_merged_name = df.groupby("sku")["categoria_animal"].apply(
        merge_animal_name
    )

    df["categoria_animal"] = df["sku"].map(new_merged_name)
    df = df.drop_duplicates(subset="sku").reset_index(drop=True)

    # Extraer categoria y descripcion del producto
    df[["categoria_producto", "descripcion_producto"]] = df.apply(
        get_category_and_description, axis=1
    )

    # Guardando el archivo temporal actualizado
    save_parsed_updated(df)


def main():
    """Punto de entrada para el script"""
    logger.info("=== INICIANDO UPDATE DE PRODUCTOS SAGA FALABELLA ===")

    try:
        update_product_data()
        logger.info("=== PROCESO FINALIZADO ===")

    except KeyboardInterrupt:
        logger.warning("Scrapeo interrumpido por el usuario")
        sys.exit(0)

    except Exception:
        # incluye stacktrace completo
        logger.exception("Error crítico durante la ejecución")


if __name__ == "__main__":
    main()
