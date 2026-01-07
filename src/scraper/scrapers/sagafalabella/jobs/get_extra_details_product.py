import sys

import pandas as pd

from core.logging import get_logger
from scraper.scrapers.sagafalabella.parser import get_product_detail

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
def update_product_data(data):
    # Combinar categoria_animal (perro-gato) por SKU presente en ambas categorias
    # de animal
    new_merged_name = data.groupby("sku")["categoria_animal"].apply(
        merge_animal_name
    )

    data["categoria_animal"] = data["sku"].map(new_merged_name)
    updated_data = data.drop_duplicates(subset="sku").reset_index(drop=True)

    # Extraer categoria y descripcion del producto
    updated_data[["categoria_producto", "descripcion_producto"]] = (
        updated_data.apply(get_category_and_description, axis=1)
    )

    return updated_data


def main():
    from core.settings import settings

    # Obtener el dataframe
    parquet_path = settings.TMP_DIR / "saga_falabella.parquet"
    data = pd.read_parquet(parquet_path, engine="fastparquet")

    logger.info("=== INICIANDO UPDATE DE PRODUCTOS SAGA FALABELLA ===")
    updated_data = update_product_data(data)

    # Guardando el dataframe en un parquet
    tmp_file = settings.TMP_DIR / "saga_falabella_updated.parquet"
    updated_data.to_parquet(tmp_file, engine="fastparquet", index=False)

    logger.info(
        "Archivo temporal de saga_falabella_updated.parquet actualizado"
    )

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
