import sys

import pandas as pd

from core.logging import get_logger
from scraper.scrapers.sagafalabella.parser import get_product_detail

logger = get_logger(__name__)


def merge_animal_name(series: pd.Series) -> str:
    orden: list[str] = ["perro", "gato"]
    valores: set[str] = set(series.dropna().unique())

    result: list[str] = [x for x in orden if x in valores]
    return "-".join(result)


def get_category_and_description(row: pd.Series) -> pd.Series:
    try:
        sku: str = row["sku"]
        url: str = row["url"]

        cat: str | None
        desc: str | None

        logger.info(f"Extrayendo detalle del producto con sku: {sku}")
        cat, desc = get_product_detail(sku, url)

        return pd.Series([cat, desc])
    except Exception as e:
        logger.error(
            f"Error extrayendo detalle del producto (SKU {row.get('sku')}): {e}"
        )
        return pd.Series([None, None])


# Main function
def update_product_data(data: pd.DataFrame) -> pd.DataFrame:
    # Combinar categoria_animal (perro-gato) por SKU presente en ambas categorias
    # de animal
    new_merged_name = data.groupby("sku")["categoria_animal"].apply(  # type: ignore[reportUnknownMemberType]
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
    from services.datalake import DataLakeManager

    try:
        logger.info("=== INICIANDO UPDATE DE PRODUCTOS SAGA FALABELLA ===")

        # Obtener el dataframe
        parquet_path = settings.TMP_DIR / "saga_falabella.parquet"
        datalake = DataLakeManager(connection_type="local")
        data = datalake.read_data(parquet_path, fmt="parquet")

        if not isinstance(data, pd.DataFrame):
            logger.error("No se pudo leer el archivo de datos")
            return

        updated_data = update_product_data(data)

        # Guardando el dataframe en un parquet
        tmp_file = settings.TMP_DIR / "saga_falabella_updated.parquet"
        datalake.write_data(tmp_file, updated_data, fmt="parquet")

        logger.info(
            "Archivo temporal de saga_falabella_updated.parquet actualizado"
        )

        logger.info("=== PROCESO FINALIZADO ===")

    except KeyboardInterrupt:
        logger.warning("Scrapeo interrumpido por el usuario")
        sys.exit(0)

    except Exception:
        # incluye stacktrace completo
        logger.exception("Error crítico durante la ejecución")


if __name__ == "__main__":
    main()
