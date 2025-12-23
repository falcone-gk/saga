import sys

import pandas as pd

from scraper.core.logging import get_logger
from scraper.core.settings import settings
from scraper.scrapers.sagafalabella.parser import extraer_extra_detalle_producto
from scraper.scrapers.sagafalabella.repository import save_parsed_updated

logger = get_logger(__name__)


def combinar_categoria_animal(series: pd.Series) -> str:
    valores = sorted(series.unique())
    if len(valores) > 1:
        return "-".join(valores)
    return valores[0]


def actualizar_categoria_y_descripcion(row: pd.Series) -> pd.Series:
    try:
        cat, desc = extraer_extra_detalle_producto(row["sku"], row["url"])
        return pd.Series([cat, desc])
    except Exception as e:
        print(
            f"Error extrayendo detalle del producto (SKU {row.get('sku')}): {e}"
        )
        return pd.Series([None, None])


def update_product_data():
    tmp_file = settings.TMP_DIR / "saga_falabella.parquet"
    df = pd.read_parquet(tmp_file, engine="fastparquet")

    # Combinar categoria_animal por SKU
    categoria_por_sku = df.groupby("sku")["categoria_animal"].apply(
        lambda x: "-".join(sorted(x.unique()))
    )

    df["categoria_animal"] = df["sku"].map(categoria_por_sku)
    df = df.drop_duplicates(subset="sku").reset_index(drop=True)

    # Extraer categoria y descripcion del producto
    df[["categoria_producto", "descripcion_producto"]] = df.apply(
        actualizar_categoria_y_descripcion, axis=1
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
