# %% Cell 0
import sys
from pathlib import Path

import pandas as pd

ROOT = Path.cwd().parent
sys.path.insert(0, str(ROOT))

from scraper.database.engine import engine

pd.set_option("display.html.table_schema", True)

# %% Cell 1
df = pd.read_sql("SELECT * FROM webscrapping_sagafalabella2", con=engine)
df.set_index("id", inplace=True)
df.head()
# %% Cell 3
# Cantidad de filas involucradas con duplicados
df["sku"].duplicated(keep=False).sum()

# %% Cell
# cantidad de sku que tienen duplicados
df["sku"].value_counts().gt(1).sum()

# %% Cell 4
df_duplicados = df[df["sku"].duplicated(keep=False)]
df_duplicados.sort_values("sku").head()

# %% Cell
# Duplicados por categoria de producto y animal
conteo_categoria = (
    df_duplicados.groupby(["categoria_animal", "categoria_producto"])
    .size()
    .reset_index(name="cantidad")
)

# Añadir la fila de total
conteo_categoria.loc[len(conteo_categoria)] = [
    "Total General",
    "-",
    conteo_categoria["cantidad"].sum(),
]

conteo_categoria

# %% Cell 5
columnas = [
    "sku",
    # "categoria_animal",
    # "categoria_producto",
    "nombre",
    "marca",
    "precio_publico",
    "precio_sin_descuento",
    "precio_cmr",
    "product_id",
]
df[columnas].duplicated(keep=False).sum()

# %% Cell 5
sku_en_distintas_categorias = (
    df.groupby("sku")["categoria_producto"]
    .nunique()
    .reset_index(name="n_categorias")
    .query("n_categorias > 1")
)

# Cuantos sku en distintas categorias de productos?
sku_en_distintas_categorias.shape[0]
# %% Cell
# df con los sku duplicados que tienen distintas categorias de productos
df_sku_distintas_categorias = df[
    df["sku"].isin(sku_en_distintas_categorias["sku"])
].sort_values(["sku", "categoria_producto"])

df_sku_distintas_categorias[
    [
        "sku",
        "product_id",
        "nombre",
        "vendido_por",
        "categoria_animal",
        "categoria_producto",
        "precio_publico",
        "precio_sin_descuento",
        "precio_cmr",
    ]
].head(20)

# %% Cell
df_sku_distintas_categorias = df.groupby(["sku", "categoria_animal"]).filter(
    lambda x: x["categoria_producto"].nunique() > 1
)

# Ordenamos para facilitar la revisión visual
df_sku_distintas_categorias = df_sku_distintas_categorias.sort_values(
    ["sku", "categoria_animal", "categoria_producto"]
)

# Mostramos las columnas de interés
resultado = df_sku_distintas_categorias[
    [
        "sku",
        "product_id",
        "nombre",
        "vendido_por",
        "categoria_animal",
        "categoria_producto",
        "precio_publico",
        "precio_sin_descuento",
        "precio_cmr",
    ]
]

resultado.head(20)
# %% Cell
resultado.shape
# %% Cell
# cantidad de sku que tienen tres duplicados
df["sku"].value_counts().gt(2).sum()
# %% Cell
