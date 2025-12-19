# %%
import sys
from pathlib import Path

import pandas as pd

ROOT = Path.cwd().parent
sys.path.insert(0, str(ROOT))

from scraper.database.engine import engine

pd.set_option("display.html.table_schema", True)
# %%
df = pd.read_sql("SELECT * FROM webscrapping_sagafalabella2", con=engine)
df.set_index("id", inplace=True)
df.head()

# %%
columns = [
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

# total de filas involucradas en duplicados
duplicados = df[df.duplicated(subset="sku", keep=False)].sort_values("sku")[
    columns
]
total_filas_duplicadas = len(duplicados)

# cantidad de sku que se repiten (SKU únicos duplicados)
sku_repetidos = df["sku"].value_counts()
cantidad_sku_repetidos = (sku_repetidos > 1).sum()

print(f"Total de filas involucradas en duplicados: {total_filas_duplicadas}")
print(f"Cantidad de SKU repetidos: {cantidad_sku_repetidos}")

# %%
cond_mismo_animal_distinto_categoria = duplicados.groupby("sku").filter(
    lambda x: x["categoria_animal"].nunique() == 1
    and x["categoria_producto"].nunique() > 1
)

cantidad_mismo_animal_distinto_producto = cond_mismo_animal_distinto_categoria[
    "sku"
].nunique()

cond_diferente_animal = duplicados.groupby("sku").filter(
    lambda x: x["categoria_animal"].nunique() > 1
)

cantidad_diferente_animal = cond_diferente_animal["sku"].nunique()

cantidad_sku_repetidos_mas_dos = (sku_repetidos > 2).sum()

print(
    f"Cantidad de SKU con mismo animal distinta categoria producto: {cantidad_mismo_animal_distinto_producto}"
)
print(f"Cantidad de SKU con diferente animal: {cantidad_diferente_animal}")
print(f"Cantidad de SKU repetidos mas de dos: {cantidad_sku_repetidos_mas_dos}")

# %%
cantidad_sku_repetidos_menos_tres = (
    (sku_repetidos < 3) & (sku_repetidos > 1)
).sum()
print(
    f"Cantidad de SKU repetidos menos de tres: {cantidad_sku_repetidos_menos_tres}"
)
# %%
cond_mismo_animal_distinto_categoria.head(6)
# %%
cond_diferente_animal.tail(6)

# %%
sku_mas_de_2 = df["sku"].value_counts().loc[lambda x: x > 2].index

df_sku_mas_de_2 = df[df["sku"].isin(sku_mas_de_2)][columns].sort_values("sku")
df_sku_mas_de_2
