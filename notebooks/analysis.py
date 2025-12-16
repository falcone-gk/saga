# %% Cell 0
import sys
from pathlib import Path

import pandas as pd

from db.engine import engine

ROOT = Path.cwd().parent  # notebooks/ → root
sys.path.insert(0, str(ROOT))

pd.set_option("display.html.table_schema", True)
# %% Cell 1
df = pd.read_sql("SELECT * FROM webscrapping_sagafalabella", con=engine)
df.set_index("id", inplace=True)
df.head()
# %% Cell 2
df["sku"].duplicated().any()
# %% Cell 3
df["sku"].duplicated().sum()
# %% Cell 4
df_duplicados = df[df["sku"].duplicated(keep=False)]
# df_duplicados[["sku", "categoria_animal", "categoria_producto"]].sort_values(
#     "sku"
# )
# df_duplicados.groupby("sku").agg(
#     {"categoria_animal": "nunique", "categoria_producto": "nunique"}
# ).reset_index()
df_duplicados.tail()


# %% Cell 5
df_gato_higiene = df[
    (df["categoria_animal"] == "gato")
    & (df["categoria_producto"] == "Higiene y cuidado")
]

df_gato_higiene_duplicados = df_gato_higiene[
    df_gato_higiene["sku"].duplicated(keep=False)
].sort_values("sku")
# %% Cell 5
df_gato_higiene_duplicados.head()
# %% Cell 5
conteo_categoria = (
    df_duplicados.groupby("categoria_producto")
    .size()
    .reset_index(name="cantidad")
)

conteo_categoria

# %% Cell 5

df_duplicados[df_duplicados["categoria_producto"] == "Casas"].sort_values("sku")

# %% Cell 5
df[df["sku"] == "137918607"]
# %% Cell 5
columnas = [
    "sku",
    "categoria_animal",
    "categoria_producto",
    # "nombre",
    # "marca",
    # "precio_publico",
    # "precio_sin_descuento",
    # "product_id",
]
df[columnas].duplicated(keep=False).sum()

# %% Cell 5
df_duplicados.info()
# %% Cell 5
# %% Cell 5
# %% Cell 5
# %% Cell 5
# %% Cell 5
# %% Cell 5
# %% Cell 5
