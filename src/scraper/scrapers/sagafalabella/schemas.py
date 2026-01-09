from typing import List, Literal, Optional, TypedDict

import pyarrow as pa
from pydantic import BaseModel


class ProductPrice(BaseModel):
    type: Literal[
        "normalPrice",
        "cmrPrice",
        "eventPrice",
        "internetPrice",
    ]
    crossed: bool = False
    price: List[str] = []


class RawProduct(BaseModel):
    """Estructura del producto tal como viene del JSON original"""

    displayName: str
    skuId: str
    productId: str
    brand: Optional[str] = None
    sellerName: Optional[str] = None
    url: str
    prices: List[ProductPrice] = []


class ScrapedProduct(BaseModel):
    """Estructura final del producto procesado"""

    categoria_animal: str
    categoria_producto: Optional[str]
    sub_categoria_producto: Optional[str] = None
    marca: Optional[str]
    nombre: str
    vendido_por: Optional[str]
    titulo_promocion: Optional[str] = None
    descripcion_promocion: Optional[str] = None
    descripcion_producto: Optional[str]
    peso_considerado: Optional[str]
    precio_sin_descuento: Optional[float]
    precio_publico: Optional[float]
    precio_cmr: Optional[float]
    fecha_extraccion_inicio: str
    fecha_extraccion_final: Optional[str]
    product_id: str
    sku: str
    url: str


# Esquema de pyarrow basado en ScrapedProduct
SCRAPED_PRODUCT_SCHEMA = pa.schema(
    [
        ("categoria_animal", pa.string()),
        ("categoria_producto", pa.string()),
        ("sub_categoria_producto", pa.string()),
        ("marca", pa.string()),
        ("nombre", pa.string()),
        ("vendido_por", pa.string()),
        ("titulo_promocion", pa.string()),
        ("descripcion_promocion", pa.string()),
        ("descripcion_producto", pa.string()),
        ("peso_considerado", pa.string()),
        ("precio_sin_descuento", pa.float64()),
        ("precio_publico", pa.float64()),
        ("precio_cmr", pa.float64()),
        ("fecha_extraccion_inicio", pa.string()),
        ("fecha_extraccion_final", pa.string()),
        ("product_id", pa.string()),
        ("sku", pa.string()),
        ("url", pa.string()),
    ]
)


class CategoryMetadata(TypedDict):
    animal: str
    category_label: str  # El nombre legible (ej: "Alimentos")
    category_url: str  # El slug para la URL (ej: "Alimento-para-perros")
