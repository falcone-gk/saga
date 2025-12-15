from sqlalchemy import BigInteger, Column, DateTime, Numeric, String
from sqlalchemy.orm import declarative_base

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
