from typing import Dict

from scraper.scrapers.sagafalabella.schemas import CategoryMetadata

# Mapa de búsqueda rápida por ID
CATEGORY_LOOKUP: Dict[str, CategoryMetadata] = {
    "CATG15475": {
        "animal": "perro",
        "category_label": "Alimentos",
        "category_url": "Alimento-para-perros",
    },
    "CAT14110477": {
        "animal": "perro",
        "category_label": "Platos",
        "category_url": "Platos--dispensadores-y-botellas",
    },
    "CATG15478": {
        "animal": "perro",
        "category_label": "Antiparasitarios",
        "category_url": "Antiparasitarios",
    },
    "CATG15476": {
        "animal": "perro",
        "category_label": "Camas",
        "category_url": "Camas-y-colchones-para-perro",
    },
    "CATG15477": {
        "animal": "perro",
        "category_label": "Casas",
        "category_url": "Casas-para-perros",
    },
    "CAT12500467": {
        "animal": "perro",
        "category_label": "Jaulas y transporte",
        "category_url": "Jaulas-y-transporte-para-perros",
    },
    "CAT14110481": {
        "animal": "perro",
        "category_label": "Arnes",
        "category_url": "Arnes--Collares-y-Correas",
    },
    "CATG15481": {
        "animal": "perro",
        "category_label": "Peluqueria canina",
        "category_url": "Peluqueria-canina",
    },
    "CAT12500465": {
        "animal": "perro",
        "category_label": "Juguetes y entrenamiento",
        "category_url": "Juguetes-y-entrenamiento-para-perro",
    },
    "CAT12500466": {
        "animal": "perro",
        "category_label": "Ropa y accesorios",
        "category_url": "Ropa-y-accesorios-para-perros",
    },
    "CATG15470": {
        "animal": "gato",
        "category_label": "Alimentos",
        "category_url": "Alimento-para-Gatos",
    },
    "CATG33635": {
        "animal": "gato",
        "category_label": "Platos",
        "category_url": "Platos-para-gatos",
    },
    "CATG15472": {
        "animal": "gato",
        "category_label": "Arena",
        "category_url": "Arena",
    },
    "CATG33754": {
        "animal": "gato",
        "category_label": "Areneros",
        "category_url": "Areneros",
    },
    "CATG14641": {
        "animal": "gato",
        "category_label": "Camas",
        "category_url": "Camas-para-gatos",
    },
    "CATG33636": {
        "animal": "gato",
        "category_label": "Rascadores",
        "category_url": "Rascadores-para-gato",
    },
    "CATG14643": {
        "animal": "gato",
        "category_label": "Juguetes",
        "category_url": "Juguetes-y-entretencion-para-gatos",
    },
    "CATG14640": {
        "animal": "gato",
        "category_label": "Arneses y collares",
        "category_url": "Arneses-y-collares",
    },
    "CATG14642": {
        "animal": "gato",
        "category_label": "Higiene y cuidado",
        "category_url": "Higiene-y-cuidados-para-gatos",
    },
}


# API publica de saga que entrega una lista con paginacion de los productos
BASE_URL = (
    "https://www.falabella.com.pe/s/browse/v1/listing/pe"
    "?page={page}&categoryId={category_id}&categoryName={category_name}&pid=799c102f-9b4c-44be-a421-23e366a63b82"
)


# API publica de productos de saga utilizado solo para obtener la descripcion
PRODUCT_URL = "https://www.falabella.com.pe/s/browse/v3/product/pe?productId={}"
