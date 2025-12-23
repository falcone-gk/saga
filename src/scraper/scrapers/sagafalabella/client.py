import requests

from .constants import BASE_URL, PRODUCT_URL


def fetch_products_page(page, category_id, category_name):
    url = BASE_URL.format(
        page=page,
        category_id=category_id,
        category_name=category_name,
    )
    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        return None
    return response.json().get("data", {}).get("results", [])


# TODO: Verificar si aun se puede usar este endpoint ya que saga parece
# que ha cambiado el endpoint de la API
def fetch_api_product_extra_details(product_id):
    url = PRODUCT_URL.format(product_id)
    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        return None
    return response.json().get("data", {}).get("description")


def fetch_html_product_extra_details(product_url):
    """
    Se hace un request a la url de un producto para obtener su descripcion
    y categoria de producto.
    """
    response = requests.get(product_url, timeout=10)
    if response.status_code != 200:
        return None
    return response.text
