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


def fetch_product_description(product_id):
    url = PRODUCT_URL.format(product_id)
    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        return None
    return response.json().get("data", {}).get("description")
