from typing import Any, Dict, List, Optional

import requests

from .constants import BASE_URL, PRODUCT_URL


def fetch_products_page(
    page: int, category_id: str, category_name: str
) -> Optional[List[Dict[str, Any]]]:
    """
    Obtiene la lista de productos de una página y categoría específica.
    Retorna None si hay un error de red o List[dict] si es exitoso.
    """
    url: str = BASE_URL.format(
        page=page,
        category_id=category_id,
        category_name=category_name,
    )
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return None

        # Usamos .get() encadenado con seguridad
        data: Dict[str, Any] = response.json().get("data", {})
        results: List[Dict[str, Any]] = data.get("results", [])
        return results
    except (requests.RequestException, ValueError):
        return None


def fetch_api_product_extra_details(product_id: str) -> Optional[str]:
    """
    Intenta obtener la descripción vía API.
    """
    url: str = PRODUCT_URL.format(product_id)
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return None

        # Casting explícito a str ya que esperamos la descripción
        description: Optional[str] = (
            response.json().get("data", {}).get("description")
        )
        return description
    except (requests.RequestException, ValueError):
        return None


def fetch_html_product_extra_details(product_url: str) -> Optional[str]:
    """
    Se hace un request a la url de un producto para obtener su HTML completo.
    """
    try:
        response = requests.get(product_url, timeout=10)
        if response.status_code != 200:
            return None
        return response.text
    except requests.RequestException:
        return None
