import pytest

from scraper.scrapers.sagafalabella.client import (
    fetch_products_page,
)


@pytest.mark.integration
def test_fetch_products_page_real_request() -> None:
    """
    Valida que la API de Saga Falabella siga respondiendo con la estructura esperada.
    """
    # Usamos datos reales conocidos (ejemplo: categoria de Mascotas/Alimentos)
    # Dado que es el ID de una categoria es posible que esto sea estable y no varie
    page = 1
    category_id = "CATG15475"
    category_name = "Alimentos"

    results = fetch_products_page(page, category_id, category_name)

    # Verificamos que no sea None y que traiga contenido
    assert results is not None, "La API no respondió o el formato cambió"
    assert isinstance(results, list)
    assert len(results) > 0, "La API retorno una lista vacía"

    # Validamos que los diccionarios internos tengan las llaves que necesitamos
    first_product = results[0]
    assert "skuId" in first_product
    assert "productId" in first_product
    assert "url" in first_product
    assert "prices" in first_product
