from typing import Any
from unittest.mock import MagicMock, patch

from scraper.scrapers.sagafalabella.parser import (
    get_prices,
    get_product_detail,
)
from scraper.scrapers.sagafalabella.schemas import RawProduct


def test_get_prices_with_all_types() -> None:
    raw_data: dict[str, Any] = {
        "productId": "123",
        "skuId": "456",
        "displayName": "Producto Test",
        "brand": "Marca",
        "sellerName": "Falabella",
        "url": "http://...",
        "prices": [
            {"type": "normalPrice", "crossed": True, "price": ["100.00"]},
            {"type": "internetPrice", "crossed": False, "price": ["80.00"]},
            {"type": "cmrPrice", "crossed": False, "price": ["70.00"]},
        ],
    }

    product: RawProduct = RawProduct(**raw_data)

    normal: float | None
    internet: float | None
    cmr: float | None
    normal, internet, cmr = get_prices(product)

    assert normal == 100.0
    assert internet == 80.0
    assert cmr == 70.0


@patch("scraper.scrapers.sagafalabella.parser.fetch_html_product_extra_details")
def test_get_product_detail_success(mock_fetch: MagicMock) -> None:
    # Simulamos el HTML con la estructura de breadcrumbs requerida
    mock_html: str = """
    <html>
        <body>
            <ol class="Breadcrumbs-module_breadcrumb__b47ha">
                <li><a href="#">Home</a></li>
                <li><a href="#">Mascotas - Perros</a></li>
                <li><a href="#">Higiene y cuidados para perros</a></li>
                <li><a href="#">Antiparasitarios</a></li>
            </ol>
            <script id="__NEXT_DATA__">
            {
                "props": {
                    "pageProps": {
                        "productData": {
                            "description": "Una descripción de prueba"
                        }
                    }
                }
            }
            </script>
        </body>
    </html>
    """
    mock_fetch.return_value = mock_html

    # Ahora desempaquetamos 3 valores
    category, sub_category, description = get_product_detail(
        "sku_test", "url_test"
    )

    # Verificaciones
    assert category == "Higiene y cuidados para perros"
    assert sub_category == "Antiparasitarios"
    assert description == "Una descripción de prueba"
    mock_fetch.assert_called_once_with("url_test")


def test_get_product_detail_fail_gracefully() -> None:
    with patch(
        "scraper.scrapers.sagafalabella.parser.fetch_html_product_extra_details"
    ) as mock_fetch:
        mock_fetch.return_value = None

        # Desempaquetamos 3 valores incluso en el caso de error
        category, sub_category, description = get_product_detail(
            "sku_test", "url_test"
        )

        assert category is None
        assert sub_category is None
        assert description is None
