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
    mock_html: str = """
    <html>
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
        <a class="Breadcrumbs-module_selected-bread-crumb__ZPj02" href="/category/cat123">Link</a>
    </html>
    """
    mock_fetch.return_value = mock_html

    with patch(
        "scraper.scrapers.sagafalabella.parser.get_category_name_by_id"
    ) as mock_cat:
        mock_cat: MagicMock
        mock_cat.return_value = "Alimentos"

        category: str | None
        description: str | None
        category, description = get_product_detail("sku_test", "url_test")

        assert category == "Alimentos"
        assert description == "Una descripción de prueba"
        mock_fetch.assert_called_once_with("url_test")


def test_get_product_detail_fail_gracefully() -> None:
    with patch(
        "scraper.scrapers.sagafalabella.parser.fetch_html_product_extra_details"
    ) as mock_fetch:
        mock_fetch: MagicMock
        mock_fetch.return_value = None

        category: str | None
        description: str | None
        category, description = get_product_detail("sku_test", "url_test")

        assert category is None
        assert description is None
