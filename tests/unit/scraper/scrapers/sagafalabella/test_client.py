from unittest.mock import MagicMock, patch

import requests

from scraper.scrapers.sagafalabella.client import (
    fetch_api_product_extra_details,
    fetch_html_product_extra_details,
    fetch_products_page,
)


@patch("requests.get")
def test_fetch_products_page_success(mock_get: MagicMock) -> None:
    # Configuramos el mock para devolver un JSON válido
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "data": {"results": [{"productId": "1"}, {"productId": "2"}]}
    }
    mock_get.return_value = mock_response

    results = fetch_products_page(
        page=1, category_id="cat123", category_name="alimentos"
    )

    assert results is not None
    assert len(results) == 2
    assert results[0]["productId"] == "1"
    mock_get.assert_called_once()


@patch("requests.get")
def test_fetch_products_page_server_error(mock_get: MagicMock) -> None:
    # Simulamos un error 500
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_get.return_value = mock_response

    results = fetch_products_page(1, "id", "name")
    assert results is None


# Este test valida una funcion que por ahora no se utiliza
# debido a que saga cambió el retorno de la API de producto.
@patch("requests.get")
def test_fetch_api_product_extra_details_exception(mock_get: MagicMock) -> None:
    # Simulamos una excepción de red (ej: Timeout)
    mock_get.side_effect = requests.RequestException("Timeout")

    description = fetch_api_product_extra_details("prod123")
    assert description is None


@patch("requests.get")
def test_fetch_html_product_extra_details_success(mock_get: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "<html><body>Test</body></html>"
    mock_get.return_value = mock_response

    html_content = fetch_html_product_extra_details("http://fake-url.com")

    assert html_content == "<html><body>Test</body></html>"
    assert isinstance(html_content, str)
