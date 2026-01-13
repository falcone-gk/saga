from typing import Union

import pytest

from scraper.utils.text import clean_html, get_weight_from_text

# --- Tests para clean_html ---


@pytest.mark.parametrize(
    "input_html, expected_output",
    [
        ("<p>Hola <b>Mundo</b></p>", "Hola Mundo"),
        ("&lt;div&gt;Texto escapado&lt;/div&gt;", "Texto escapado"),
        ("<script>alert('error')</script><p>Contenido</p>", "Contenido"),
        ("   ", None),
        ("<div>Línea 1</div>\n\n<div>Línea 2</div>", "Línea 1 Línea 2"),
    ],
)
def test_clean_html(input_html: str, expected_output: Union[str, None]):
    assert clean_html(input_html) == expected_output


# --- Tests para get_weight_from_text ---


@pytest.mark.parametrize(
    "input_text, expected_weight",
    [
        ("Arroz Costeño 5kg", "5 kg"),
        ("Harina 500gr", "500 g"),  # Normaliza 'gr' a 'g'
        ("Detergente 2.5 Kg", "2.5 kg"),
        ("Aceite 900 g", "900 g"),
        ("Pack de 12 latas", None),
        ("Queso 250,5 gr", "250.5 g"),  # Normaliza coma a punto
    ],
)
def test_get_weight_from_text(
    input_text: str, expected_weight: Union[str, None]
):
    assert get_weight_from_text(input_text) == expected_weight
