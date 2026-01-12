import html
import re

from bs4 import BeautifulSoup


def limpiar_html(texto: str) -> str | None:
    """
    Limpia HTML (normal o escapado) y devuelve solo texto legible.
    """

    # Desescapar HTML (&lt;div&gt; -> <div>)
    texto = html.unescape(texto)

    # Parsear HTML
    soup = BeautifulSoup(texto, "html.parser")

    # Eliminar tags basura
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    # Obtener texto con saltos lógicos
    texto_limpio = soup.get_text(separator="\n", strip=True)

    # Normalizar espacios
    texto_limpio = re.sub(r"[ \t]+", " ", texto_limpio)

    # Limpiar saltos de línea múltiples
    texto_limpio = re.sub(r"\n{2,}", "\n", texto_limpio)

    # Quitar líneas vacías
    texto_limpio = " ".join(
        line.strip() for line in texto_limpio.splitlines() if line.strip()
    )

    return texto_limpio or None


def get_weight_from_text(name: str) -> str | None:
    """
    Extrae valores como '7kg', '7 Kg', '7gr', '7 g' y devuelve '7 kg', '7 gr', etc.
    """
    patron = re.compile(r"(\d+(?:[\.,]\d+)?)\s*(kg|g|gr)", flags=re.IGNORECASE)

    m = patron.search(name)
    if not m:
        return None

    valor = m.group(1).replace(",", ".")  # Normaliza decimales
    unidad = m.group(2).lower()  # Unidad en minúsculas

    # Normalizar unidades equivalentes
    if unidad == "gr":
        unidad = "g"

    return f"{valor} {unidad}"
