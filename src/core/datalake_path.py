from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class Layer(Enum):
    """Capas lógicas del Datalake."""

    RAW = "raw"  # Datos persistidos tal cual vienen del origen (scraping)
    MASTER = "master"  # Datos limpios, transformados y optimizados


class Extension(Enum):
    """Extensiones de archivo soportadas y estandarizadas."""

    JSON = "json"
    PARQUET = "parquet"
    CSV = "csv"


@dataclass(frozen=True)
class DatalakePathBuilder:
    """
    Constructor de rutas estandarizadas para el ecosistema de datos de Biomont.

    Esta clase asegura que todos los archivos (ya sean de scraping, procesos ETL o
    reportes) sigan una estructura de carpetas y una nomenclatura de archivos coherente.

    La estructura generada sigue el patrón:
    `{root}/{layer}/{pais}/{area}/{dataset}/{uuaa}_{dataset}_{frecuencia}_{layer}_{fecha}.{extension}`

    Attributes:
        root (str): Directorio base del Datalake (ej. '/biomont').
    """

    root: str = "/biomont"

    def build(
        self,
        *,
        layer: Layer,
        pais: str,
        area: str,
        dataset: str,
        uuaa: str,
        frecuencia: str,
        fecha: str,
        extension: Extension,
    ) -> str:
        """
        Construye una ruta completa basada en los estándares de la plataforma.

        Args:
            layer (Layer): Capa de destino (RAW, MASTER).
            pais (str): Código del país o nombre (ej. 'pe' de peru).
            area (str): Área de negocio (ej. 'co' de comercial).
            dataset (str): Nombre del conjunto de datos (ej. 'webscraping_nombre_comercio').
            uuaa (str): Identificador de la aplicación o proceso (ej. 'atencion').
            frecuencia (str): Periodicidad del dato (ej. 'daily', 'hourly').
            fecha (str): Marca de tiempo del dato (ej. '20251224').
            extension (Extension): Formato del archivo.

        Returns:
            str: Ruta completa normalizada.

        Examples:
            >>> builder = DatalakePathBuilder()
            >>> builder.build(
            ...     layer=Layer.RAW, pais="peru", area="retail", dataset="productos",
            ...     uuaa="scraper", frecuencia="diario", fecha="20251224",
            ...     extension=Extension.JSON
            ... )
            '/biomont/raw/peru/retail/productos/scraper_productos_diario_raw_20251224.json'
        """
        # Nomenclatura del archivo: uuaa_dataset_frecuencia_capa_fecha.ext
        filename = f"{uuaa}_{dataset}_{frecuencia}_{layer.value}_{fecha}.{extension.value}"

        # Construcción jerárquica de carpetas
        full_path = (
            Path(self.root) / layer.value / pais / area / dataset / filename
        )

        return str(full_path)

    def raw(self, **kwargs) -> str:
        """
        Atajo para generar rutas en la capa RAW.
        Por defecto utiliza la extensión JSON.

        Args:
            **kwargs: Argumentos requeridos por el método `build` (excepto layer).

        Examples:
            >>> path = builder.raw(
            ...     pais="pe", area="comercial", dataset="ventas",
            ...     uuaa="atencion", frecuencia="mensual", fecha="202512"
            ... )
        """
        kwargs.setdefault("extension", Extension.JSON)
        return self.build(layer=Layer.RAW, **kwargs)

    def master(self, **kwargs) -> str:
        """
        Atajo para generar rutas en la capa MASTER.
        Por defecto utiliza la extensión PARQUET.

        Args:
            **kwargs: Argumentos requeridos por el método `build` (excepto layer).

        Examples:
            >>> path = builder.master(
            ...     pais="pe", area="finanzas", dataset="kpis",
            ...     uuaa="bi", frecuencia="anual", fecha="2025"
            ... )
        """
        kwargs.setdefault("extension", Extension.PARQUET)
        return self.build(layer=Layer.MASTER, **kwargs)
