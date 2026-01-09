from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class Layer(Enum):
    RAW = "raw"
    MASTER = "master"


class Extension(Enum):
    JSON = "json"
    PARQUET = "parquet"
    CSV = "csv"


@dataclass(frozen=True)
class DatalakePathBuilder:
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
        # Nomenclatura del archivo
        filename = f"{uuaa}_{dataset}_{frecuencia}_{layer.value}_{fecha}.{extension.value}"

        # Construcción con Path para asegurar compatibilidad de sistema operativo
        full_path = (
            Path(self.root) / layer.value / pais / area / dataset / filename
        )

        return str(full_path)

    def raw(
        self,
        pais: str,
        area: str,
        dataset: str,
        uuaa: str,
        frecuencia: str,
        fecha: str,
        extension: Extension = Extension.JSON,
    ) -> str:
        """Atajo para capa RAW con tipado explícito."""
        return self.build(
            layer=Layer.RAW,
            pais=pais,
            area=area,
            dataset=dataset,
            uuaa=uuaa,
            frecuencia=frecuencia,
            fecha=fecha,
            extension=extension,
        )

    def master(
        self,
        pais: str,
        area: str,
        dataset: str,
        uuaa: str,
        frecuencia: str,
        fecha: str,
        extension: Extension = Extension.PARQUET,
    ) -> str:
        """Atajo para capa MASTER con tipado explícito."""
        return self.build(
            layer=Layer.MASTER,
            pais=pais,
            area=area,
            dataset=dataset,
            uuaa=uuaa,
            frecuencia=frecuencia,
            fecha=fecha,
            extension=extension,
        )
