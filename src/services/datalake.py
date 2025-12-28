import json
from io import BytesIO, StringIO
from pathlib import Path
from typing import Any, Union

from core.datalake_path import DatalakePathBuilder
from core.settings import settings


class DatalakeClient:
    """
    Cliente unificado para la gestión de archivos en el Datalake de Biomont.

    Abstrae el almacenamiento físico (Local vs HDFS) y centraliza la lógica de
    escritura y lectura. Utiliza `DatalakePathBuilder` para asegurar que todas
    las rutas sigan el estándar definido por la organización.

    Attributes:
        builder (DatalakePathBuilder): Generador de rutas estandarizadas.
        backend (str): Indica si el cliente opera en modo 'LOCAL' o 'HDFS'.
    """

    def __init__(self):
        """
        Inicializa el cliente detectando el entorno desde settings.ENV.
        Si el entorno es 'local' o 'dev', se utiliza almacenamiento en disco.
        """
        self.env = settings.ENV
        self.builder = DatalakePathBuilder()
        self._is_local = self.env in ("local", "dev")

        if self._is_local:
            self.backend = "LOCAL"
            self.base_path: Path = getattr(
                settings, "DATALAKE_LOCAL_DIR", settings.BASE_DIR / "data"
            )
        else:
            self.backend = "HDFS"
            from hdfs import InsecureClient

            self.client = InsecureClient(
                settings.HDFS_HOST, user=settings.HDFS_USER
            )

    def _resolve_path(self, path: str) -> Union[Path, str]:
        """
        Traduce una ruta lógica a una ruta física real.
        """
        clean_path = path.lstrip("/")
        return (
            self.base_path / clean_path if self._is_local else f"/{clean_path}"
        )

    def write(self, path: str, content: Any):
        """
        Escribe contenido en el Datalake detectando el formato automáticamente.

        Args:
            path (str): Ruta destino (generada preferiblemente por self.builder).
            content (Any): Datos a guardar. Soporta dict, list, str, bytes y BytesIO.

        Examples:
            >>> client = DatalakeClient()

            # Caso 1: Guardar JSON (Scraping Raw)
            >>> path = client.builder.raw(uuaa="atencion", dataset="compras", ...)
            >>> client.write(path, {"id": 123, "status": "ok"})

            # Caso 2: Guardar CSV (String plano)
            >>> csv_data = "id,name\\n1,Producto"
            >>> client.write("landing/peru/ventas/file.csv", csv_data)

            # Caso 3: Guardar Parquet (desde Pandas)
            >>> buffer = BytesIO()
            >>> df.to_parquet(buffer)
            >>> client.write("master/peru/bi/data.parquet", buffer)
        """
        resolved_path = self._resolve_path(path)

        # 1. Preparación de datos (Serialización)
        if isinstance(content, (dict, list)):
            prepared_data = json.dumps(content, ensure_ascii=False)
            mode = "text"
        elif isinstance(content, str):
            prepared_data = content
            mode = "text"
        elif isinstance(content, BytesIO):
            prepared_data = content.getvalue()
            mode = "bytes"
        elif isinstance(content, bytes):
            prepared_data = content
            mode = "bytes"
        else:
            prepared_data = str(content)
            mode = "text"

        # 2. Escritura según Backend
        if self._is_local:
            resolved_path.parent.mkdir(parents=True, exist_ok=True)
            if mode == "text":
                resolved_path.write_text(prepared_data, encoding="utf-8")
            else:
                resolved_path.write_bytes(prepared_data)
        else:
            data_to_send = (
                StringIO(prepared_data)
                if mode == "text"
                else BytesIO(prepared_data)
            )
            self.client.write(
                resolved_path,
                data_to_send,
                overwrite=True,
                encoding="utf-8" if mode == "text" else None,
            )

    def read(self, path: str, as_json: bool = False) -> Any:
        """
        Lee el contenido de un archivo del Datalake.

        Args:
            path (str): Ruta del archivo a leer.
            as_json (bool): Si es True, convierte el texto leído en un objeto Python (dict/list).

        Returns:
            Any: Contenido en formato str, bytes o dict/list.

        Examples:
            >>> client = DatalakeClient()

            # Leer un JSON como diccionario
            >>> data = client.read("raw/peru/compras.json", as_json=True)
            >>> print(data['status'])

            # Leer un archivo de texto plano
            >>> texto = client.read("landing/peru/logs.txt")
        """
        resolved_path = self._resolve_path(path)

        if self._is_local:
            # Si es parquet o extensión binaria, leemos bytes, si no texto
            if path.lower().endswith((".parquet", ".png", ".jpg", ".zip")):
                data = resolved_path.read_bytes()
            else:
                data = resolved_path.read_text(encoding="utf-8")
        else:
            with self.client.read(resolved_path) as reader:
                data = reader.read()

        return json.loads(data) if as_json else data
