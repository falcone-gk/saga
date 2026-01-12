import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq
from typing_extensions import Literal

from core.logging import get_logger
from core.schemas import HDFSConfig

logger = get_logger(__name__)

type Storage = Literal["local", "hdfs"]


class DataLakeManager:
    """
    Clase para gestionar operaciones de lectura y escritura en Data Lake (HDFS).
    Refactorizada para usar PyArrow FileSystem API.
    """

    def __init__(
        self,
        connection_type: Storage = "local",
        config: Optional[HDFSConfig] = None,
    ):
        """
        Inicializa el sistema de archivos.

        :param connection_type: 'local' o 'hdfs'. USAR 'local' PARA TESTING O LOCAL.
        :param config: Objeto HDFSConfig si se usa hdfs, de lo contrario None.
        """
        if connection_type == "hdfs":
            if not config:
                raise ValueError("Se requiere 'config' para conexiones HDFS.")
            self.filesystem = fs.HadoopFileSystem(
                host=config.host,
                port=config.port,
                user=config.user,
            )
        else:
            self.filesystem = fs.LocalFileSystem()

    def write_data(
        self,
        path: Union[str, Path],
        data: Union[Dict[str, Any], List[Any], pd.DataFrame],
        fmt: str = "json",
        **kwargs: Any,
    ) -> None:
        """Escribe datos en HDFS usando la interfaz de PyArrow."""

        path_as_str = str(path)

        if fmt == "json":
            content = json.dumps(data, indent=4).encode("utf-8")
            with self.filesystem.open_output_stream(path_as_str) as stream:
                stream.write(content)

        elif fmt == "csv":
            if not isinstance(data, pd.DataFrame):
                raise TypeError(
                    "Para CSV, 'data' debe ser un pandas DataFrame."
                )

            # Usamos un buffer para convertir el DF a CSV y luego escribir al stream
            with self.filesystem.open_output_stream(path_as_str) as stream:
                data.to_csv(
                    stream,
                    index=kwargs.get("index", False),
                    encoding="utf-8",
                    **kwargs,
                )

        elif fmt == "parquet":
            if not isinstance(data, pd.DataFrame):
                raise TypeError(
                    "Para Parquet, 'data' debe ser un pandas DataFrame."
                )

            # PyArrow escribe directamente DataFrames de Pandas a HDFS de forma nativa
            table = pa.Table.from_pandas(data)
            with self.filesystem.open_output_stream(path_as_str) as stream:
                pq.write_table(table, stream, **kwargs)

        else:
            raise ValueError(f"Formato '{fmt}' no soportado.")

        logger.info(f"Archivo guardado exitosamente en: {path}")

    def read_data(
        self, path: Union[str, Path], fmt: str = "json"
    ) -> Union[Dict[str, Any], List[Any], pd.DataFrame]:
        """Lee datos desde HDFS usando PyArrow FileSystem."""

        path_as_str = str(path)

        if fmt == "parquet":
            # Pasamos el filesystem y la ruta; pq.read_table se encarga del resto
            table = pq.read_table(path_as_str, filesystem=self.filesystem)
            return table.to_pandas()

        with self.filesystem.open_input_stream(path_as_str) as stream:
            if fmt == "json":
                return json.loads(stream.readall().decode("utf-8"))

            elif fmt == "csv":
                # pandas puede leer directamente desde el stream de PyArrow
                return pd.read_csv(stream)

            else:
                raise ValueError(f"Formato '{fmt}' no soportado.")

    def list_files(self, hdfs_dir: str) -> List[str]:
        """Lista los archivos en un directorio de HDFS."""
        # FileSelector permite listar archivos de forma recursiva si se desea
        selector = fs.FileSelector(hdfs_dir, recursive=False)
        items = self.filesystem.get_file_info(selector)
        return [item.path for item in items]

    def delete_file(self, path: str) -> None:
        """Borra un archivo o directorio en HDFS."""
        self.filesystem.delete_file(path)
