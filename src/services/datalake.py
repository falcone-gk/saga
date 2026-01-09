import json
from typing import Any, Dict, List, Union

import pandas as pd
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq

from core.logging import get_logger
from core.schemas import HDFSConfig

logger = get_logger(__name__)


class DataLakeManager:
    """
    Clase para gestionar operaciones de lectura y escritura en Data Lake (HDFS).
    Refactorizada para usar PyArrow FileSystem API.
    """

    def __init__(self, config: HDFSConfig):
        """
        Inicializa la conexión con HDFS usando PyArrow.

        Nota: Requiere que las variables de entorno HADOOP_HOME o CLASSPATH
        estén configuradas correctamente en el sistema.
        """

        self.hdfs = fs.HadoopFileSystem(
            host=config.host,
            port=config.port,
            user=config.user,
        )

    def write_data(
        self,
        hdfs_path: str,
        data: Union[Dict[str, Any], List[Any], pd.DataFrame],
        fmt: str = "json",
        **kwargs: Any,
    ) -> None:
        """Escribe datos en HDFS usando la interfaz de PyArrow."""

        if fmt == "json":
            content = json.dumps(data, indent=4).encode("utf-8")
            with self.hdfs.open_output_stream(hdfs_path) as stream:
                stream.write(content)

        elif fmt == "csv":
            if not isinstance(data, pd.DataFrame):
                raise TypeError(
                    "Para CSV, 'data' debe ser un pandas DataFrame."
                )

            # Usamos un buffer para convertir el DF a CSV y luego escribir al stream
            with self.hdfs.open_output_stream(hdfs_path) as stream:
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
            with self.hdfs.open_output_stream(hdfs_path) as stream:
                pq.write_table(table, stream, **kwargs)

        else:
            raise ValueError(f"Formato '{fmt}' no soportado.")

        logger.info(f"Archivo guardado exitosamente en: {hdfs_path}")

    def read_data(
        self, hdfs_path: str, fmt: str = "json"
    ) -> Union[Dict[str, Any], List[Any], pd.DataFrame]:
        """Lee datos desde HDFS usando PyArrow FileSystem."""

        with self.hdfs.open_input_stream(hdfs_path) as stream:
            if fmt == "json":
                return json.loads(stream.readall().decode("utf-8"))

            elif fmt == "csv":
                # pandas puede leer directamente desde el stream de PyArrow
                return pd.read_csv(stream)

            elif fmt == "parquet":
                # Leemos la tabla y convertimos a DataFrame
                table = pq.read_table(stream)
                return table.to_pandas()

            else:
                raise ValueError(f"Formato '{fmt}' no soportado.")

    def list_files(self, hdfs_dir: str) -> List[str]:
        """Lista los archivos en un directorio de HDFS."""
        # FileSelector permite listar archivos de forma recursiva si se desea
        selector = fs.FileSelector(hdfs_dir, recursive=False)
        items = self.hdfs.get_file_info(selector)
        return [item.path for item in items]

    def delete_file(self, hdfs_path: str) -> None:
        """Borra un archivo o directorio en HDFS."""
        self.hdfs.delete_file(hdfs_path)
