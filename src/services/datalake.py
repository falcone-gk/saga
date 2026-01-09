import io
import json
from typing import Any, Dict, List, Union

import pandas as pd
from hdfs import InsecureClient

from core.schemas import HDFSConfig


class DataLakeManager:
    """
    Clase para gestionar operaciones de lectura y escritura en Data Lake (HDFS).

    Soporta formatos JSON, CSV y Parquet utilizando buffers en memoria para
    evitar la creación de archivos temporales locales.
    """

    def __init__(self, config: HDFSConfig):
        """
        Inicializa la conexión con el NameNode.

        Args:
            url (str): URL del WebHDFS (ej. 'http://localhost:9870').
            user (str): Usuario con permisos de escritura en el clúster.
        """
        self.client: InsecureClient = InsecureClient(
            config.url, user=config.user
        )

    def write_data(
        self,
        hdfs_path: str,
        data: Union[Dict, List, pd.DataFrame],
        fmt: str = "json",
        **kwargs: Any,
    ) -> None:
        """
        Escribe datos en HDFS en el formato especificado.

        Args:
            hdfs_path (str): Ruta completa de destino en HDFS.
            data (Union[Dict, List, pd.DataFrame]): Datos a guardar. Dict/List para JSON,
                                                    DataFrame para CSV/Parquet.
            fmt (str): Formato del archivo ('json', 'csv', 'parquet'). Por defecto 'json'.
            **kwargs: Argumentos adicionales para pandas (ej. sep, compression, index).

        Example:
            >>> manager = HDFSManager('http://localhost:9870', 'hdfs')
            >>> df = pd.DataFrame({'a': [1], 'b': [2]})
            >>> manager.write_data('/user/data/test.parquet', df, fmt='parquet')
        """
        if fmt == "json":
            content = json.dumps(data, indent=4)
            self.client.write(
                hdfs_path, content, encoding="utf-8", overwrite=True
            )

        elif fmt == "csv":
            if not isinstance(data, pd.DataFrame):
                raise TypeError(
                    "Para CSV, 'data' debe ser un pandas DataFrame."
                )
            with self.client.write(
                hdfs_path, encoding="utf-8", overwrite=True
            ) as writer:
                data.to_csv(writer, index=kwargs.get("index", False), **kwargs)

        elif fmt == "parquet":
            if not isinstance(data, pd.DataFrame):
                raise TypeError(
                    "Para Parquet, 'data' debe ser un pandas DataFrame."
                )
            buffer = io.BytesIO()
            data.to_parquet(
                buffer,
                engine="pyarrow",
                index=kwargs.get("index", False),
                **kwargs,
            )
            self.client.write(hdfs_path, buffer.getvalue(), overwrite=True)

        else:
            raise ValueError(
                f"Formato '{fmt}' no soportado. Usa 'json', 'csv' o 'parquet'."
            )

        print(f"Archivo guardado en: {hdfs_path}")

    def read_data(
        self, hdfs_path: str, fmt: str = "json"
    ) -> Union[Dict, List, pd.DataFrame]:
        """
        Lee datos desde una ruta de HDFS y los transforma al objeto Python correspondiente.

        Args:
            hdfs_path (str): Ruta del archivo en HDFS.
            fmt (str): Formato del archivo original ('json', 'csv', 'parquet').

        Returns:
            Union[Dict, List, pd.DataFrame]: Datos cargados.

        Example:
            >>> manager = HDFSManager('http://localhost:9870', 'hdfs')
            >>> data = manager.read_data('/user/data/test.json', fmt='json')
            >>> print(type(data)) # <class 'dict'>
        """
        if fmt == "json":
            with self.client.read(hdfs_path, encoding="utf-8") as reader:
                return json.load(reader)

        elif fmt == "csv":
            with self.client.read(hdfs_path, encoding="utf-8") as reader:
                return pd.read_csv(reader)

        elif fmt == "parquet":
            with self.client.read(hdfs_path) as reader:
                buffer = io.BytesIO(reader.read())
                return pd.read_parquet(buffer)

        else:
            raise ValueError(f"Formato '{fmt}' no soportado.")

    def list_files(self, hdfs_dir: str) -> List[str]:
        """Lista los archivos en un directorio de HDFS."""
        return self.client.list(hdfs_dir)
