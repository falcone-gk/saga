from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from core.logging import get_logger
from core.schemas import PostgresConfig

logger = get_logger(__name__)


class PostgresManager:
    """
    Gestiona la conexión y persistencia de datos en PostgreSQL.

    Optimizado para recibir DataFrames de Pandas provenientes del
    procesamiento de scraping o HDFS.
    """

    def __init__(self, config: PostgresConfig):
        """
        Inicializa el motor de base de datos.

        Args:
            config (PostgresConfig): Diccionario con las credenciales:
                                     host, port, user, password, database.
        """
        self.connection_string = (
            f"postgresql://{config.user}:{config.password}@"
            f"{config.host}:{config.port}/{config.database}"
        )
        # echo=False evita que se impriman todos los logs de SQL en consola
        self.engine: Engine = create_engine(self.connection_string, echo=False)

    def save_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        schema: Optional[str] = "public",
    ) -> None:
        """
        Guarda un DataFrame de Pandas en una tabla de PostgreSQL.

        Args:
            df (pd.DataFrame): Datos procesados a guardar.
            table_name (str): Nombre de la tabla de destino.
            if_exists (str): Qué hacer si la tabla existe: 'fail', 'replace', 'append'.
            schema (str, optional): Esquema de la base de datos (ej. 'public').

        Example:
            >>> db = PostgresManager(config)
            >>> db.save_dataframe(df_processed, 'results_scraping')
        """
        try:
            df.to_sql(
                name=table_name,
                con=self.engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method="multi",
                chunksize=10000,
            )
            logger.info(
                f"Datos insertados correctamente en tabla: {table_name}"
            )
        except Exception as e:
            logger.error(f"Error al guardar en Postgres: {e}")
            raise

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Ejecuta una consulta SQL personalizada y devuelve los resultados.
        """
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            return [dict(row._mapping) for row in result]
