from unittest.mock import patch

import pandas as pd

from core.schemas import PostgresConfig
from services.postgres import PostgresManager


def test_save_dataframe_calls_to_sql() -> None:
    # Setup
    config = PostgresConfig(
        host="h", port=5432, user="u", password="p", database="d"
    )
    df = pd.DataFrame({"col1": [1, 2]})

    # Mockeamos el engine de sqlalchemy para que no intente conectar
    with patch("services.postgres.create_engine"):
        manager = PostgresManager(config)
        # Mockeamos el método to_sql de pandas
        with patch.object(pd.DataFrame, "to_sql") as mock_to_sql:
            manager.save_dataframe(df, "test_table")

            # Verificamos que se llamó a to_sql con los parámetros esperados
            mock_to_sql.assert_called_once()
            _, kwargs = mock_to_sql.call_args
            assert kwargs["name"] == "test_table"
            assert kwargs["if_exists"] == "append"
