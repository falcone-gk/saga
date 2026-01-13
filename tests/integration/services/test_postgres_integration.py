from typing import Any

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from core.schemas import PostgresConfig
from services.postgres import PostgresManager


@pytest.fixture
def memory_db_manager() -> PostgresManager:
    """Fixture que devuelve un PostgresManager conectado a SQLite en memoria."""
    config = PostgresConfig(
        host="localhost",
        port=5432,
        user="user",
        password="pass",
        database="test_db",
    )
    manager = PostgresManager(config)
    # Hack para el test: sobreescribimos el engine por uno de SQLite en memoria
    manager.engine = create_engine("sqlite:///:memory:")
    return manager


@pytest.mark.integration
def test_save_and_query_integration(memory_db_manager: PostgresManager) -> None:
    # Preparar datos
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["Producto A", "Producto B"],
            "price": [10.5, 20.0],
        }
    )
    table_name = "test_products"

    # Ejecutar acción: Guardar en DB
    memory_db_manager.save_dataframe(
        df,
        table_name,
        if_exists="replace",
        schema=None,  # schema None porque sqlite no tiene esquemas
    )

    # Ejecutar acción: Consultar
    query = f"SELECT * FROM {table_name} WHERE price > 15"
    results = memory_db_manager.execute_query(query)

    # Validar
    assert len(results) == 1
    assert results[0]["name"] == "Producto B"
    assert results[0]["price"] == 20.0


@pytest.mark.integration
def test_execute_query_empty(memory_db_manager: PostgresManager) -> None:
    # Crear una tabla vacía
    memory_db_manager.engine.connect().execute(
        text("CREATE TABLE empty_table (id INT)")
    )

    results = memory_db_manager.execute_query("SELECT * FROM empty_table")
    assert results == []


@pytest.mark.integration
def test_execute_query_with_data(memory_db_manager: PostgresManager) -> None:
    """
    Valida que execute_query devuelva una lista de diccionarios
    con los datos correctos insertados manualmente.
    """
    # Crear tabla e insertar datos manualmente
    with memory_db_manager.engine.connect() as conn:
        conn.execute(text("CREATE TABLE users (id INT, name TEXT)"))
        conn.execute(
            text("INSERT INTO users (id, name) VALUES (:id, :name)"),
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        )
        # En SQLite/Postgres manual, necesitamos hacer commit para que persista
        conn.commit()

    # Ejecutar la query a través de nuestro Manager
    query = "SELECT id, name FROM users ORDER BY id ASC"
    results: list[dict[str, Any]] = memory_db_manager.execute_query(query)

    # Validaciones
    assert len(results) == 2

    # Validamos el primer registro
    assert results[0]["id"] == 1
    assert results[0]["name"] == "Alice"

    # Validamos el segundo registro
    assert results[1]["id"] == 2
    assert results[1]["name"] == "Bob"

    # Validamos que sea una lista de diccionarios (comportamiento esperado)
    assert isinstance(results[0], dict)
