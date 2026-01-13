from pathlib import Path

import pandas as pd
import pytest

from services.datalake import DataLakeManager


@pytest.fixture
def local_manager() -> DataLakeManager:
    # Usamos el modo local para no depender de un servidor HDFS
    return DataLakeManager(connection_type="local")


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame({"id": [1, 2], "name": ["test1", "test2"]})


@pytest.mark.integration
def test_write_read_json(
    local_manager: DataLakeManager, tmp_path: Path
) -> None:
    file_path = tmp_path / "data.json"
    data = {"key_1": "value_1", "key_2": "value_2"}

    # Escribir
    local_manager.write_data(file_path, data, fmt="json")

    # Leer
    result = local_manager.read_data(file_path, fmt="json")
    assert not isinstance(result, pd.DataFrame)

    assert result == data
    assert isinstance(result, dict)


@pytest.mark.integration
def test_write_read_parquet(
    local_manager: DataLakeManager, sample_df: pd.DataFrame, tmp_path: Path
) -> None:
    file_path = tmp_path / "data.parquet"

    # Escribir
    local_manager.write_data(file_path, sample_df, fmt="parquet")

    # Leer
    result = local_manager.read_data(file_path, fmt="parquet")

    assert isinstance(result, pd.DataFrame)

    # Validar contenido del DataFrame
    pd.testing.assert_frame_equal(result, sample_df)


@pytest.mark.integration
def test_list_and_delete_files(
    local_manager: DataLakeManager, tmp_path: Path
) -> None:
    # Creamos un subdirectorio en el path temporal
    test_dir = tmp_path / "test_folder"
    test_dir.mkdir()
    file_path = test_dir / "temp.json"

    local_manager.write_data(file_path, {"a": 1}, fmt="json")

    # Listar (PyArrow suele devolver paths absolutos o relativos al fs)
    files = local_manager.list_files(str(test_dir))
    assert any(str(file_path.name) in f for f in files)

    # Borrar
    local_manager.delete_file(str(file_path))
    files_after = local_manager.list_files(str(test_dir))
    assert len(files_after) == 0


@pytest.mark.unit
def test_write_invalid_format(
    local_manager: DataLakeManager, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="Formato 'xml' no soportado"):
        local_manager.write_data(tmp_path / "test.xml", {}, fmt="xml")


@pytest.mark.unit
def test_write_parquet_without_dataframe(
    local_manager: DataLakeManager, tmp_path: Path
) -> None:
    with pytest.raises(
        TypeError, match="Para Parquet, 'data' debe ser un pandas DataFrame"
    ):
        local_manager.write_data(
            tmp_path / "test.parquet", {"not": "a_dataframe"}, fmt="parquet"
        )
