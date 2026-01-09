from dataclasses import dataclass


@dataclass
class PostgresConfig:
    """Esquema de configuracion para PostgreSQL"""

    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass
class HDFSConfig:
    """Esquema de configuracion para HDFS"""

    host: str
    port: int
    user: str
