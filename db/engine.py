from sqlalchemy import create_engine

DATABASE_URL = "postgresql+psycopg2://postgres:root@localhost:5432/test_biomont"

engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    echo=False,
)
