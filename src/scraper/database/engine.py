from sqlalchemy import create_engine

from scraper.core.logging import get_logger
from scraper.core.settings import settings

logger = get_logger(__name__)

engine = create_engine(
    settings.DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
)

logger.info("Engine SQLAlchemy inicializado")
