import logging
from logging.handlers import RotatingFileHandler

from scraper.core.settings import settings


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    level = logging.DEBUG if settings.DEBUG else logging.INFO
    logger.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # consola
    console = logging.StreamHandler()
    console.setFormatter(formatter)

    # archivo
    file_handler = RotatingFileHandler(
        settings.LOG_DIR / f"{settings.APP_NAME}.log",
        maxBytes=5_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file_handler)

    return logger
