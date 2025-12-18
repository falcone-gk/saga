import logging
from logging.handlers import TimedRotatingFileHandler

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

    # ---------- Consola ----------
    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(formatter)

    # ---------- Archivo (rotación diaria) ----------
    file_handler = TimedRotatingFileHandler(
        filename=settings.LOG_DIR / "logs.log",
        when="midnight",  # rota a medianoche
        interval=1,  # cada 1 día
        backupCount=7,  # conserva 7 días de logs
        encoding="utf-8",
        utc=False,  # usa hora local (pon True si usas UTC)
    )

    # sufijo del archivo rotado
    file_handler.suffix = "%Y-%m-%d"

    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file_handler)

    return logger
