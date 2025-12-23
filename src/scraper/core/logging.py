import logging
import sys
from datetime import datetime
from pathlib import Path

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

    # ---------- Nombre del script (SAFE) ----------
    script_path = Path(sys.argv[0])
    script_name = script_path.stem or "interactive"

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    log_file = Path(settings.LOG_DIR) / f"{script_name}_{timestamp}.log"

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    logger.addHandler(console)
    logger.addHandler(file_handler)

    return logger
