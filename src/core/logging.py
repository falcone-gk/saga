import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from core.settings import settings


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
    logger.addHandler(console)

    # ---------- Nombre del script (SAFE) ----------
    # Evitamos que al correr test se generen logs
    if "pytest" in sys.modules or os.getenv("PYTEST_CURRENT_TEST"):
        return logger

    try:
        script_name = Path(sys.argv[0]).stem or "interactive"
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_file = settings.LOG_DIR / f"{script_name}_{timestamp}.log"

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception:
        # fallback silencioso: consola sigue funcionando
        pass

    return logger
