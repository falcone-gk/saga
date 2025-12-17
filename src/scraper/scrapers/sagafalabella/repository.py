from scraper.database.session import SessionLocal

from .models import WebScrappingSagaFalabella


def bulk_insert_falabella(products):
    session = SessionLocal()
    try:
        session.bulk_insert_mappings(WebScrappingSagaFalabella, products)
        session.commit()
    finally:
        session.close()
