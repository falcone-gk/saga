from db.models import WebScrappingSagaFalabella
from db.session import SessionLocal
from get_products import scrape_all


def insert_webscrapping(db_session, data: list[dict]):
    db_session.bulk_insert_mappings(
        WebScrappingSagaFalabella,
        data,
    )


def main():
    print("Iniciando scraping...\n")
    data = scrape_all()
    db = SessionLocal()

    try:
        insert_webscrapping(db, data)
        db.commit()
    finally:
        db.close()

    print(f"\nINFO: Se realizó el scraping de {len(data)} productos.")


if __name__ == "__main__":
    main()
