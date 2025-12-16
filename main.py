import time

from get_products import scrape_all

# def insert_webscrapping(db_session, data):
#     db_session.bulk_insert_mappings(
#         WebScrappingSagaFalabella,
#         data,
#     )


def main():
    start = time.perf_counter()

    print("Iniciando scraping...\n")
    # data = scrape_all()
    counter = scrape_all()
    # db = SessionLocal()

    # try:
    #     insert_webscrapping(db, data)
    #     db.commit()
    # finally:
    #     db.close()

    print(f"\nINFO: Se realizó el scraping TOTAL de {counter} productos.")

    end = time.perf_counter()
    elapsed = end - start

    print(f"Tiempo total: {elapsed:.2f} segundos")


if __name__ == "__main__":
    main()
