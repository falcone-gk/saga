from .jobs.get_extra_details_product import update_product_data
from .jobs.save_to_sql import save_to_sql
from .jobs.scraper import scrape

__all__ = [
    "scrape",
    "update_product_data",
    "save_to_sql",
]
