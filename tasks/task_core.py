from typing import Type
import pandas as pd
from sqlalchemy.orm import DeclarativeBase

from config.settings import DEFAULT_START_DATES
from util.db_utils import get_latest_date, ModelFrameMapper
from base_class.base_scraper import DateChunkScraper

def run_core_task(model: Type[DeclarativeBase]) -> None:
    """
    Run the IndexPrice task. This defines the reference timeline for the entire pipeline.
    Only supports IndexPrice model for now.
    """
    if model.__name__ != "IndexPrice":
        raise ValueError(f"task_core.run_core_task only supports IndexPrice, not {model.__name__}")

    latest = get_latest_date(model.__tablename__)
    start_date = latest if latest else DEFAULT_START_DATES["IndexPrice"]

    print(f"scraping [IndexPrice] from {start_date.date()}")

    scraper: DateChunkScraper = model._scraper(start_date=start_date)
    for chunk_df in scraper.run():

        if latest is not None:
            chunk_df = chunk_df[chunk_df["date"] > start_date]

        ModelFrameMapper(model).save_to_sql(chunk_df)

if __name__ == "__main__":

    from models import IndexPrice
    run_core_task(IndexPrice)