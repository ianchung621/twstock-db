from typing import Type
from sqlalchemy.orm import DeclarativeBase

from config.settings import DEFAULT_START_DATES
from util.db_utils import get_latest_date, get_min_date, ModelFrameMapper
from base_class.base_scraper import PeriodicScraper

def run_periodic_task(model: Type[DeclarativeBase]) -> None:
    
    if not issubclass(model._scraper, PeriodicScraper):
        raise ValueError(
            f"task_core.run_periodic_task only supports PeriodicScraper, "
            f"the scraper of {model.__name__} uses {model._scraper.__bases__[0].__name__}"
        )

    latest = get_latest_date(model.__tablename__)

    if latest is not None: # start from latest date
        start_date = latest
    else: # start from min index date
        index_start = get_min_date("index_price")
        if index_start is None:
            raise RuntimeError(
                f"No existing data in `{model.__tablename__}`, and index_price has no timeline to anchor from."
            )
        start_date = index_start

    print(f"scraping [{model.__name__}] from {start_date.date()}")

    scraper: PeriodicScraper = model._scraper(start_date=start_date)
    df = scraper.run()

    if latest is not None:
        df = df[df["date"] > start_date]

    ModelFrameMapper(model).save_to_sql(df)

if __name__ == "__main__":

    from config.settings import ROUTINE_CONFIG
    from models import get_model
    
    for model_name in ROUTINE_CONFIG['periodic']:
        model = get_model(model_name)
        run_periodic_task(model)