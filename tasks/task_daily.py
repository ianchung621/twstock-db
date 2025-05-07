from typing import Type
from sqlalchemy.orm import DeclarativeBase
from tqdm import tqdm

from config.settings import DEFAULT_START_DATES
from database.db_utils import get_latest_date, get_min_date, get_date_serie, ModelFrameMapper
from base_class.base_scraper import DailyScraper


def run_daily_task(model: Type[DeclarativeBase]) -> None:
    if not issubclass(model._scraper, DailyScraper):
        raise ValueError(
            f"task_daily.run_daily_task only supports DailyScraper, "
            f"the scraper of {model.__name__} uses {model._scraper.__bases__[0].__name__}"
        )

    default_start = DEFAULT_START_DATES.get(model.__name__, DEFAULT_START_DATES["IndexPrice"])

    latest = get_latest_date(model.__tablename__)
    if latest is not None: 
        start_date = latest # priority 1: continue from the latest recorded date
    else:
        index_min = get_min_date("index_price")
        if index_min is not None and index_min > default_start:
            start_date = index_min # priority 2: if no data, anchor to the min date in index_price
        else:
            start_date = default_start # priority 3: if index_price min_date < default_start (requesting unavalable date), fallback to the default starting point of web data providers

    print(f"Scraping [{model.__name__}] from {start_date.date()}")

    date_serie = get_date_serie("index_price")
    if date_serie is None:
        raise RuntimeError("No valid date series found in index_price")

    if latest is not None:
        target_dates = date_serie[date_serie > start_date]
    else:
        target_dates = date_serie[date_serie >= start_date]

    mapper = ModelFrameMapper(model)

    for date in tqdm(target_dates, desc=f"Scraping [{model.__name__}]"):
        scraper: DailyScraper = model._scraper(date=date)
        df = scraper.run()
        mapper.save_to_sql(df)



if __name__ == "__main__":
    import yaml
    import models

    def get_model(model_name: str) -> Type[DeclarativeBase]:
        model_cls = getattr(models, model_name, None)
        if model_cls is None:
            raise AttributeError(f"Model {model_name} not found in models/__init__.py")
        return model_cls

    with open("config/routine.yaml", "r") as f:
        routine_config = yaml.safe_load(f)

    for model_name in routine_config["daily"]:
        model = get_model(model_name)
        run_daily_task(model)
