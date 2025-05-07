from typing import Type
from sqlalchemy.orm import DeclarativeBase

from base_class.base_scraper import OneTimeScraper, DailyScraper, PeriodicScraper
from base_class.base_transformer import Transformer

from tasks.task_core import run_core_task
from tasks.task_onetime import run_onetime_task
from tasks.task_daily import run_daily_task
from tasks.task_periodic import run_periodic_task
from tasks.task_transform import run_transform_task
from tasks.create_tables import create_tables

def run_task(model: Type[DeclarativeBase]) -> None:

    if model.__name__ == "IndexPrice":
        run_core_task(model)
        return
    
    scraper_cls = model._scraper

    if issubclass(scraper_cls, OneTimeScraper):
        run_onetime_task(model)
    elif issubclass(scraper_cls, DailyScraper):
        run_daily_task(model)
    elif issubclass(scraper_cls, PeriodicScraper):
        run_periodic_task(model)
    elif issubclass(scraper_cls, Transformer):
        run_transform_task(model)
    else:
        raise ValueError(f"Unsupported scraper/transformer type: {scraper_cls.__name__}")
