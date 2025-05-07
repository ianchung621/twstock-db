from typing import Type
from sqlalchemy.orm import DeclarativeBase

from database.db_utils import ModelFrameMapper
from base_class.base_scraper import OneTimeScraper

def run_onetime_task(model: Type[DeclarativeBase]):
    
    if not issubclass(model._scraper, OneTimeScraper):
        raise ValueError(
            f"task_core.run_onetime_task only supports OneTimeScraper, "
            f"the scraper of {model.__name__} uses {model._scraper.__bases__[0].__name__}"
        )

    scraper: OneTimeScraper = model._scraper()
    df = scraper.run()
    print(f'scraping [{model.__tablename__}]')
    ModelFrameMapper(model).save_to_sql(df, update_mode = 'replace')

if __name__ == "__main__":

    from config.settings import ROUTINE_CONFIG
    from models import get_model
    
    for model_name in ROUTINE_CONFIG['onetime']:
        model = get_model(model_name)
        run_onetime_task(model)