from typing import Type
from sqlalchemy.orm import DeclarativeBase

from database.db_utils import ModelFrameMapper
from base_class.base_transformer import Transformer
import models.index_price


def run_transform_task(model: Type[DeclarativeBase]):
    
    if not issubclass(model._scraper, Transformer):
        raise ValueError(
            f"task_transform.run_transform_task only supports Transformer, "
            f"the `_scraper` of {model.__name__} uses {model._scraper.__bases__[0].__name__}"
        )

    transformer: Transformer = model._scraper()
    df = transformer.run()
    print(f'transforming [{model.__tablename__}]')
    ModelFrameMapper(model).save_to_sql(df, update_mode='replace')


if __name__ == "__main__":

    from config.settings import ROUTINE_CONFIG
    from models import get_model
    
    for model_name in ROUTINE_CONFIG['transformation']:
        model = get_model(model_name)
        run_transform_task(model)
