from sqlalchemy import create_engine, inspect
import models
from models import Base
from config.settings import SQLALCHEMY_DATABASE_URL, DB_NAME, ROUTINE_CONFIG

def _get_all_table_set():

    def get_model(model_name: str):
        model_cls = getattr(models, model_name, None)
        if model_cls is None:
            raise AttributeError(f"Model {model_name} not found in models/__init__.py")
        return model_cls
    
    return set(get_model(mn).__tablename__ for mn in ROUTINE_CONFIG['all'])

def _get_table_set(engine):
    inspector = inspect(engine)
    return set(inspector.get_table_names())

def create_tables():

    print("Create tables")
    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    existing_tables = _get_table_set(engine)
    if existing_tables == _get_all_table_set():
        print("All tables exists. Skipping.")
        return
    
    Base.metadata.create_all(engine)
    updated_tables = _get_table_set(engine)
    
    new_tables = updated_tables - existing_tables

    print(f"{DB_NAME} content: {sorted(existing_tables)}")
    print(f"Add new tables: {sorted(new_tables)}")

if __name__ == "__main__":    
    create_tables()
