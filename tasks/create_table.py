from sqlalchemy import create_engine
from models import Base
from config.settings import SQLALCHEMY_DATABASE_URL

def create_all_tables():
    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    Base.metadata.create_all(engine)

if __name__ == "__main__":
    from config.settings import DB_NAME
    create_all_tables()
