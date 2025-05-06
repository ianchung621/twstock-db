import pandas as pd
import warnings
from io import BytesIO
from typing import Type

import psycopg
from sqlalchemy import create_engine,inspect
from sqlalchemy import DateTime, String, Integer, Float, REAL, BigInteger
from sqlalchemy.orm import DeclarativeBase

from config.settings import SQLALCHEMY_DATABASE_URL, DB_DSN

def get_engine():
    return create_engine(SQLALCHEMY_DATABASE_URL)

def _copy_to_buffer(cursor: psycopg.Cursor, query: str) -> BytesIO:
    buffer = BytesIO()
    with cursor.copy(f"COPY ({query}) TO STDOUT WITH CSV HEADER") as copy:
        for chunk in copy:
            buffer.write(chunk)
    buffer.seek(0)
    return buffer

def read_sql_fast(query_str: str, dsn: str = DB_DSN) -> pd.DataFrame:
    """Read a SQL query into a DataFrame quickly using PostgreSQL COPY."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        buffer = _copy_to_buffer(cur, query_str)
        df = pd.read_csv(buffer)
    return df

def get_latest_date(table_name: str) -> pd.Timestamp | None:

    query = f"SELECT MAX(date) AS latest FROM {table_name}"
    df = read_sql_fast(query, dsn=DB_DSN)
    if df.empty: 
        return None
    else:
        latest = df["latest"].iloc[0]
        return pd.to_datetime(latest) if pd.notna(latest) else None


def get_min_date(table_name: str) -> pd.Timestamp | None:

    query = f"SELECT MIN(date) AS mindate FROM {table_name}"
    df = read_sql_fast(query, dsn=DB_DSN)
    if df.empty: 
        return None
    else:
        mindate = df["mindate"].iloc[0]
        return pd.to_datetime(mindate) if pd.notna(mindate) else None
        
class ModelFrameMapper:
    """
    A utility class to map SQLAlchemy models to Pandas dtypes and SQLAlchemy dtypes.
    """
    
    def __init__(self, model: Type[DeclarativeBase]):
        self.model = model

    @property
    def pandas_dtypes(self) -> dict:
        """
        Extracts Pandas dtype mappings from the SQLAlchemy model.
        """
        dtype_mapping = {}
        for column in self.model.__table__.columns:
            col_type = column.type
            if isinstance(col_type, BigInteger):
                dtype_mapping[column.name] = "int64"
            elif isinstance(col_type, Integer):
                dtype_mapping[column.name] = "int32"
            elif isinstance(col_type, REAL): 
                dtype_mapping[column.name] = "float32"
            elif isinstance(col_type, Float):  
                dtype_mapping[column.name] = "float64"
            elif isinstance(col_type, DateTime):
                dtype_mapping[column.name] = "datetime64[ns]"
            elif isinstance(col_type, String):
                dtype_mapping[column.name] = "string"
        return dtype_mapping

    @property
    def sqlalchemy_dtypes(self) -> dict:
        """
        Extracts SQLAlchemy dtype mappings for `df.to_sql()`.
        """
        sqlalchemy_dtype_mapping = {}
        for column in self.model.__table__.columns:
            
            if isinstance(column.type, BigInteger):
                sqlalchemy_dtype_mapping[column.name] = BigInteger
            elif isinstance(column.type, Integer):
                sqlalchemy_dtype_mapping[column.name] = Integer
            elif isinstance(column.type, REAL):
                sqlalchemy_dtype_mapping[column.name] = REAL
            elif isinstance(column.type, Float):
                sqlalchemy_dtype_mapping[column.name] = Float(precision=64)
            elif isinstance(column.type, DateTime):
                sqlalchemy_dtype_mapping[column.name] = DateTime
            elif isinstance(column.type, String):
                sqlalchemy_dtype_mapping[column.name] = String(column.type.length)
        return sqlalchemy_dtype_mapping

    def cast_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Casts a DataFrame to match the SQLAlchemy model's dtypes.
        """
        return df.astype(self.pandas_dtypes)

    def save_to_sql(self, df: pd.DataFrame, engine = get_engine(), update_mode = 'append'):
        """
        Saves the DataFrame to SQL with the correct dtypes.
        """
        non_nullable_columns = [column.name for column in inspect(self.model).columns if not column.nullable]
        nan_rows = df[df[non_nullable_columns].isna().any(axis=1)]

        if not nan_rows.empty:
            warning_message = f"Warning: {len(nan_rows)} rows contain NaN values:\n{nan_rows}"
            warnings.warn(warning_message)
            
        df = df.dropna(subset=non_nullable_columns, ignore_index=True)
        df = self.cast_dataframe(df)

        if df.empty:
            print(f"No valid rows to insert into `{self.model.__tablename__}`. Skipping.")
            return
        
        print(f"{update_mode} table: {self.model.__tablename__}\n{df.head()}\n...\n{df.tail()}")
        df.to_sql(
            self.model.__tablename__,
            con=engine,
            if_exists=update_mode,
            index=False,
            dtype=self.sqlalchemy_dtypes
        )
    
    def read_sql(self, query_str=None, dsn: str = DB_DSN):
        
        df = read_sql_fast(query_str, dsn)
        dtype_in_col = {col: dtype for col, dtype in self.pandas_dtypes.items() if col in df.columns}
        df = df.astype(dtype=dtype_in_col)
        
        return df

if __name__ == "__main__":

    print(get_min_date("index_price"))