import pandas as pd
from sqlalchemy import Column, DateTime, String, REAL

from base_class.base_transformer import Transformer
from models.base import Base
from models.stock_price import StockPrice
from database.db_utils import read_sql_fast, ModelFrameMapper


class AdjustedPriceTransformer(Transformer):

    def __init__(self):
        
        adj_record_df = read_sql_fast('''SELECT date, stock_id, adjustment_factor, 'div' AS source_table FROM stock_dividend
        UNION ALL
        SELECT date, stock_id, adjustment_factor, 'capred' AS source_table FROM stock_cap_reduction
        UNION ALL
        SELECT date, stock_id, adjustment_factor, 'split' AS source_table FROM stock_split
        ''')
        adj_record_df['date'] = pd.to_datetime(adj_record_df['date'])

        print("[AdjustedPriceTransformer]: Loading stock_price")

        price_record_df = ModelFrameMapper(StockPrice).read_sql("""SELECT date,stock_id,open,high,low,close 
                                                                     FROM stock_price
                                                                     """)
        
        print("[AdjustedPriceTransformer]: Calculating adjustment factor")

        all_dates = sorted(price_record_df['date'].unique())
        all_stocks = sorted(price_record_df['stock_id'].unique())
        
        self.adj_df = (adj_record_df
                    .groupby(['date', 'stock_id'])['adjustment_factor']
                    .prod()
                    .unstack())
        self.adj_df = self.adj_df.reindex(index=all_dates, columns=all_stocks, fill_value=1.0)
        self.adj_df = self.adj_df.fillna(1.0).cumprod()
        self.adj_df = self.adj_df/self.adj_df.iloc[-1] # Backward adjustment

        self.price_df = price_record_df.pivot(index='date',columns='stock_id') # col: MultiIndex(price, stock_id)
    
    def run(self) -> pd.DataFrame:
        adjusted_price_records = {
            # (idx: date, col: stock_id) -> (idx: MultiIndex(date, stock_id) col: None)
            col: (self.adj_df * self.price_df[col]).stack()
            for col in ['open', 'high', 'low', 'close']}
        df = pd.concat(adjusted_price_records, axis=1).reset_index()
        return df

class AdjustedPrice(Base):
    __tablename__ = 'adjusted_price'
    # Although this is a transformer, we use `_scraper` here to integrate with the task pipeline
    _scraper = AdjustedPriceTransformer

    date = Column(DateTime, primary_key=True, nullable=False)
    stock_id = Column(String(8), primary_key=True, nullable=False)
    open = Column(REAL, nullable=True)
    high = Column(REAL, nullable=True)
    low = Column(REAL, nullable=True)
    close = Column(REAL, nullable=True)