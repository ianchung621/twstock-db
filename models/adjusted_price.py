import pandas as pd
from sqlalchemy import Column, DateTime, String, REAL

from base_class.base_transformer import Transformer
from models.base import Base
from models.stock_price import StockPrice
from database.db_utils import read_sql_fast, ModelFrameMapper

def align_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Align two DataFrames by index and columns, filling missing values."""
    common_index = df1.index.union(df2.index)
    common_columns = df1.columns.union(df2.columns)
    
    df1_aligned = df1.reindex(index=common_index, columns=common_columns)
    df2_aligned = df2.reindex(index=common_index, columns=common_columns)
    
    return df1_aligned, df2_aligned

def merge_price_dataframes(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Merge multiple DataFrames on 'date' and 'stock_id', handling overlapping price columns."""

    merged_df = dfs[0]
    
    for i, df in enumerate(dfs[1:], start=1):
        merged_df = merged_df.merge(df, on=['date', 'stock_id'], how='outer', suffixes=(False,False))

    return merged_df

class AdjustedPriceTransformer(Transformer):

    def __init__(self):
        
        adj_record_df = read_sql_fast('''SELECT date, stock_id, adjustment_factor, 'div' AS source_table FROM stock_dividend
        UNION ALL
        SELECT date, stock_id, adjustment_factor, 'capred' AS source_table FROM stock_cap_reduction
        UNION ALL
        SELECT date, stock_id, adjustment_factor, 'split' AS source_table FROM stock_split
        ORDER BY date''')
        self.price_record_df = ModelFrameMapper(StockPrice).read_sql("SELECT date,stock_id,open,high,low,close FROM stock_price")
        self.adj_df = (adj_record_df.pivot(index='date',
                                    columns='stock_id',
                                    values='adjustment_factor')
                                    )
        
    def _adjust(self, col = 'close'):
        price_df = self.price_record_df.pivot(index='date',columns='stock_id',values=col)
        price_df_aligned, adj_df_aligned = align_dataframes(price_df, self.adj_df)
        adj_df_aligned = adj_df_aligned.fillna(1).cumprod()
        adj_price_df_aligned = adj_df_aligned*price_df_aligned
        adj_price_df = adj_price_df_aligned.reindex_like(price_df)
        return adj_price_df.reset_index().melt(id_vars='date', 
                                        var_name="stock_id",
                                        value_name=col).sort_values(['date','stock_id'], ignore_index=True)
    
    def run(self):
        return merge_price_dataframes([self._adjust('open'), 
                                       self._adjust('high'), 
                                       self._adjust('low'), 
                                       self._adjust('close')])

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