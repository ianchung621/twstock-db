import pandas as pd
from sqlalchemy import Column, DateTime, String, BigInteger
from io import StringIO
from concurrent.futures import ThreadPoolExecutor

from database.db_utils import table_has_data, read_sql_fast
from base_class.base_scraper import PeriodicScraper
from models.stock_info import StockInfoScraper
from models.base import Base


class StockRevenueScraper(PeriodicScraper):

    def __init__(self, start_date):
        super().__init__(start_date)

        if table_has_data('stock_info'):
            df = read_sql_fast("SELECT stock_id, asset_type FROM stock_info")
        else:
            df = StockInfoScraper().run()
        
        self.stock_ids = df.loc[df['asset_type'] == 'stk', 'stock_id'].to_list()
        self.columns = ["date","stock_id","revenue"]


    @staticmethod
    def _mk_to_datetime(s:pd.Series): # format: 108/01
        extracted = s.str.split('/', n=1, expand=True)
        s_year = extracted[0].astype(int) + 1911
        s_month = extracted[1]
        s_dt = pd.to_datetime(s_year.astype(str) + "-" + s_month + "-01")
        return s_dt
    
    def _scrape_stock(self, stock_id: str):

        response = self.session.get(f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zch/zch_{stock_id}.djhtm")
        self.validate_response(response)
        
        try:
            df = pd.read_html(StringIO(response.text), attrs={'id':'oMainTable'}, skiprows=6)[0]
            df = df[[0,1]]
            df.columns = ['date','revenue']
            df['date'] = self._mk_to_datetime(df['date'])
            df['stock_id'] = stock_id
            return df[self.columns]
        
        except Exception as e:
            print(f"Extraction Error occurs at {response.url}")
            print(e)
            return pd.DataFrame(columns=self.columns)

    def run(self):
        with ThreadPoolExecutor() as executor:
            dfs = list(executor.map(self._scrape_stock, self.stock_ids))

        df = pd.concat([df for df in dfs if not df.empty], ignore_index=True)    
        df = df[df['date'] >= self.start_date].reset_index(drop=True)
        return df


class StockRevenue(Base):
    __tablename__ = 'stock_revenue'
    _scraper = StockRevenueScraper

    date = Column(DateTime, primary_key=True, nullable=False)
    stock_id = Column(String(8), primary_key=True, nullable=False)
    revenue = Column(BigInteger, nullable=False)