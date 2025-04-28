import pandas as pd
import requests
from io import StringIO
from sqlalchemy import Column, String, DateTime

from config.settings import USER_AGENT
from base_class.base_scraper import OneTimeScraper
from models.base import Base

class StockInfoScraper(OneTimeScraper):

    def __init__(self):
        super().__init__()
        self.columns = ["stock_id","stock_name","market_type","industry_type","asset_type","listing_date"]
    
    def run(self):
        response_twse = requests.get("https://isin.twse.com.tw/isin/C_public.jsp?strMode=2",
                                headers={"user-agent": USER_AGENT})
        response_tpex = requests.get("https://isin.twse.com.tw/isin/C_public.jsp?strMode=4",
                                headers={"user-agent": USER_AGENT})
        df_twse = pd.read_html(StringIO(response_twse.text), skiprows=1)[0].ffill()
        df_tpex = pd.read_html(StringIO(response_tpex.text), skiprows=1)[0].ffill()
        df = pd.concat([df_twse, df_tpex], ignore_index=True)
        df.columns = ["id_and_name","isin_code","listing_date","market_type","industry_type","CFIcode","asset_type"]
        df = df.drop(df[df.nunique(axis=1) == 1].index)
        df['market_type'] = df['market_type'].map({"上市臺灣創新板":"tib",
                                                   "上市":"twse",
                                                   "上櫃":"tpex"})
        df['asset_type'] = df['asset_type'].map({"創新板":"inno",
                                                 "股票":"stk",
                                                 "ETF":"etf",
                                                 "ETN":"etn"})
        df['listing_date'] = pd.to_datetime(df['listing_date'])
        df[['stock_id',"stock_name"]] = df['id_and_name'].str.split(n=1, expand=True)
        df = df.dropna(ignore_index=True)
        return df[self.columns]

class StockInfo(Base):
    __tablename__ = 'stock_info'
    _scraper = StockInfoScraper

    stock_id = Column(String(8), primary_key=True, nullable=False)
    stock_name = Column(String(16), nullable=True)
    market_type = Column(String(4), nullable=True)
    industry_type = Column(String(8), nullable=True)
    asset_type = Column(String(4), nullable=True)
    listing_date = Column(DateTime, nullable=True)
