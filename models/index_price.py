import pandas as pd
from sqlalchemy import Column, DateTime, REAL, BigInteger

from base_class.base_scraper import DateChunkScraper
from models.base import Base

class IndexPriceScraper(DateChunkScraper):

    def __init__(self, start_date: pd.Timestamp):
        super().__init__(start_date)
        start_date = start_date.replace(day=1)
        end_date = pd.Timestamp.now().replace(day=1)
        self.query_dates = pd.date_range(start = start_date, end = end_date, freq='MS')
    
    def _scrape_date(self, date:pd.Timestamp, test_mode = False):
        url = "https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_HIST"
        params = {"date": date.strftime('%Y%m%d'), "response": "json"}
        response = self.session.get(url, params=params)
        self.validate_response(response)
        df = pd.DataFrame(response.json()['data'], columns = ['date','open','high','low','close'])
        df['date'] = df['date'].str.strip().str.replace(r"^\d+", lambda x: str(int(x.group()) + 1911), regex=True)
        df['date'] = pd.to_datetime(df['date'], format="%Y/%m/%d")
        if test_mode: return df
        self.respect_rate_limit()
        
        url = "https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK"
        response = self.session.get(url, params=params)
        self.validate_response(response)
        volume = pd.DataFrame(response.json()['data'])[2]
        df['volume'] = volume
        self.respect_rate_limit()
        return df
    def run(self):
        for t in self.query_dates:
            df = self._scrape_date(t)
            df = df[df['date'] >= self.start_date].reset_index(drop=True)
            numeric_cols = ['open','high','low','close','volume']
            df[numeric_cols] = self.parse_comma(df[numeric_cols])
            df[numeric_cols] = self.to_numeric(df[numeric_cols]).astype(float)
            yield df

class IndexPrice(Base):
    __tablename__ = 'index_price'
    _scraper = IndexPriceScraper

    date = Column(DateTime, primary_key=True, nullable=False)
    open = Column(REAL, nullable=True)
    high = Column(REAL, nullable=True)
    low = Column(REAL, nullable=True)
    close = Column(REAL, nullable=True)
    volume = Column(BigInteger, nullable=False)
