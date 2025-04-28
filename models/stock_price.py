import warnings
import pandas as pd
import requests
from sqlalchemy import Column, DateTime, String, REAL, BigInteger

from config.settings import USER_AGENT
from base_class.base_scraper import DailyScraper
from models.base import Base

class StockPriceScraper(DailyScraper):

    def __init__(self, date:pd.Timestamp):
        super().__init__(date)
        self.__int_cols = ['volume', 'transactions_number', 'turnover']
        self.__float_cols = ['open', 'high', 'low', 'close']
        self.__columns = ["date", "stock_id",
                            'open', 'high', 'low', 'close',
                            'volume', 'turnover', 'transactions_number']

    def _scrape_twse(self):
        response = requests.post(
            "http://www.twse.com.tw/exchangeReport/MI_INDEX",
            params = {"response":"json", "date":self.date.strftime('%Y%m%d'), "type":"ALLBUT0999"},
            headers={"user-agent":USER_AGENT})
        self.validate_response(response)
        try:
            stock_table = response.json()['tables'][8]
            df = pd.DataFrame(data = stock_table['data'], 
                              columns = stock_table['fields'])
            df = df[['證券代號', '開盤價', '最高價', '最低價', '收盤價', '成交股數', '成交金額', '成交筆數']]
            df = df.rename(columns={"證券代號": "stock_id",
                                    "成交股數": "volume", "成交筆數": "transactions_number", "成交金額": "turnover",
                                    "開盤價": "open", "收盤價": "close",
                                    "最高價": "high", "最低價": "low",
                                    })
            df = self.parse_comma(df)
            df[self.__float_cols] = self.to_numeric(df[self.__float_cols]).astype(float)
            df[self.__int_cols] = self.to_numeric(df[self.__int_cols]).astype('Int64')
            df["date"] = self.date
            return df[self.__columns]
        
        except Exception as e:
            print(f"Extraction Error occurs at {response.url}")
            print(e)
            return pd.DataFrame(columns=self.__columns)

    @staticmethod
    def __select_otc_id(df: pd.DataFrame):
        cond = (df['stock_id'].str.len() > 5) & (df['stock_id'].str[0] == '7')
        return df.loc[~cond].reset_index()

    def _scrape_tpex(self):
        url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes"
        response = requests.post(url,
                    params={"response":"json", "date":self.date.strftime('%Y/%m/%d')},
                    headers={"user-agent":USER_AGENT})
        self.validate_response(response)
        try:
            stock_table = response.json()['tables'][0]
            df = pd.DataFrame(data = stock_table['data'], 
                              columns = stock_table['fields'])
            df = df[['代號', '開盤', '最高', '最低', '收盤', '成交股數', '成交金額(元)', '成交筆數']]
            df = df.rename(columns={"代號": "stock_id",
                                    "成交股數": "volume", "成交筆數": "transactions_number", "成交金額(元)": "turnover",
                                    "開盤": "open", "收盤": "close",
                                    "最高": "high", "最低": "low",
                                    })
            df = self.__select_otc_id(df)
            df = self.parse_comma(df)
            df[self.__float_cols] = self.to_numeric(df[self.__float_cols]).astype(float)
            df[self.__int_cols] = self.to_numeric(df[self.__int_cols]).astype('Int64')
            df["date"] = self.date
            return df[self.__columns]
        
        except Exception as e:
            print(f"Extraction Error occurs at {response.url}")
            print(e)
            return pd.DataFrame(columns=self.__columns)


    def run(self):
        df1 = self._scrape_twse()
        df2 = self._scrape_tpex()
        if df1.empty:
            warnings.warn(f'twse {self.date.date()} is empty')
        if df2.empty:
            warnings.warn(f'tpex {self.date.date()} is empty')
        df = pd.concat([df1, df2], ignore_index=True)
        return df

class StockPrice(Base):
    __tablename__ = 'stock_price'
    _scraper = StockPriceScraper

    date = Column(DateTime, primary_key=True, nullable=False)
    stock_id = Column(String(8), primary_key=True, nullable=False)
    open = Column(REAL, nullable=True)
    high = Column(REAL, nullable=True)
    low = Column(REAL, nullable=True)
    close = Column(REAL, nullable=True)
    volume = Column(BigInteger, nullable=False)
    turnover = Column(BigInteger, nullable=False)
    transactions_number = Column(BigInteger, nullable=False)