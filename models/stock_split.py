import pandas as pd
import requests
from io import StringIO
from sqlalchemy import Column, DateTime, REAL, String

from config.settings import USER_AGENT
from base_class.base_scraper import PeriodicScraper
from models.base import Base

class StockSplitScraper(PeriodicScraper):
    def __init__(self, start_date: pd.Timestamp):
        """
        The `date` column represents the date when the stock resumes trading after split (or merge),  
        meaning the adjustment factor includes this date
        """
        super().__init__(start_date)
        self.date_now = pd.Timestamp.now().normalize()
    
    @staticmethod
    def _mk_to_datetime(s:pd.Series, format): 
        if format == 'twse': # format: 108/01/02
            s_year = s.str.extract(r'(\d+)/(\d+)/(\d+)')[0].astype(int) + 1911
            s_month_day = s.str.extract(r'\d+/(\d+)/(\d+)')
        elif format == 'tpex': # format: 1080102
            s_year = s.str[:3].astype(int) + 1911
            s_month_day = s.str[3:5], s.str[5:7]
        s_dt = pd.to_datetime(s_year.astype(str) + "-" + s_month_day[0] + "-" + s_month_day[1])
        return s_dt

    def _twse_split(self):
        
        response = requests.get(
            "https://www.twse.com.tw/rwd/zh/change/TWTB8U",
             headers = {"user-agent":USER_AGENT},
             params = {"response":"csv", "startDate":self.start_date.strftime('%Y%m%d'), "endDate":self.date_now.strftime('%Y%m%d')})
        try:
            df = pd.read_csv(StringIO(response.text), header=1, dtype=str)
        except Exception as e:
            print(f"pd.read_csv error : {e}")
            return pd.DataFrame(columns=['date', 'stock_id', 'adjustment_factor',
                    'split_close', 'split_ref_price', 'open_ref_price'])
        df = df[['恢復買賣日期','股票代號','停止買賣前收盤價格','恢復買賣參考價','開盤競價基準']]
        df = df.dropna(thresh = len(df.columns))
        df = df.rename(columns = {'恢復買賣日期':"date",'股票代號':"stock_id",
                                '停止買賣前收盤價格':"split_close",'開盤競價基準':"open_ref_price",
                                '恢復買賣參考價':"split_ref_price"})
        df['date'] = self._mk_to_datetime(df['date'], 'twse')
        float_cols = ['split_close', 'split_ref_price', 'open_ref_price']
        df[float_cols] = self.parse_comma(df[float_cols])
        df[float_cols] = self.to_numeric(df[float_cols])
        df['adjustment_factor'] = df['split_close']/df['open_ref_price']
        return df

    def _tpex_split(self):
        response = requests.post(
            "https://www.tpex.org.tw/www/zh-tw/bulletin/pvChgRslt",
             headers = {"user-agent":USER_AGENT},
             data = {"response":"csv", "startDate":self.start_date.strftime('%Y/%m/%d'), "endDate":self.date_now.strftime('%Y/%m/%d')})
        try:
            df = pd.read_csv(StringIO(response.text), header=2, dtype=str)
        except Exception as e:
            print(f"pd.read_csv error : {e}")
            return pd.DataFrame(columns=['date', 'stock_id', 'adjustment_factor',
                    'split_close', 'split_ref_price', 'open_ref_price'])
        df = df[['恢復買賣日期', '證券代號', '最後交易日之收盤價格', '恢復買賣開始參考價', '開始交易基準價']]
        df = df.dropna(thresh = len(df.columns))
        df = df.rename(columns = {'恢復買賣日期':"date",'證券代號':"stock_id",
                                '最後交易日之收盤價格':"split_close",'開始交易基準價':"open_ref_price",
                                '恢復買賣開始參考價':"split_ref_price"})
        df['date'] = self._mk_to_datetime(df['date'], 'tpex')
        float_cols = ['split_close', 'split_ref_price', 'open_ref_price']
        df[float_cols] = self.parse_comma(df[float_cols])
        df[float_cols] = self.to_numeric(df[float_cols])
        df['adjustment_factor'] = df['split_close']/df['open_ref_price']
        return df
    
    def _twse_etf_split(self):
        
        response = requests.get(
            "https://www.twse.com.tw/rwd/zh/split/TWTCAU",
             headers = {"user-agent":USER_AGENT},
             params = {"response":"csv", "startDate":self.start_date.strftime('%Y%m%d'), "endDate":self.date_now.strftime('%Y%m%d')})
        try:
            df = pd.read_csv(StringIO(response.text.replace("=","")), header=1, dtype=str)
        except Exception as e:
            print(f"pd.read_csv error : {e}")
            return pd.DataFrame(columns=['date', 'stock_id', 'adjustment_factor',
                    'split_close', 'split_ref_price', 'open_ref_price'])
        df = df[['恢復買賣日期','ETF代號','停止買賣前收盤價格','恢復買賣參考價','開盤競價基準']]
        df = df.dropna(thresh = len(df.columns))
        df = df.rename(columns = {'恢復買賣日期':"date",'ETF代號':"stock_id",
                                '停止買賣前收盤價格':"split_close",'開盤競價基準':"open_ref_price",
                                '恢復買賣參考價':"split_ref_price"})
        df['date'] = self._mk_to_datetime(df['date'], 'twse')
        float_cols = ['split_close', 'split_ref_price', 'open_ref_price']
        df[float_cols] = self.parse_comma(df[float_cols])
        df[float_cols] = self.to_numeric(df[float_cols])
        df['adjustment_factor'] = df['split_close']/df['open_ref_price']
        return df
    
    def _tpex_etf_split(self):
        response_split = requests.post(
            "https://www.tpex.org.tw/www/zh-tw/bulletin/etfSplitRslt",
             headers = {"user-agent":USER_AGENT},
             data = {"response":"csv", "startDate":self.start_date.strftime('%Y/%m/%d'), "endDate":self.date_now.strftime('%Y/%m/%d')})
        try:
            df_split = pd.read_csv(StringIO(response_split.text), header=2, dtype=str)
        except Exception as e:
            print(f"pd.read_csv error : {e}")
            df_split = pd.DataFrame(columns=['恢復買賣日期', '證券代號', '最後交易日之收盤價格', '恢復買賣開始參考價', '開始交易基準價'])
        response_merge = requests.post(
            "https://www.tpex.org.tw/www/zh-tw/bulletin/etfRvsRslt",
             headers = {"user-agent":USER_AGENT},
             data = {"response":"csv", "startDate":self.start_date.strftime('%Y/%m/%d'), "endDate":self.date_now.strftime('%Y/%m/%d')})
        try:
            df_merge = pd.read_csv(StringIO(response_merge.text), header=2, dtype=str)
        except Exception as e:
            print(f"pd.read_csv error : {e}")
            df_merge = pd.DataFrame(columns=['恢復買賣日期', '證券代號', '最後交易日之收盤價格', '恢復買賣開始參考價', '開始交易基準價'])
        df_split = df_split[['恢復買賣日期', '證券代號', '最後交易日之收盤價格', '恢復買賣開始參考價', '開始交易基準價']]
        df_merge = df_merge[['恢復買賣日期', '證券代號', '最後交易日之收盤價格', '恢復買賣開始參考價', '開始交易基準價']]
        df = pd.concat([df_split, df_merge], ignore_index = True)
        df = df.dropna(thresh = len(df.columns))
        df = df.rename(columns = {'恢復買賣日期':"date",'證券代號':"stock_id",
                                '最後交易日之收盤價格':"split_close",'開始交易基準價':"open_ref_price",
                                '恢復買賣開始參考價':"split_ref_price"})
        df['date'] = self._mk_to_datetime(df['date'], 'tpex')
        float_cols = ['split_close', 'split_ref_price', 'open_ref_price']
        df[float_cols] = self.parse_comma(df[float_cols])
        df[float_cols] = self.to_numeric(df[float_cols])
        df['adjustment_factor'] = df['split_close']/df['open_ref_price']
        return df
    


    def run(self):
        dfs = [self._twse_split(), 
             self._tpex_split(),
             self._tpex_etf_split(),
             self._twse_etf_split()]
        df = pd.concat(
            [dfi for dfi in dfs if not dfi.empty],
            ignore_index=True)
        df = df.sort_values('date', ignore_index=True).drop_duplicates(ignore_index=True)
        return df[['date', 'stock_id', 'adjustment_factor',
                    'split_close', 'split_ref_price', 'open_ref_price']]
    

class StockSplit(Base):
    __tablename__ = 'stock_split'
    _scraper = StockSplitScraper

    date = Column(DateTime, primary_key=True, nullable=False)
    stock_id = Column(String(8), primary_key=True, nullable=False)
    adjustment_factor = Column(REAL, nullable=True)
    split_close	= Column(REAL, nullable=True)
    split_ref_price	= Column(REAL, nullable=True)
    open_ref_price = Column(REAL, nullable=True)