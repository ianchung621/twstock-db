import pandas as pd
import requests
from io import StringIO
from sqlalchemy import Column, DateTime, REAL, String
import warnings

from config.settings import USER_AGENT
from base_class.base_scraper import PeriodicScraper
from models.base import Base

class StockDividendScraper(PeriodicScraper):
    def __init__(self, start_date: pd.Timestamp):
        """
        The `date` column represents the date when the stock completes its dividend distribution,  
        meaning the adjustment factor includes this date
        """
        super().__init__(start_date)
        self.date_now = pd.Timestamp.now().normalize()
    
    @staticmethod
    def _mk_to_datetime(s:pd.Series, format): 
        if format == 'twse': # format: 108年01月02日
            s_year = s.str.extract(r'(\d+)年')[0].astype(int) + 1911
            s_month_day = s.str.extract(r'(\d+)月(\d+)日')
        elif format == 'tpex': # format: 108/01/01
            s_year = s.str.extract(r'(\d+)/(\d+)/(\d+)')[0].astype(int) + 1911
            s_month_day = s.str.extract(r'\d+/(\d+)/(\d+)')
        s_dt = pd.to_datetime(s_year.astype(str) + "-" + s_month_day[0] + "-" + s_month_day[1])
        return s_dt

    def _twse_divide_ratio(self):
        
        response = requests.get(
            "https://www.twse.com.tw/rwd/zh/exRight/TWT49U",
             headers = {"user-agent":USER_AGENT},
             params = {"response":"csv", "startDate":self.start_date.strftime('%Y%m%d'), "endDate":self.date_now.strftime('%Y%m%d')})
        try:
            df = pd.read_csv(StringIO(response.text.replace('=',"")), header=1, dtype=str)
        except Exception as e:
            print(f"pd.read_csv error : {e}")
            return pd.DataFrame(columns=['date', 'stock_id', 'adjustment_factor', 
                    'ex_div_close', 'open_ref_price', 'dividend_value', 'div_ref_price',
                    'dividend_type'])
        df = df[['資料日期','股票代號','除權息前收盤價','開盤競價基準','權值+息值','權/息','除權息參考價']]
        df = df.dropna(thresh = len(df.columns))
        df = df.rename(columns = {'資料日期':"date",'股票代號':"stock_id",
                                '除權息前收盤價':"ex_div_close",'開盤競價基準':"open_ref_price",
                                '權值+息值':"dividend_value",'權/息':"dividend_type",'除權息參考價':"div_ref_price"})
        df['date'] = self._mk_to_datetime(df['date'], 'twse')
        float_cols = ['ex_div_close', 'open_ref_price', 'dividend_value','div_ref_price']
        df[float_cols] = self.parse_comma(df[float_cols])
        df[float_cols] = self.to_numeric(df[float_cols])
        df['adjustment_factor'] = df['ex_div_close']/df['open_ref_price']
        if not set(df['dividend_type'].unique()) <= {'權', '息', '權息'}:
            warnings.warn(f'unexpected dividend type occurs in: {df['dividend_type'].unique()}')
        df['dividend_type'] = df['dividend_type'].map({'權':'rights','息':'dividend','權息':"both"})
        return df

    def _tpex_divide_ratio(self):
        response = requests.post(
            "https://www.tpex.org.tw/www/zh-tw/bulletin/exDailyQ",
             headers = {"user-agent":USER_AGENT},
             data = {"response":"csv", "startDate":self.start_date.strftime('%Y/%m/%d'), "endDate":self.date_now.strftime('%Y/%m/%d')})
        try:
            df = pd.read_csv(StringIO(response.text.replace('=',"")), header=2, dtype=str)
        except Exception as e:
            print(f"pd.read_csv error : {e}")
            return pd.DataFrame(columns=['date', 'stock_id', 'adjustment_factor', 
                    'ex_div_close', 'open_ref_price', 'dividend_value', 'div_ref_price',
                    'dividend_type'])
        df = df[['除權息日期', '代號', '除權息前收盤價', '開始交易基準價', '權值+息值', '權/息', '除權息參考價']]
        df = df.dropna(thresh = len(df.columns))
        df = df.rename(columns = {'除權息日期':"date",'代號':"stock_id",
                                '除權息前收盤價':"ex_div_close",'開始交易基準價':"open_ref_price",
                                '權值+息值':"dividend_value",'權/息':"dividend_type",'除權息參考價':"div_ref_price"})
        df['date'] = self._mk_to_datetime(df['date'],'tpex')
        float_cols = ['ex_div_close', 'open_ref_price', 'dividend_value','div_ref_price']
        df[float_cols] = self.parse_comma(df[float_cols])
        df[float_cols] = self.to_numeric(df[float_cols])
        df['adjustment_factor'] = df['ex_div_close']/df['open_ref_price']
        if not set(df['dividend_type'].unique()) <= {'除息', '除權', '除權息'}:
            warnings.warn(f'unexpected dividend type occurs in: {df['dividend_type'].unique()}')
        df['dividend_type'] = df['dividend_type'].map({'除權':'rights','除息':'dividend','除權息':"both"})
        return df

    def run(self):
        cols = ['date', 'stock_id', 'adjustment_factor', 
                'ex_div_close', 'open_ref_price', 'dividend_value', 'div_ref_price',
                'dividend_type']
        dfs = [self._twse_divide_ratio(), self._tpex_divide_ratio()]
        df = self.safe_concat(dfs, cols)
        df = df.sort_values('date', ignore_index=True).drop_duplicates(ignore_index=True)
        return df
    
class StockDividend(Base):
    __tablename__ = 'stock_dividend'
    _scraper = StockDividendScraper

    date = Column(DateTime, primary_key=True, nullable=False)
    stock_id = Column(String(8), primary_key=True, nullable=False)
    adjustment_factor = Column(REAL, nullable=True)
    ex_div_close = Column(REAL, nullable=True)
    open_ref_price = Column(REAL, nullable=True)
    dividend_value = Column(REAL, nullable=True)
    div_ref_price = Column(REAL, nullable=True)
    dividend_type = Column(String(8), nullable=True)