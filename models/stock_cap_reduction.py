import pandas as pd
from io import StringIO
from sqlalchemy import Column, DateTime, REAL, String
import warnings

from config.settings import DEFAULT_START_DATES
from base_class.base_scraper import PeriodicScraper
from models.base import Base


class StockCapReductionScraper(PeriodicScraper):
    def __init__(self, start_date: pd.Timestamp):
        """
        The `date` column represents the date when the stock resumes trading after a capital reduction,  
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

    def _twse_cap_reduction(self):
        
        start_date = max(self.start_date, DEFAULT_START_DATES["StockCapReduction"])
        response = self.session.get(
            "https://www.twse.com.tw/rwd/zh/reducation/TWTAUU",
             params = {"response":"csv", 
                       "startDate":start_date.strftime('%Y%m%d'), 
                       "endDate":self.date_now.strftime('%Y%m%d')})
        try:
            df = pd.read_csv(StringIO(response.text), header=1, dtype=str)
        except Exception as e:
            print(f"twse_cap_reduction pd.read_csv error : {e}")
            return pd.DataFrame(columns=['date', 'stock_id', 'adjustment_factor',
                    'reduction_close', 'reduction_ref_price', 'open_ref_price', 'reduction_reason'])
        df = df[['恢復買賣日期','股票代號','停止買賣前收盤價格','恢復買賣參考價','開盤競價基準','減資原因']]
        df = df.dropna(thresh = len(df.columns))
        df = df.rename(columns = {'恢復買賣日期':"date",'股票代號':"stock_id",
                                '停止買賣前收盤價格':"reduction_close",'開盤競價基準':"open_ref_price",
                                '減資原因':"reduction_reason",'恢復買賣參考價':"reduction_ref_price"})
        df['date'] = self._mk_to_datetime(df['date'], 'twse')
        float_cols = ['reduction_close', 'reduction_ref_price', 'open_ref_price']
        df[float_cols] = self.parse_comma(df[float_cols])
        df[float_cols] = self.to_numeric(df[float_cols])
        df['adjustment_factor'] = df['reduction_close']/df['open_ref_price']
        if not set(df['reduction_reason'].unique()) <= {'退還股款', '彌補虧損'}:
            warnings.warn(f'unexpected reduction reason occurs in: {df['reduction_reason'].unique()}')
        df['reduction_reason'] = df['reduction_reason'].map({'退還股款':'return_cap','彌補虧損':'offset_loss'})
        return df

    def _tpex_cap_reduction(self):
        response = self.session.post(
            "https://www.tpex.org.tw/www/zh-tw/bulletin/revivt",
             data = {"response":"csv", 
                     "startDate":self.start_date.strftime('%Y/%m/%d'), 
                     "endDate":self.date_now.strftime('%Y/%m/%d')})
        try:
            df = pd.read_csv(StringIO(response.text), header=2, dtype=str)
        except Exception as e:
            print(f"tpex_cap_reduction pd.read_csv error : {e}")
            return pd.DataFrame(columns=['date', 'stock_id', 'adjustment_factor',
                    'reduction_close', 'reduction_ref_price', 'open_ref_price', 'reduction_reason'])
        df = df[['恢復買賣日期','股票代號','最後交易日之收盤價格','減資恢復買賣開始日參考價格','開始交易基準價','減資原因']]
        df = df.dropna(thresh = len(df.columns))
        df = df.rename(columns = {'恢復買賣日期':"date",'股票代號':"stock_id",
                        '最後交易日之收盤價格':"reduction_close",'開始交易基準價':"open_ref_price",
                        '減資原因':"reduction_reason",'減資恢復買賣開始日參考價格':"reduction_ref_price"})
        df['date'] = self._mk_to_datetime(df['date'], 'tpex')
        float_cols = ['reduction_close', 'reduction_ref_price', 'open_ref_price']
        df[float_cols] = self.parse_comma(df[float_cols])
        df[float_cols] = self.to_numeric(df[float_cols])
        df['adjustment_factor'] = df['reduction_close']/df['open_ref_price']
        if not set(df['reduction_reason'].unique()) <= {'現金減資', '彌補虧損'}:
            warnings.warn(f'unexpected reduction reason occurs in: {df['reduction_reason'].unique()}')
        df['reduction_reason'] = df['reduction_reason'].map({'現金減資':'return_cap','彌補虧損':'offset_loss'})
        return df

    def run(self):
        cols = ['date', 'stock_id', 'adjustment_factor',
                'reduction_close', 'reduction_ref_price', 'open_ref_price', 'reduction_reason']
        dfs = [self._twse_cap_reduction(), self._tpex_cap_reduction()]
        df = self.safe_concat(dfs, cols)
        df = df.sort_values('date', ignore_index=True).drop_duplicates(ignore_index=True)
        return df

class StockCapReduction(Base):
    __tablename__ = 'stock_cap_reduction'
    _scraper = StockCapReductionScraper

    date = Column(DateTime, primary_key=True, nullable=False)
    stock_id = Column(String(8), primary_key=True, nullable=False)
    adjustment_factor = Column(REAL, nullable=True)
    reduction_close = Column(REAL, nullable=True)
    reduction_ref_price	= Column(REAL, nullable=True)
    open_ref_price = Column(REAL, nullable=True)
    reduction_reason = Column(String(16), nullable=True)