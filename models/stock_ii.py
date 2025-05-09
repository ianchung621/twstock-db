import pandas as pd
from sqlalchemy import Column, DateTime, String, BigInteger

from base_class.base_scraper import DailyScraper
from models.base import Base
from util.retry import retry

class StockIIScraper(DailyScraper):

    def __init__(self, date):
        super().__init__(date)
        self._prefixes = ["fii_non_dealer_", "fii_dealer_", "trust_", "dealer_self_", "dealer_hedge_"]

        suffixes = ['volume', 'turnover']
        data_cols = [f"{prefix}{suffix}" for suffix in suffixes for prefix in self._prefixes]

        self._columns = pd.Index(['date','stock_id'] + data_cols)

    @staticmethod
    def _rename_to_suffix_style(col: str) -> str:
        suffixes = {'買進股數': 'buy', '賣出股數': 'sell', '買賣超股數': 'volume'}
        for pattern, suffix in suffixes.items():
            if pattern in col:
                return col.replace(pattern, '') + suffix
        return col

    def _parse_df(self, df: pd.DataFrame):
        num_cols = [col for col in df.columns if col != 'stock_id' and col.isascii()]
        df[num_cols] = self.parse_comma(df[num_cols])
        df[num_cols] = self.to_numeric(df[num_cols]).astype(int)
        for prefix in self._prefixes:
            df[f'{prefix}turnover'] = df[f'{prefix}buy'] + df[f'{prefix}sell']
        df['date'] = self.date
        return df[self._columns]
    
    def _scrape_twse(self):
        response = self.session.get("https://www.twse.com.tw/rwd/zh/fund/T86",
                                    params={"date":self.date.strftime("%Y%m%d"),
                                            "selectType":"ALLBUT0999",
                                            "response":"json"})
        table = response.json()
        df = pd.DataFrame(table['data'], columns=table['fields'])
        df.columns = [self._rename_to_suffix_style(col) for col in df.columns]
        df.columns = (df.columns
                    .str.replace('證券代號', 'stock_id')
                    .str.replace('外陸資\\(不含外資自營商\\)', 'fii_non_dealer_', regex=True)
                    .str.replace('外資自營商', 'fii_dealer_', regex=True)
                    .str.replace('投信', 'trust_', regex=True)
                    .str.replace('自營商\\(自行買賣\\)', 'dealer_self_', regex=True)
                    .str.replace('自營商\\(避險\\)', 'dealer_hedge_', regex=True))

        return df

    def _scrape_tpex(self):
        response = self.session.post("https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade",
                                     data={"type":"Daily", 
                                           "sect":"EW",
                                           "date":self.date.strftime("%Y/%m/%d"),
                                           "response":"json"})
        table = response.json()['tables'][0]
        df = pd.DataFrame(table['data'], columns=table['fields'])
        col_prefixes =  pd.Index(
            ["", ""] +
            ["fii_non_dealer_"] * 3 + ["fii_dealer_"] * 3 + ["外資及陸資_"] * 3 +
            ["trust_"] * 3 + ["dealer_self_"] * 3 + ["dealer_hedge_"] * 3 +
            ["自營商_"] * 3 + [""]
        )
        df.columns = col_prefixes + df.columns
        df.columns = [self._rename_to_suffix_style(col) for col in df.columns]
        df.columns = df.columns.str.replace('代號', 'stock_id')
        
        return df

    @retry()
    def run(self):
        dfs = [self._scrape_twse(), self._scrape_tpex()]
        df = pd.concat(dfs, join='inner', ignore_index=True)
        self.respect_rate_limit(delay=1)
        return self._parse_df(df)

class StockII(Base):
    __tablename__ = "stock_ii"
    _scraper = StockIIScraper

    date = Column(DateTime, primary_key=True, nullable=False)
    stock_id = Column(String(8), primary_key=True, nullable=False)

    fii_non_dealer_volume = Column(BigInteger, nullable=False)
    fii_dealer_volume = Column(BigInteger, nullable=False)
    trust_volume = Column(BigInteger, nullable=False)
    dealer_self_volume = Column(BigInteger, nullable=False)
    dealer_hedge_volume = Column(BigInteger, nullable=False)

    fii_non_dealer_turnover = Column(BigInteger, nullable=False)
    fii_dealer_turnover = Column(BigInteger, nullable=False)
    trust_turnover = Column(BigInteger, nullable=False)
    dealer_self_turnover = Column(BigInteger, nullable=False)
    dealer_hedge_turnover = Column(BigInteger, nullable=False)
