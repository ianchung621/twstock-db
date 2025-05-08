import numpy as np
import pandas as pd
from io import StringIO
from sqlalchemy import Column, DateTime, String, REAL, BigInteger

from base_class.base_scraper import DailyScraper
from models.base import Base

class FutureLargeTraderScraper(DailyScraper):

    def __init__(self, date):
        super().__init__(date)
        self.columns = [
                        'date', 'contract_id', 'contract_name',
                        'buy_top5_volume','buy_top5_ratio', 'buy_top5_ii_ratio', 
                        'buy_top10_volume', 'buy_top10_ratio', 'buy_top10_ii_ratio',
                        'sell_top5_volume', 'sell_top5_ratio', 'sell_top5_ii_ratio',
                        'sell_top10_volume', 'sell_top10_ratio', 'sell_top10_ii_ratio',
                        'total_volume'
                    ]

    def get_contract_dict(self):
        response = self.session.get("https://www.taifex.com.tw/cht/3/getLargeTradersFutContract",
                                    params = {"queryDate":self.date.strftime("%Y/%m/%d")})
        contract_list = response.json()['contractList'] # {'idsort', 'topoi_com_name', 'topoi_kind_id'}
        return {contract['topoi_com_name'].strip(): contract['topoi_kind_id'].strip() for contract in contract_list}
    
    def run(self):
        response = self.session.post("https://www.taifex.com.tw/cht/3/largeTraderFutQry",
                                 data = {"queryDate":self.date.strftime("%Y/%m/%d"),
                                         "contractId": "all"})
        
        df = pd.read_html(StringIO(response.text))[0]
        df.columns = [
                    'contract_name', 'due_date',
                    'buy_top5_volume', 'buy_top5_ratio',
                    'buy_top10_volume', 'buy_top10_ratio',
                    'sell_top5_volume', 'sell_top5_ratio',
                    'sell_top10_volume', 'sell_top10_ratio',
                    'total_volume'
                    ]
        df = df[df['due_date'] == "所有 契約"].reset_index(drop=True)
        df['contract_name'] = df['contract_name'].str.split('(', n=1).str[0].str.strip()
        df['contract_id'] = df['contract_name'].map(self.get_contract_dict())
        vol_cols = ['buy_top5_volume', 'buy_top10_volume', 'sell_top5_volume', 'sell_top10_volume']
        df[vol_cols] = self.parse_comma(df[vol_cols])
        for col in vol_cols:

            extracted = df[col].str.replace(" ","").str.extract(r'([\d,]+)\s*\(([\d,]+)\)').astype(int)

            total_volume = extracted[0]
            institution_volume = extracted[1]
            
            df[col] = total_volume
            df[f'{col.removesuffix("_volume")}_ii_ratio'] = np.where(total_volume == 0, 0., institution_volume / total_volume)
        
        ratio_cols = ['buy_top5_ratio', 'buy_top10_ratio', 'sell_top5_ratio', 'sell_top10_ratio']

        for col in ratio_cols:

            df[col] = df[col].str.split('(', n=1).str[0]
            df[col] = df[col].str.replace('%', '').astype(float)/100
        
        df['date'] = self.date

        return df[self.columns].dropna(subset="contract_id", ignore_index=True)

class FutureLargeTrader(Base):
    __tablename__ = 'future_large_trader'
    _scraper = FutureLargeTraderScraper

    date = Column(DateTime, primary_key=True, nullable=False)
    contract_id = Column(String(8), primary_key=True, nullable=False)
    contract_name = Column(String(16), nullable=False)
    buy_top5_volume = Column(BigInteger, nullable=False)
    buy_top5_ratio = Column(REAL, nullable=False)
    buy_top5_ii_ratio = Column(REAL, nullable=False)
    buy_top10_volume = Column(BigInteger, nullable=False)
    buy_top10_ratio = Column(REAL, nullable=False)
    buy_top10_ii_ratio = Column(REAL, nullable=False)
    sell_top5_volume = Column(BigInteger, nullable=False)
    sell_top5_ratio = Column(REAL, nullable=False)
    sell_top5_ii_ratio = Column(REAL, nullable=False)
    sell_top10_volume = Column(BigInteger, nullable=False)
    sell_top10_ratio = Column(REAL, nullable=False)
    sell_top10_ii_ratio = Column(REAL, nullable=False)
    total_volume = Column(BigInteger, nullable=False)

