import pandas as pd
import requests
from sqlalchemy import Column, DateTime, String, BigInteger
from io import StringIO
from concurrent.futures import ThreadPoolExecutor

from config.settings import USER_AGENT
from base_class.base_scraper import DailyScraper
from models.broker_info import BrokerInfoScraper
from models.base import Base


class BrokerTransactionScraper(DailyScraper):

    def __init__(self, date: pd.Timestamp):
        super().__init__(date)
        self.broker_info = BrokerInfoScraper().run().set_index("broker_id")
        self.date_str = self.date.strftime("%Y-%m-%d")
        self.__columns = ["date","stock_id","broker_id","volume","turnover"]

        self.session = requests.Session()
        self.session.headers.update({"user-agent": USER_AGENT})
    
    def _scrape_broker(self, broker_id: str):
        response = self.session.get("https://fubon-ebrokerdj.fbs.com.tw/z/zg/zgb/zgb0.djhtm",
                                params={
                                    "a":self.broker_info.loc[broker_id, "broker_group_query_str"],
                                    "b":self.broker_info.loc[broker_id, "broker_query_str"],
                                    "c":"B",
                                    "e":self.date_str,
                                    "f":self.date_str
                                })
        self.validate_response(response)
        
        try:
            dfs = pd.read_html(StringIO(response.text), extract_links="all", attrs={"class":"t0"}, skiprows = 1)
            df_buy = dfs[0].drop(0)
            df_sell = dfs[1].drop(0)
            df = pd.concat([df_buy, df_sell.iloc[::-1]], ignore_index=True)
            no_data_flag = ("無此券商分點交易資料", None)
            if df.iloc[0, 0] == no_data_flag:
                df = pd.DataFrame(columns = range(4))
            df["broker_id"] = broker_id
            return df
        
        except Exception as e:
            print(f"Extraction Error occurs at {response.url}")
            print(e)
            return pd.DataFrame(columns=self.__columns)
    
    def parse_df(self, df: pd.DataFrame):
        df.columns = ["stk_link", "buy","sell","volume","broker_id"]
        df['volume'] = df['volume'].str[0]
        df['buy'] = df['buy'].str[0]
        df['sell'] = df['sell'].str[0]
        stk_0 = df['stk_link'].str[0]
        stk_1 = df['stk_link'].str[1]
        etf_ids = stk_1.str.extract(r"Link2Stk\('(\w+)'\)")[0] # regex for ETFs: javascript:Link2Stk('0050');
        stock_ids = stk_0.str.extract(r"GenLink2stk\('AS(\d+)'")[0] # regex for stocks: GenLink2stk('AS2330','台積電');
        df["stock_id"] = etf_ids.combine_first(stock_ids)
        df = self.parse_comma(df)
        df[["buy","sell","volume"]] = self.to_numeric(df[["buy","sell","volume"]]).astype(int)
        df['turnover'] = df['buy'] + df['sell']
        df['date'] = self.date
        return df[self.__columns]
    
    def run(self):
        with ThreadPoolExecutor() as executor:
            dfs = list(executor.map(self._scrape_broker, self.broker_info.index))
        result = pd.concat([df for df in dfs if not df.empty], ignore_index=True)
        return self.parse_df(result)


class BrokerTransaction(Base):
    __tablename__ = 'broker_transaction'
    _scraper = BrokerTransactionScraper

    date = Column(DateTime, primary_key=True, nullable=False)
    stock_id = Column(String(8), primary_key=True, nullable=False)
    broker_id = Column(String(4), primary_key=True, nullable=False)
    volume = Column(BigInteger, nullable=False)
    turnover = Column(BigInteger, nullable=False)