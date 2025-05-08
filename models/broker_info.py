import pandas as pd
import re
from sqlalchemy import Column, String

from base_class.base_scraper import OneTimeScraper
from models.base import Base

class BrokerInfoScraper(OneTimeScraper):

    @staticmethod
    def parse_broker_id(broker_id_hash: str) -> str:
        if len(broker_id_hash) == 4:
            return broker_id_hash
        elif len(broker_id_hash) == 16:
            chars = ""
            for i in range(0, len(broker_id_hash), 4):
                chunk = broker_id_hash[i:i+4]
                hex_byte = chunk[-2:]  # take last 2 characters
                ascii_char = chr(int(hex_byte, 16))
                chars += ascii_char
            return chars
        else:
            raise ValueError(f"Unexpected broker hashing: {broker_id_hash}")
    
    def parse_group(self, text: str) -> pd.DataFrame:
        columns = ["broker_id","broker_name","broker_group_query_str","broker_query_str"]
        brokers = text.split('!')
        df = pd.DataFrame([b.split(',') for b in brokers], columns = ["broker_query_str", "broker_name"])
        df["broker_group_query_str"] = df["broker_query_str"].iloc[0]
        df['broker_id'] = df['broker_query_str'].apply(self.parse_broker_id)
        return df[columns]
    
    def run(self):
        response = self.session.get('https://fubon-ebrokerdj.fbs.com.tw/z/js/zbrokerjs.djjs')
        raw_str = response.content.decode('big5').splitlines()[0] # raw broker list str in js code
        raw_str = raw_str.replace("(牛牛牛)","犇")
        raw_str = re.search(r"var g_BrokerList = '(.*?)';",raw_str, re.DOTALL).group(1)
        groups = raw_str.split(';')
        df = pd.concat([self.parse_group(group) for group in groups]).drop_duplicates("broker_id",ignore_index = True)
        return df

class BrokerInfo(Base):
    __tablename__ = 'broker_info'
    _scraper = BrokerInfoScraper

    broker_id = Column(String(4), primary_key=True, nullable=False)
    broker_name = Column(String(16), nullable=False)
    broker_group_query_str = Column(String(4), nullable=False)
    broker_query_str = Column(String(16), nullable=False)
    