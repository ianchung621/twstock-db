import pandas as pd
from io import StringIO
from sqlalchemy import Column, String

from base_class.base_scraper import OneTimeScraper
from models.base import Base

class ContractInfoScraper(OneTimeScraper):

    def __init__(self):
        super().__init__()
        self.columns = ["contract_id","stock_id","stock_name"]
    
    def run(self):
        response = self.session.get("https://www.taifex.com.tw/cht/2/stockLists")
        df = pd.read_html(StringIO(response.text))[1]
        df = df.rename(columns={"股票期貨、 選擇權 商品代碼":"contract_id",
                                "標的證券 簡稱":"stock_name",
                                "證券代號":"stock_id"})
        return df[self.columns].dropna(ignore_index=True)

class ContractInfo(Base):
    __tablename__ = 'contract_info'
    _scraper = ContractInfoScraper

    contract_id = Column(String(8), primary_key=True, nullable=False)
    stock_id = Column(String(8), nullable=True)
    stock_name = Column(String(16), nullable=True)

