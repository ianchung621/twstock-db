from abc import ABC, abstractmethod
from typing import Iterator
from requests import Response
from requests.exceptions import HTTPError
import pandas as pd
import random
import time


class BaseScraper(ABC):
    """
    Base class for all scrapers.
    All subclasses must implement `.run()` method.
    """
    @abstractmethod
    def run(self) -> pd.DataFrame:
        pass

    @staticmethod
    def validate_response(response: Response):
        if response.status_code != 200:
            param_str = response.request.url.split('?', 1)[-1]
            param_str = "{" + param_str.replace("&",", ").replace("=",": ") + "}"
            raise HTTPError(f'request of {param_str} fails, status code: {response.status_code}, url: {response.url}')
    
    @staticmethod
    def parse_comma(df: pd.DataFrame):
        return df.astype(str).apply(lambda col: col.str.replace(",", ""))
    
    @staticmethod
    def to_numeric(df: pd.DataFrame):
        return df.apply(pd.to_numeric, errors='coerce')
    
    @staticmethod
    def respect_rate_limit(delay = 2., random_wait = True):
        if random_wait:
            time.sleep(delay + 0.1*random.randint(1,10))
        else:
            time.sleep(delay)



class OneTimeScraper(BaseScraper):
    """
    For scrapers that require no input and return one static DataFrame.
    """
    def __init__(self):
        pass


class DailyScraper(BaseScraper):
    """
    For scrapers that take a single `date` (typically daily).
    """
    def __init__(self, date: pd.Timestamp):
        self.date = date


class PeriodicScraper(BaseScraper):
    """
    For scrapers that take a `start_date` and return a batch (e.g. quarterly or monthly dump).
    """
    def __init__(self, start_date: pd.Timestamp):
        self.start_date = start_date

class BaseChunkScraper(ABC):
    """
    General base class for scrapers that yield multiple DataFrame chunks based on a chunking key.
    Each yielded DataFrame will be processed/inserted immediately.
    
    Typical chunking keys: dates, stock ids, sectors, etc.
    """
    def __init__(self, start_date: pd.Timestamp):
        self.start_date = start_date

    @abstractmethod
    def run(self) -> Iterator[pd.DataFrame]:
        """
        Should yield one DataFrame per logical chunk.
        """
        pass

    @staticmethod
    def validate_response(response: Response):
        if response.status_code != 200:
            param_str = response.request.url.split('?', 1)[-1]
            param_str = "{" + param_str.replace("&", ", ").replace("=", ": ") + "}"
            raise HTTPError(f'request of {param_str} fails, status code: {response.status_code}, url: {response.url}')
    
    @staticmethod
    def parse_comma(df: pd.DataFrame):
        return df.astype(str).apply(lambda col: col.str.replace(",", ""))
    
    @staticmethod
    def to_numeric(df: pd.DataFrame):
        return df.apply(pd.to_numeric, errors='coerce')
    
    @staticmethod
    def respect_rate_limit(delay=2.0, random_wait=True):
        if random_wait:
            time.sleep(delay + 0.1 * random.randint(1, 10))
        else:
            time.sleep(delay)


class DateChunkScraper(BaseChunkScraper):
    """
    For scrapers that iterate over dates and yield DataFrame chunks.
    """
    def __init__(self, start_date: pd.Timestamp):
        super().__init__(start_date)


class StockChunkScraper(BaseChunkScraper):
    """
    For scrapers that iterate over stock_ids and yield DataFrame chunks.
    """
    def __init__(self, start_date: pd.Timestamp, stock_ids: list[str] = None):
        super().__init__(start_date)
        self.stock_ids = stock_ids

