from abc import ABC, abstractmethod
from typing import Iterator
from requests import Response, Session
from requests.exceptions import HTTPError
import pandas as pd
import random
import time

from config.settings import USER_AGENT
from util.proxy_utils import get_random_proxy

class BaseScraper(ABC):
    """
    Base class for all scrapers.
    All subclasses must implement `.run()` method.
    """
    @abstractmethod
    def run(self) -> pd.DataFrame:
        pass

    @property
    def session(self):
        if not hasattr(self, '_session'):
            s = Session()
            s.headers.update({"user-agent": USER_AGENT})
            self._session = s
        return self._session

    @property
    def proxy(self) -> dict | None:
        """Return a proxy dict, or None if not needed."""
        try:
            proxy = get_random_proxy()
            print(f'\nUsing Proxy {proxy}')
            return {"https": proxy}
        except Exception:
            return None

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
    
    @staticmethod
    def safe_concat(dfs: list[pd.DataFrame], col_names: list[str] = None) -> pd.DataFrame:
        valid_dfs = [df for df in dfs if not df.empty]
        
        if not valid_dfs:
            return pd.DataFrame(columns=col_names) if col_names else pd.DataFrame()
        else:
            df = pd.concat(valid_dfs, ignore_index=True)
            return df[col_names] if col_names else df


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

    @property
    def session(self):
        if not hasattr(self, '_session'):
            s = Session()
            s.headers.update({"user-agent": USER_AGENT})
            self._session = s
        return self._session

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

