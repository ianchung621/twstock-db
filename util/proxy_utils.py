import random
import requests
import pandas as pd
from io import StringIO
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

_working_proxies: list[str] = []

def load_proxies() -> list[str]:
    response = requests.get('https://www.sslproxies.org/')
    df = pd.read_html(StringIO(response.text), attrs={"class":"table table-striped table-bordered"})[0]
    return (df['IP Address'].astype(str) + ":" + df['Port'].astype(str)).tolist()

def test_proxy(proxy: str, test_url: str) -> bool:
    try:
        response = requests.get(test_url, proxies={"https": proxy})
        if response.status_code == 200:
            return proxy
    except Exception:
        return None

def get_working_proxies(test_url: str = "http://httpbin.org/ip") -> list[str]:
    proxy_list = load_proxies()
    with ThreadPoolExecutor() as executor:
        results = list(tqdm(executor.map(lambda px: test_proxy(px, test_url), proxy_list), total=len(proxy_list), desc="Testing proxies"))
    working = [proxy for proxy in results if proxy is not None]
    global _working_proxies
    _working_proxies = working
    return working

def get_random_proxy() -> str:
    global _working_proxies
    if not _working_proxies:
        _working_proxies = get_working_proxies()
    return random.choice(_working_proxies)

if __name__ == "__main__":
    print(load_proxies())
    print(f"working: {len(get_working_proxies(test_url='wwwc.twse.com.tw'))}")