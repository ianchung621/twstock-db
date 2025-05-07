import os
import yaml
import pandas as pd

# === User-Agent Header ===
USER_AGENT = os.getenv("USER_AGENT")

# === Database Settings ===
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

SQLALCHEMY_DATABASE_URL = f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
DB_DSN = f"dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT}"

# === Default Start Dates ===
DEFAULT_START_DATES = {
    "IndexPrice": pd.Timestamp.today().normalize() - pd.DateOffset(years=6), # full history: "1999-01-01"
    "StockPrice": pd.Timestamp("2004-02-11"), # twse full history: "2004-02-11", tpex full history: "2007-04-23"
    "BrokerTransaction": pd.Timestamp.today().normalize() - pd.DateOffset(years=5),
    "FutureLargeTrader": pd.Timestamp("2004-07-01"), # 自2008年12月17日起，本表於最後結算日所揭露之未沖銷部位不含當月份到期商品之數量; 自2022年11月9日起，本表臺股期貨(TX+MTX/4+TMF/20)之「週契約」統計對象包含所有未到期之週契約
    "StockCapReduction": pd.Timestamp("2011-01-01") # twse
}

# === Routine Configuration ===
with open("config/routine.yaml", "r") as f:
    ROUTINE_CONFIG = yaml.safe_load(f)