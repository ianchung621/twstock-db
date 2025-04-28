# Taiwan Stock ETL Pipeline

An ETL project that scrapes, processes, and stores Taiwan stock market data into a PostgreSQL database.

---

## Requirements

- **Python 3.12**  
  ⚠️ **Python 3.13 is NOT supported** (due to SSL certificate verification issues with `requests` module).
- PostgreSQL
- Install Python dependencies via `requirements.txt`.

---

## Setup Instructions

### 1. Create and activate environment
```bash
conda create -n twstock-db python=3.12
conda activate twstock-db
```
### 2. Install dependencies
```bash
pip install -r requirements.txt
```
### 3. Create a `.env` file at the project root
Example `.env` file content:
```bash
PYTHONPATH=/path/to/your/workspace
USER_AGENT="your-user-agent-string"

DB_NAME="twstock_db"
DB_USER="postgres"
DB_PASSWORD="your_password_here"
DB_PORT="5432"
DB_HOST="localhost"
```
### 4. Create tables and run ETL tasks
```bash
python main.py
```

