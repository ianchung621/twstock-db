# Taiwan Stock ETL Pipeline

An ETL project that scrapes, processes, and stores Taiwan stock market data into a PostgreSQL database.


---

## Schema Diagram

[View full diagram](docs/schema_diagram.md)

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

### 5. Command Line Options

You can customize which models to run using the following options:

| Flag | Description |
|------|-------------|
| `-r`, `--routine` | Run a predefined group of models from `config/routine.yaml`. <br>Example values: `all`, `standard`, `daily`, `onetime`, `transformation`, etc. <br>**Default**: `all` |
| `-i`, `--include-onetime` | Include `OneTimeScraper` models (e.g. `BrokerInfo`, `ContractInfo`). <br>By default, these are skipped if the corresponding table has existing data. |
| `-m`, `--model` | Run a specific model by name (e.g. `StockPrice`, `IndexPrice`). <br>This bypasses the `--routine` setting. |

#### 🔧 Examples:

Run models listed under the `standard` routine (commonly used when you only need `AdjustedPrice`, including its dependencies like `StockPrice`, `StockDividend`, `StockCapReduction`, etc):

```bash
python main.py --routine standard
```

Run the `BrokerInfo` model explicitly:

```bash
python main.py --model BrokerInfo
```

Run all models, including `OneTimeScraper` (if the corresponding table has existing data, this will overwrite the data):

```bash
python main.py --include-onetime
```

