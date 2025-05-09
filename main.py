import argparse
from config.settings import ROUTINE_CONFIG
from util.db_utils import table_has_data
from tasks import run_task, create_tables
from models import get_model
from base_class.base_scraper import OneTimeScraper

def main():
    available_routines = list(ROUTINE_CONFIG.keys())
    all_models = ROUTINE_CONFIG.get("all", [])

    parser = argparse.ArgumentParser(description="Run scraping tasks")

    parser.add_argument(
        "-r", "--routine",
        type=str,
        default="all",
        help=f"Routine section to use from config/routine.yaml (available: {', '.join(available_routines)})"
    )

    parser.add_argument(
        "-i", "--include-onetime",
        action="store_true",
        help="Include OneTimeScraper tasks (default: skip them if table has data)"
    )

    parser.add_argument(
        "-m", "--model",
        type=str,
        help=f"Run a specific model by name (available: {', '.join(all_models)})"
    )

    args = parser.parse_args()

    create_tables()

    if args.model:
        model = get_model(args.model)
        run_task(model)
        return

    routine_key = args.routine
    if routine_key not in ROUTINE_CONFIG:
        raise ValueError(f"Routine '{routine_key}' not found in routine.yaml")

    for model_name in ROUTINE_CONFIG[routine_key]:
        model = get_model(model_name)

        if (not args.include_onetime
            and issubclass(model._scraper, OneTimeScraper)
            and table_has_data(model.__tablename__)
            ):
            print(f"Skipping OneTimeScraper (already has data): {model_name}")
            continue

        run_task(model)

if __name__ == "__main__":
    main()