import argparse
from enum import Enum
from src.models import DataSource
from src.ingestion.base import YahooFinanceSource
from src.database import MarketDataRepository
from src.database import IngestionLogRepository
from datetime import datetime, timedelta
import time

parser = argparse.ArgumentParser("Ingest")

parser.add_argument("--symbols", help="Ticker symbols.", type=str)
parser.add_argument("--source", help="Financial data source.", type=DataSource, choices=[s.value for s in DataSource])
parser.add_argument("--days", help="Range of days the data should be ingested for.", type=int)

args = parser.parse_args()

symbols: str = args.symbols
source: DataSource = args.source
days: int = args.days

symbol_list = symbols.split(",")

end_date = datetime.now()
start_date = end_date - timedelta(days=days)

yahoo_source = YahooFinanceSource()
market_data_repository = MarketDataRepository()
ingestion_log_repository = IngestionLogRepository()

loop_count = 0
for s in symbol_list:
    start_time = time.time()
    error_message = None
    rows_fetched = 0
    rows_inserted = 0
    status = "success"

    # Rate limit for Alpha Vantage: 12 seconds between calls
    if source == DataSource.ALPHA_VANTAGE and loop_count > 0:
        print(f"  Rate limiting: waiting 12 seconds before next Alpha Vantage call...")
        time.sleep(12)

    loop_count += 1

    try:
        print(f"Fetching {s} from {source.value} ({start_date.date()} to {end_date.date()})...")
        ohlcv_records = yahoo_source.fetch_ohlcv(
            symbol=s,
            start_date=start_date,
            end_date=end_date
        )
        rows_fetched = len(ohlcv_records)
        print(f"  Fetched {rows_fetched} records")

        # Insert data
        rows_attempted, rows_inserted = market_data_repository.insert_records(
            records=ohlcv_records
        )
        print(f"  Inserted {rows_inserted}/{rows_attempted} records (duplicates skipped: {rows_attempted - rows_inserted})")

        # Determine status
        if rows_inserted == 0 and rows_fetched > 0:
            status = "partial"
        elif rows_fetched == 0:
            status = "failed"
            error_message = "No data fetched from source"
    except Exception as e:
        status = "failed"
        error_message = str(e)
        print(f"  ERROR: {error_message}")
    finally:
        # Always log the run
        duration_ms = int((time.time() - start_time) * 1000)
        ingestion_log_repository.log_run(
            source=source,
            symbol = s,
            status=status,
            rows_fetched=rows_fetched,
            rows_inserted=rows_inserted,
            duration_ms=duration_ms,
            error_message=error_message
        )
        print(f"  Logged run: {status} (duration: {duration_ms}ms)\n")
