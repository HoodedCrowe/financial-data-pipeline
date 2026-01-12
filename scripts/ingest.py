import argparse
from enum import Enum
from src.models import DataSource
from src.ingestion.base import YahooFinanceSource
from src.database import MarketDataRepository
from src.database import IngestionLogRepository
from datetime import datetime, timedelta

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

for s in symbol_list:
    ohlcv_records = yahoo_source.fetch_ohlcv(symbol=s, start_date=start_date, end_date=end_date)
    market_data_repository.insert_records(records=ohlcv_records)
    ingestion_log_repository.log_run(source=source, symbol=s)
    break
