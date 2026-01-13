import argparse
from datetime import datetime, timedelta
import time

from src.models import DataSource
from src.ingestion.base import YahooFinanceSource
from src.database import MarketDataRepository, IngestionLogRepository


def run_ingestion(symbols: list[str], source: DataSource, days: int = 30) -> dict:
    """
    Run ingestion for given symbols and source.

    Args:
        symbols: List of ticker symbols to fetch.
        source: Data source to use.
        days: Number of days to look back.

    Returns:
        dict with keys: source, symbols_processed, total_rows_fetched,
                       total_rows_inserted, errors (list of dicts), duration
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    yahoo_source = YahooFinanceSource()
    market_data_repository = MarketDataRepository()
    ingestion_log_repository = IngestionLogRepository()

    # Track statistics
    overall_start_time = time.time()
    total_fetched = 0
    total_inserted = 0
    errors = []

    for i, symbol in enumerate(symbols):
        start_time = time.time()
        error_message = None
        rows_fetched = 0
        rows_inserted = 0
        status = "success"

        # Rate limit for Alpha Vantage: 12 seconds between calls
        if source == DataSource.ALPHA_VANTAGE and i > 0:
            print(f"  Rate limiting: waiting 12 seconds before next Alpha Vantage call...")
            time.sleep(12)

        try:
            print(f"Fetching {symbol} from {source.value} ({start_date.date()} to {end_date.date()})...")
            ohlcv_records = yahoo_source.fetch_ohlcv(
                symbol=symbol,
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
                symbol=symbol,
                status=status,
                rows_fetched=rows_fetched,
                rows_inserted=rows_inserted,
                duration_ms=duration_ms,
                error_message=error_message
            )
            print(f"  Logged run: {status} (duration: {duration_ms}ms)\n")

            # Update totals
            total_fetched += rows_fetched
            total_inserted += rows_inserted

            if error_message:
                errors.append({
                    "symbol": symbol,
                    "error": error_message,
                    "status": status
                })

    overall_duration = time.time() - overall_start_time

    return {
        "source": source.value,
        "symbols_processed": len(symbols),
        "total_rows_fetched": total_fetched,
        "total_rows_inserted": total_inserted,
        "errors": errors,
        "duration": overall_duration,
        "start_date": start_date.date(),
        "end_date": end_date.date(),
    }


def main():
    """Parse arguments and run ingestion."""
    parser = argparse.ArgumentParser(description="Ingest market data from external sources")

    parser.add_argument("--symbols", help="Comma-separated ticker symbols", type=str, required=True)
    parser.add_argument("--source", help="Financial data source", type=str,
                       choices=[s.value for s in DataSource], required=True)
    parser.add_argument("--days", help="Range of days to ingest", type=int, default=30)

    args = parser.parse_args()

    # Parse symbols
    symbol_list = [s.strip() for s in args.symbols.split(",")]
    source = DataSource(args.source)

    # Run ingestion
    result = run_ingestion(symbol_list, source, args.days)

    # Print formatted summary
    summary = f"""{'=' * 70}
INGESTION SUMMARY
{'=' * 70}
Source:           {result['source']}
Date range:       {result['start_date']} to {result['end_date']}
Symbols:          {result['symbols_processed']} ({', '.join(symbol_list)})
Total duration:   {result['duration']:.2f}s

Rows fetched:     {result['total_rows_fetched']}
Rows inserted:    {result['total_rows_inserted']}
Duplicates:       {result['total_rows_fetched'] - result['total_rows_inserted']}

Errors:           {len(result['errors'])}
"""

    print(summary)

    if result['errors']:
        print("ERRORS:")
        for error in result['errors']:
            print(f"  - {error['symbol']}: {error['error']}")
        print("=" * 70)
    else:
        print("=" * 70)


if __name__ == "__main__":
    main()
