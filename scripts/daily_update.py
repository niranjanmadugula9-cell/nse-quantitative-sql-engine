import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# ==========================================
# 1. DATABASE CONFIGURATION
# ==========================================
DB_USER = "postgres"
DB_PASS = "niranjan" 
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "nse_analytics"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# ==========================================
# 2. INCREMENTAL SYNC LOGIC
# ==========================================
def update_daily_market_data():
    print("--------------------------------------------------")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting Incremental Data Sync...")
    print("--------------------------------------------------")

    # Fetch symbols currently registered in dim_stock
    with engine.connect() as conn:
        result = conn.execute(text("SELECT symbol FROM dim_stock;"))
        symbols = [row[0] for row in result.fetchall()]

    if not symbols:
        print("[!] No symbols found in dim_stock. Run ingest_nse.py first.")
        return

    # Look back 7 days to cover weekends and holidays safely
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    print(f"Syncing records from {start_date} to today for: {', '.join(symbols)}\n")

    for symbol in symbols:
        # Format index tickers vs regular stock tickers
        ticker = symbol if symbol.startswith("^") else f"{symbol}.NS"
        
        df = yf.download(ticker, start=start_date, progress=False)

        if df.empty:
            print(f"[!] No new records for {ticker}")
            continue

        # Flatten multi-level headers if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]

        df.reset_index(inplace=True)

        clean_df = pd.DataFrame({
            "symbol": symbol,
            "trade_date": pd.to_datetime(df["Date"]).dt.date,
            "open_price": df["Open"].round(2),
            "high_price": df["High"].round(2),
            "low_price": df["Low"].round(2),
            "close_price": df["Close"].round(2),
            "volume": df["Volume"].astype("int64")
        }).dropna()

        # Idempotent Upsert into PostgreSQL
        with engine.begin() as conn:
            clean_df.to_sql("temp_daily_staging", conn, if_exists="replace", index=False)
            upsert_query = text("""
                INSERT INTO fact_stock_daily (symbol, trade_date, open_price, high_price, low_price, close_price, volume)
                SELECT symbol, trade_date, open_price, high_price, low_price, close_price, volume
                FROM temp_daily_staging
                ON CONFLICT (symbol, trade_date) DO UPDATE 
                SET open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume;

                DROP TABLE IF EXISTS temp_daily_staging;
            """)
            conn.execute(upsert_query)

        print(f"[OK] Successfully synced latest candles for {symbol}")

    print("\n--------------------------------------------------")
    print("Incremental Sync Complete.")
    print("--------------------------------------------------")

if __name__ == "__main__":
    update_daily_market_data()