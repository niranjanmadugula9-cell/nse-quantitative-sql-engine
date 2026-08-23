import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text

DB_USER = "postgres"
DB_PASS = " "  
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "nse_analytics"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)


stocks = [
    {
        "symbol": "RELIANCE",
        "company_name": "Reliance Industries Ltd",
        "sector": "Energy",
        "industry": "Oil & Gas",
        "market_cap_category": "Large Cap"
    },
    {
        "symbol": "TCS",
        "company_name": "Tata Consultancy Services Ltd",
        "sector": "Information Technology",
        "industry": "IT Services",
        "market_cap_category": "Large Cap"
    },
    {
        "symbol": "HDFCBANK",
        "company_name": "HDFC Bank Ltd",
        "sector": "Financial Services",
        "industry": "Private Bank",
        "market_cap_category": "Large Cap"
    },
    {
        "symbol": "INFY",
        "company_name": "Infosys Ltd",
        "sector": "Information Technology",
        "industry": "IT Services",
        "market_cap_category": "Large Cap"
    },
    {
        "symbol": "ICICIBANK",
        "company_name": "ICICI Bank Ltd",
        "sector": "Financial Services",
        "industry": "Private Bank",
        "market_cap_category": "Large Cap"
    },
    {
        "symbol": "LT",
        "company_name": "Larsen & Toubro Ltd",
        "sector": "Industrials",
        "industry": "Construction",
        "market_cap_category": "Large Cap"
    },
    {
        "symbol": "ITC",
        "company_name": "ITC Ltd",
        "sector": "Consumer Goods",
        "industry": "FMCG",
        "market_cap_category": "Large Cap"
    }
]


def run_pipeline():
    print("--------------------------------------------------")
    print("Starting NSE Data Ingestion Pipeline...")
    print("--------------------------------------------------")

    # Step A: Populate Dimension Table (dim_stock)
    print("Step 1/2: Populating dim_stock metadata...")
    df_meta = pd.DataFrame(stocks)
    
    with engine.begin() as conn:
        for _, row in df_meta.iterrows():
            insert_query = text("""
                INSERT INTO dim_stock (symbol, company_name, sector, industry, market_cap_category)
                VALUES (:symbol, :company_name, :sector, :industry, :market_cap_category)
                ON CONFLICT (symbol) DO UPDATE 
                SET company_name = EXCLUDED.company_name,
                    sector = EXCLUDED.sector,
                    industry = EXCLUDED.industry,
                    market_cap_category = EXCLUDED.market_cap_category;
            """)
            conn.execute(insert_query, row.to_dict())
            
    print("Metadata populated/updated successfully.\n")

    print("Step 2/2: Ingesting daily OHLCV historical price data...")
    
    for item in stocks:
        symbol = item["symbol"]
        ticker = f"{symbol}.NS" 

        # Fetch 3+ years of daily OHLCV data
        raw_df = yf.download(ticker, start="2022-01-01", progress=False)

        if raw_df.empty:
            print(f"[!] Warning: No data found for {ticker}. Skipping.")
            continue

        if isinstance(raw_df.columns, pd.MultiIndex):
            raw_df.columns = [col[0] for col in raw_df.columns]

        raw_df.reset_index(inplace=True)

        clean_df = pd.DataFrame({
            "symbol": symbol,
            "trade_date": pd.to_datetime(raw_df["Date"]).dt.date,
            "open_price": raw_df["Open"].round(2),
            "high_price": raw_df["High"].round(2),
            "low_price": raw_df["Low"].round(2),
            "close_price": raw_df["Close"].round(2),
            "volume": raw_df["Volume"].astype("int64")
        })

        clean_df.dropna(inplace=True)

        with engine.begin() as conn:
            clean_df.to_sql("temp_staging_prices", conn, if_exists="replace", index=False)
            
            upsert_query = text("""
                INSERT INTO fact_stock_daily (symbol, trade_date, open_price, high_price, low_price, close_price, volume)
                SELECT symbol, trade_date, open_price, high_price, low_price, close_price, volume
                FROM temp_staging_prices
                ON CONFLICT (symbol, trade_date) DO UPDATE 
                SET open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume;
                
                DROP TABLE IF EXISTS temp_staging_prices;
            """)
            conn.execute(upsert_query)

        print(f"[OK] Ingested {len(clean_df)} daily records for {symbol}.")

    print("\n--------------------------------------------------")
    print("ETL Ingestion Complete: All data stored in PostgreSQL.")
    print("--------------------------------------------------")

if __name__ == "__main__":
    run_pipeline()