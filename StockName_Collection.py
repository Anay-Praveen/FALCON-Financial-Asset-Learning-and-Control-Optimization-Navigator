import os
import time
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta


def main():
    # Create output directories if they don't exist.
    for folder in ["training", "validation", "testing"]:
        os.makedirs(folder, exist_ok=True)

    # Read stock symbols from "Stocks.csv" (expects a column "Symbol")
    df_stocks = pd.read_csv("Stocks.csv")
    symbols = df_stocks["Symbol"].dropna().unique().tolist()

    # List to hold mapping info: each record maps a sequential StockID to a Symbol.
    mapping_all = []

    # Sequential stock counter (1 for first valid stock, 2 for second, etc.)
    stock_counter = 1

    for symbol in symbols:
        print(f"Processing {symbol}...")
        # Use yesterday's date as the end date to avoid DST-related issues.
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        try:
            # Download full available daily historical data for the stock.
            df = yf.download(symbol, interval='1d', period="max", end=end_date)
        except Exception as e:
            print(f"  Failed to download data for {symbol}: {e}")
            #time.sleep(1)
            continue

        if df is None or df.empty:
            print(f"  {symbol} returned no data. Skipping.")
            #time.sleep(1)
            continue

        # Sort data by date (ascending)
        df.sort_index(inplace=True)

        # Calculate split indices: 60% training, next 20% validation, remaining 20% testing.
        n = len(df)
        train_end = int(0.6 * n)
        valid_end = int(0.8 * n)

        # Split the data.
        df_train = df.iloc[:train_end].copy()
        df_val = df.iloc[train_end:valid_end].copy()
        df_test = df.iloc[valid_end:].copy()

        # Add columns for traceability.
        df_train["Symbol"] = symbol
        df_val["Symbol"] = symbol
        df_test["Symbol"] = symbol

        df_train["StockID"] = stock_counter
        df_val["StockID"] = stock_counter
        df_test["StockID"] = stock_counter

        # Save individual CSV files into the corresponding folders.
        train_filename = os.path.join("training", f"{symbol}.csv")
        val_filename = os.path.join("validation", f"{symbol}.csv")
        test_filename = os.path.join("testing", f"{symbol}.csv")

        df_train.to_csv(train_filename, index=True)
        df_val.to_csv(val_filename, index=True)
        df_test.to_csv(test_filename, index=True)

        # Record the mapping information.
        mapping_all.append({
            "StockID": stock_counter,
            "Symbol": symbol
        })

        print(f"  {symbol}: {n} total rows → {len(df_train)} training, {len(df_val)} validation, {len(df_test)} testing rows.")

        stock_counter += 1

        # Wait 1 second before processing the next symbol.
        #time.sleep(1)

    # Save the mapping CSV file.
    if mapping_all:
        mapping_df = pd.DataFrame(mapping_all)
        mapping_df.to_csv("mapping.csv", index=False)
        print("\nMapping file saved as 'mapping.csv'.")
    else:
        print("No data was collected.")


if __name__ == "__main__":
    main()
