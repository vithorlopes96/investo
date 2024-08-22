import pandas as pd
import yfinance as yf

def extract_closed_prices():
    try:
        stocks = ['CVX', 'NVDA', 'AAPL','TM','V','JPM']
        for i in stocks:
            msft = yf.Ticker(i)
            data_temp = msft.history(period="1y")
            df_fin  = pd.concat([df_fin,data_temp])
            
        df_fin = df_fin.reset_index()
        print(df_fin)
    except Exception as e:
        raise ValueError(f"Extraction failed: {e}")
    return df_fin