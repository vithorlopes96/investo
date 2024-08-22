import pandas as pd
import yfinance as yf
import boto3
from io import StringIO

class LambdaFunctions:

    def __init__(self, stocks=None):
        self.stocks = stocks if stocks else ['CVX', 'NVDA', 'AAPL', 'TM', 'V', 'JPM']
        self.df_fin = pd.DataFrame()

    def extract_closed_prices(self):
        try:
            for stock in self.stocks:
                data_temp = yf.download(stock, period="1y", interval='1h')
                data_temp['stock'] = stock
                self.df_fin = pd.concat([self.df_fin, data_temp])
                
            self.df_fin = self.df_fin.reset_index()
        except Exception as e:
            raise ValueError(f"Extraction failed: {e}")
        return self.df_fin

    def save_to_s3(self, bucket_name, file_name):
        csv_buffer = StringIO()
        self.df_fin.to_csv(csv_buffer, index=False)
        s3_resource = boto3.resource('s3')
        s3_resource.Object(bucket_name, file_name).put(Body=csv_buffer.getvalue())

def main():
    lambda_functions = LambdaFunctions()
    df_fin = lambda_functions.extract_closed_prices()
    bucket_name = 'your-s3-bucket-name'
    file_name = 'path/to/your-file.csv'
    lambda_functions.save_to_s3(bucket_name, file_name)
    print("File saved to S3 successfully.")

if __name__ == "__main__":
    main()