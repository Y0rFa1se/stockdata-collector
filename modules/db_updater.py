from supabase import create_client, Client
from tqdm import tqdm
import numpy as np

class DBUpdater:
    def __init__(self, url: str, key: str):
        self.supabase: Client = create_client(url, key)

    def upsert_symbols(self, symbols):
        stack = []
        for i in tqdm(range(len(symbols))):
            stack.append({
                "Symbol": symbols.loc[i, "Symbol"],
                "Name": symbols.loc[i, "Name"],
                "IndustryCode": symbols.loc[i, "IndustryCode"],
                "Industry": symbols.loc[i, "Industry"]
            })

        response = (
            self.supabase.table("NASDAQ_List")
            .upsert(stack)
            .execute()
        )

        return response

    def upsert_price(self, symbol: str, df):
        replace_values = ["", "-", "N/A", "null"]
        df = df.replace(replace_values, np.nan) 
        df = df.replace({np.nan: None})
        df["Volume"] = df["Volume"].astype(int)
        stack = []
        for row in df.itertuples():
            stack.append({
                "Symbol": symbol,
                "Date": row.Index.isoformat(),
                "Open": row.Open,
                "High": row.High,
                "Low": row.Low,
                "Close": row.Close,
                "Volume": row.Volume,
                "AdjClose": row._6
            })

        response = (
            self.supabase.table("NASDAQ_Price")
            .upsert(stack)
            .execute()
        )

        return response