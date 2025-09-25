from modules import data_reader, db_updater
from dotenv import load_dotenv
from tqdm import tqdm
import os

def main():
    load_dotenv()

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    db = db_updater.DBUpdater(url, key)

    symbols = data_reader.nasdaq_symbols()
    db.upsert_symbols(symbols)

    for i in tqdm(range(len(symbols))):
        symbol = symbols.loc[i, "Symbol"]
        df = data_reader.nasdaq_price(symbol)
        db.upsert_price(symbol, df)

if __name__ == "__main__":
    main()
