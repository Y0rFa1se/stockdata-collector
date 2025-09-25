import FinanceDataReader as fdr

def nasdaq_symbols():
    symbols = fdr.StockListing("NASDAQ")

    return symbols

def nasdaq_price(symbol: str):
    df = fdr.DataReader(symbol, "2000")

    return df