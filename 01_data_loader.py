import pandas as pd


def load_universe() -> pd.DataFrame:
    """
    Reads the global universe built by 01_macro_and_universe.py,
    deduplicates, and saves a clean ticker list to data_loaded.csv.
    Returns a DataFrame with columns: ticker, index.
    """
    df = pd.read_csv("global_universe.csv")

    if df.empty:
        raise RuntimeError(
            "global_universe.csv is empty — run 01_macro_and_universe.py first."
        )

    df.drop_duplicates(subset="ticker", keep="first", inplace=True)
    df.sort_values("ticker", inplace=True)
    df.reset_index(drop=True, inplace=True)

    df.to_csv("data_loaded.csv", index=False)
    return df


if __name__ == "__main__":
    try:
        universe = load_universe()
        print(f"Total unique tickers loaded: {len(universe)}")
        print(f"Indices represented: {universe['index'].unique().tolist()}")
        print(f"First 20 tickers: {universe['ticker'].head(20).tolist()}")
    except Exception as e:
        print(f"Error loading universe: {e}")
