from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo
import pandas as pd


HERE = Path(__file__).parent
JST = ZoneInfo("Asia/Tokyo")


def parse_ja_date(dt: datetime) -> date:
    return dt.astimezone(JST).date()


def parse_csv(filename: str | Path) -> pd.DataFrame:
    df = pd.read_csv(filename,
                    header=None,
                    )
    df.columns = ["id", "day_import", "date_import", "length", "audio", "title_import"]
    df["datetime"] = pd.to_datetime(df["date_import"], format=" %d %b %Y %H:%M:%S %Z")
    df["date"] = df.loc[:, "datetime"].apply(lambda x: parse_ja_date(x))
    df["title"] = df.loc[:, "title_import"].astype("string")
    return df


def ignore_columns(df: pd.DataFrame, limit: str | None) -> pd.DataFrame:
    if limit is not None:
        df_out = df.loc[df.loc[:, "datetime"] > limit, ["id", "date", "length", "audio", "title"]]
    else:
        df_out = df.loc[:, ["id", "date", "length", "audio", "title"]]
    return df_out


def main(input: str | Path, output: str | Path, limit: str | None):
    df = parse_csv(input)
    df_out = ignore_columns(df, limit)
    df_out.to_parquet(output, index=False)


if __name__ == "__main__":
    input = HERE.parent / "data" / "episode-list-202501.csv"
    output = HERE.parent / "store" / "podcast-title-list-202301-202501.parquet"
    limit = "2023-01-01 00:00:00+9:00"
    main(input, output, limit)
