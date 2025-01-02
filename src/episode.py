from dataclasses import dataclass
from datetime import time as dt_time
from datetime import timedelta
from pathlib import Path
import re
import pandas as pd


HERE = Path(__file__).parent
DATA_DIR = HERE.parent / "data"
STORE_DIR = HERE.parent / "store"
divider_time = timedelta(minutes=5)
RE_PODCAST = re.compile(r"[_-](\d+)[_-]")


@dataclass
class SplitedText:
    part: int
    start: timedelta
    end: timedelta
    text: str


@dataclass
class Episode:
    id_: int
    title: str | None
    texts: list[SplitedText]


def str_to_timedelta(s: str) -> timedelta:
    t = dt_time.fromisoformat(s)
    return timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)


def make_episode(id_: int, title: str, srt_filename: str) -> Episode:
    episode = Episode(
        id_=id_,
        title=title,
        texts=[]
    )
    part = 1
    start = None
    end = None
    text = None

    with open(srt_filename) as f:
        for line in f:
            first = None
            second = None
            line_text = None
            if line.strip().isdigit():
                continue
            elif line.strip() == "":
                continue
            elif "-->" in line:
                first_str, second_str = line.strip().split("-->")
                first = str_to_timedelta(first_str.strip())
                second = str_to_timedelta(second_str.strip())
            else:
                line_text = line.strip()
            
            if first:
                if start is None:
                    start = first
            if line_text:
                if text is None:
                    text = line_text
                else:
                    text += "\n" + line_text

            if start and second and text:
                if abs(second - start) > divider_time:
                    end = second
                    st = SplitedText(part=part, start=start, end=end, text=text)
                    episode.texts.append(st)

                    # print(text)
                    part += 1
                    start = None
                    text = None
        # print(episode)
        print(len(episode.texts))
    return episode


def make_df(episode: Episode) -> pd.DataFrame:
    data = []
    for text in episode.texts:
        data.append([episode.id_, text.part, text.start, text.end, text.text])
    df = pd.DataFrame(data, columns=["id", "part", "start", "end_", "text"])
    return df


def get_srt_files():
    lst = []
    for file_path in DATA_DIR.glob("*.srt"):
        m = RE_PODCAST.search(file_path.name)
        if m is not None:
            filename = file_path.name
            id_ = int(m.group(1))
            lst.append({"id": id_, "srt": filename})
    return lst


def main():
    lst = sorted(get_srt_files(), key=lambda x: x["id"])
    print(f"{len(lst)=}")
    for item in lst:
        print(item["id"])
        episode = make_episode(item["id"], item.get("title"), DATA_DIR / item["srt"])
        df = make_df(episode)
        # print(df)
        df.to_parquet(STORE_DIR / f"podcast-{item['id']}.parquet")
        # break


if __name__ == "__main__":
    main()
