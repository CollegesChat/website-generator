import re
from datetime import datetime, timedelta
from typing import Any

import polars as pl
from wenjuanxing_parser.models import IP, BasicData


class NewBasicData(BasicData):
    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"answer_date={self.answer_date!r}, "
            f"num={self.num!r})"
        )


def meta_extractor(df: pl.DataFrame, idx: Any) -> NewBasicData | None:
    row = df.row(idx, named=True)
    return NewBasicData(
        answer_date=datetime.fromisoformat(str(row["开始时间"])),
        num=int(row["答题序号"]),
        time_used=timedelta(0),
        source="null",
        source_detail="null",
        ip=IP(address="127.0.0.1", location="null"),
    )


def qnum_extractor(col_name: str) -> int | None:
    match = re.match(r"^[qQ](\d+)", col_name)
    return int(match.group(1)) if match else None
