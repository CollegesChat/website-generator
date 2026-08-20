import re
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl
from wenjuanxing_parser.constants import MISSING_BASIC_DATA_KWARGS
from wenjuanxing_parser.models import BasicData


def legacy_meta_extractor(df: pl.DataFrame, idx: Any) -> BasicData | None:
    row = df.row(idx, named=True)
    return BasicData(
        answer_date=datetime.fromisoformat(str(row['开始时间'])),
        num=int(row['答题序号']),
        **MISSING_BASIC_DATA_KWARGS,
    )


def new_meta_extractor(df: pl.DataFrame, idx: Any) -> BasicData | None:
    row = df.row(idx, named=True)
    return BasicData(
        answer_date=datetime.strptime(
            str(row['提交答卷时间']),
            '%Y/%m/%d %H:%M:%S',
        ).replace(tzinfo=ZoneInfo('Asia/Shanghai')),
        num=int(row['序号']),
        **MISSING_BASIC_DATA_KWARGS,
    )


def qnum_extractor(col_name: str) -> int | None:
    match = re.match(r'^(?:[qQ])?(\d+)(?:[、.]|$)', col_name)
    return int(match.group(1)) if match else None


