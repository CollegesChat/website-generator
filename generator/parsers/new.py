import re
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl
from wenjuanxing_parser.models import IP, BasicData


class NewBasicData(BasicData):
    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}('
            f'answer_date={self.answer_date!r}, '
            f'num={self.num!r})'
        )


def meta_extractor(df: pl.DataFrame, idx: Any) -> NewBasicData | None:
    row = df.row(idx, named=True)
    return NewBasicData(
        answer_date=datetime.strptime(
            str(row['提交答卷时间']),
            '%Y/%m/%d %H:%M:%S',
        ).replace(tzinfo=ZoneInfo('Asia/Shanghai')),
        num=int(row['序号']),
        time_used=timedelta(0),
        source='null',
        source_detail='null',
        ip=IP(address='127.0.0.1', location='null'),
    )


def qnum_extractor(col_name: str) -> int | None:
    # 同时支持问卷星导出的完整题目列名（如 ``2、你的学校是？``）
    # 和简化列名（如 ``Q2``）。
    match = re.match(r'^(?:[qQ])?(\d+)(?:[、.]|$)', col_name)
    return int(match.group(1)) if match else None
