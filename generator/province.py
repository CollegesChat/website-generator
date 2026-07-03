import re
from io import BytesIO

import polars as pl

NORMAL_NAME_MATCHER = re.compile(r"大学|学院|学校")


def load_province_mapping(csv_bytes: bytes) -> list[tuple[str, str]]:
    """返回按学校名长度降序排列的 (学校名, 省份) 列表，确保长名优先匹配"""
    df = pl.read_csv(
        BytesIO(csv_bytes), has_header=False, new_columns=["province", "name"]
    )
    pairs = list(zip(df["name"].to_list(), df["province"].to_list()))
    pairs.sort(key=lambda p: len(p[0]), reverse=True)
    return pairs


def find_province(name: str, mapping: list[tuple[str, str]]) -> str:
    for key, prov in mapping:
        if key in name:
            return prov
    if not NORMAL_NAME_MATCHER.search(name):
        return "国外"
    return "其他"
