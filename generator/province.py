from io import BytesIO

import polars as pl
import regex as re

NORMAL_NAME_MATCHER = re.compile(r"^(?!.*(?:高中|初中)).*?(?:大学|学院|学校).*$")
NON_CHINESE_MATCHER = re.compile(r"^(?![{\p{P}\p{Zs}\p{Han}}]+$)[\p{L}\p{P}\p{Zs}]+$")
# use https://regex101.com/ for test.
# 匹配：外文+中文+標點（可選）
# 匹配：外文+標點（可選）
# 不匹配：中文+標點（可選）


def load_province_mapping(csv_bytes: bytes) -> list[tuple[str, str]]:
    """返回按学校名长度降序排列的 (学校名, 省份) 列表，确保长名优先匹配"""
    df = pl.read_csv(
        BytesIO(csv_bytes), has_header=False, new_columns=["province", "name"]
    )
    pairs = list(zip(df["name"].to_list(), df["province"].to_list(), strict=True))
    pairs.sort(key=lambda p: len(p[0]), reverse=True)
    return pairs


def find_province(name: str, mapping: list[tuple[str, str]]) -> str:
    for key, prov in mapping:
        if key in name:
            return prov
    if NON_CHINESE_MATCHER.search(name):
        return "国外"
    if not NORMAL_NAME_MATCHER.search(name):
        return "不予收录"
    return "国外"
