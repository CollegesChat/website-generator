import re


def qnum_extractor(col_name: str) -> int | None:
    # 同时支持问卷星导出的完整题目列名（如 ``2、你的学校是？``）
    # 和简化列名（如 ``Q2``）。
    match = re.match(r'^(?:[qQ])?(\d+)(?:[、.]|$)', col_name)
    return int(match.group(1)) if match else None
