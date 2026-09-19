"""把问卷星导出的 xlsx 预处理成 csv：丢掉不需要的元数据列。

用法::

    uv run _preprocessor.py 答卷.xlsx [-o out.csv]
"""

import argparse
from pathlib import Path

import polars as pl

DROP_COLS = ("所用时间", "来源", "来源详情", "来自IP")


def count_rows(path: Path) -> int:
    """统计已有 csv 的数据行数，读不出来时按行数兜底。"""
    try:
        return pl.read_csv(path, truncate_ragged_lines=True).height
    except Exception:  # noqa: BLE001
        return max(len(path.read_text(encoding="utf-8").splitlines()) - 1, 0)


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("xlsx", type=Path, help="问卷星导出的 xlsx")
parser.add_argument("-o", "--output", type=Path, help="输出 csv，默认写到当前目录同名 .csv")
args = parser.parse_args()

df = pl.read_excel(args.xlsx).drop(*DROP_COLS, strict=False)
out = args.output or Path(args.xlsx.with_suffix(".csv").name)
before = count_rows(out) if out.is_file() else 0

out.parent.mkdir(parents=True, exist_ok=True)
df.write_csv(out)

msg = f"已写入 {out}：本次 {df.height} 行"
if before:
    msg += f"，较原有 {before} 行新增 {df.height - before} 行"
print(msg)
