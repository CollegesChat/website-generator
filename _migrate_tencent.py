"""把腾讯文档「高校生活水平调查_数据详情表」的答卷迁移成 v1.csv 行。

源表比 v1 问卷多插了一道「入学时间」（源 Q5），所以源 Q6-Q30 整体对应 v1 Q5-Q29，
源 Q31（自由补充）对应 v1 Q30；源 Q32（对问卷的建议）在 v1 没有对应题，丢弃。

用法::

    uv run _migrate_tencent.py qq_sheet.csv [-o v1.additions.csv]
    uv run _migrate_tencent.py qq_sheet.xlsx --start-id 100000000

输入是源表格导出的 csv 或 xlsx（34 列，首行表头；无表头也能读）。
"""

import argparse
import math
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl

EXCEL_SUFFIXES = (".xlsx", ".xls")
CSV_SUFFIXES = (".csv", ".tsv")

# 源表格列下标（0 起）：0 序号 / 1-3 Q1-Q3 / 4 院校 / 5 入学时间 / 6-30 Q6-Q30 /
# 31 Q31 自由补充 / 32 Q32 问卷建议 / 33 提交时间（Excel 日期序列号）
COL_ID = 0
COL_Q1, COL_Q2, COL_Q3, COL_SCHOOL = 1, 2, 3, 4
COL_ENROLL = 5
COL_BODY_START, COL_BODY_END = 6, 30
COL_FREE = 31
COL_TIME = 33
SOURCE_COLS = 34

V1_COLUMNS = (
    ["答题序号", "来源"]
    + [f"Q{i}" for i in range(1, 31)]
    + [
        "开始时间",
        "提交时间",
        "答题时长",
        "IP省份",
        "IP城市",
        "IP地址",
        "浏览器",
        "操作系统",
    ]
)

EXCEL_EPOCH = datetime(1899, 12, 30)
# xlsx 里只填了时间的格子会被存成这一天的某个时刻，输出时要剥掉日期部分。
# 整列若被推断成 String，它会以 "1899-12-31 11:30:00" 这种文本形式出现
EXCEL_DATE_ZERO = date(1899, 12, 31)
ZERO_DATE_PREFIX = "1899-12-31 "
# 导出的 csv 里时间多是这样写的斜杠格式，v1.csv 用的是短横线
TIME_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S")
SCHOOL_SEP = "|"
DESENSITIZED = "[DESENSITIZED]"
SOURCE_NAME = "腾讯文档"
# v1.csv 里单选题存的是选项序号，不是选项文本
Q1_OPTIONS = {"是": 1, "否，我选择将数据所有权让渡给数据收集方": 2}
Q3_OPTIONS = {"是": 1.0, "否，我要求数据收集方在再发布时移除数据中标识的身份信息": 2.0}


def load_raw(path: Path) -> tuple[list[list], list[str]]:
    """读源表格（csv / xlsx），返回 (数据行, 被跳过的行的序号列文本)。

    polars 默认把首行当表头；导出时没带表头的话首行数据会被吃掉，
    所以表头列名叫不动数字时才回退成无表头重读。
    腾讯文档导出还会多带一行文档标题（如「23年7月2日9点49更新」），
    这类行连同表头行一起按「序号列不是数字」跳过。
    """
    suffix = path.suffix.lower()
    if suffix in EXCEL_SUFFIXES:
        df = pl.read_excel(path)
        if df.columns and _looks_like_number(df.columns[COL_ID]):
            df = pl.read_excel(path, has_header=False)
    elif suffix in CSV_SUFFIXES:
        separator = "\t" if suffix == ".tsv" else ","
        df = pl.read_csv(
            path,
            separator=separator,
            encoding="utf-8-sig",  # 腾讯文档导出带 BOM
            truncate_ragged_lines=True,
        )
        if df.columns and _looks_like_number(df.columns[COL_ID]):
            df = pl.read_csv(
                path,
                separator=separator,
                encoding="utf-8-sig",
                has_header=False,
                truncate_ragged_lines=True,
            )
    else:
        raise SystemExit(
            f"不支持的输入格式: {suffix or '无扩展名'}（只接受 csv / xlsx）"
        )

    rows: list[list] = []
    skipped: list[str] = []
    for row in df.iter_rows():
        cells = list(row)
        if not any(v is not None and v != "" for v in cells):
            continue
        if _looks_like_number(cells[COL_ID]):
            rows.append(cells)
        else:
            skipped.append(str(cells[COL_ID]))
    return rows, skipped


def _looks_like_number(value: object) -> bool:
    """表头列名一般是「答题序号」这类文字，若整列首格能转成数字说明其实没有表头。"""
    try:
        float(str(value))
    except ValueError:
        return False
    return True


def restore_number(value: int | float) -> tuple[str, str]:
    """还原被 Excel 转成序列号的数字，第二个返回值是存疑原因（无则空串）。

    0 < v < 1 是当天的时间（如 0.479 -> 11:30），v >= 40000 是日期，
    其余按普通数字处理（如限电 800W、断电 11 点）。
    """
    if 0 < value < 1:
        minutes = round(value * 24 * 60)
        return f"{minutes // 60:02d}:{minutes % 60:02d}", "疑似时间"
    if value >= 40000:
        moment = EXCEL_EPOCH + timedelta(days=value)
        return moment.strftime("%Y-%m-%d %H:%M:%S"), "疑似日期"
    return str(int(value) if float(value).is_integer() else value), ""


def to_cell(value: object) -> tuple[str | None, str]:
    """把源单元格转成 v1 的字符串值，顺便报告存疑原因。

    csv 读入时整列常被推断成字符串（该列多数是文本），原本的 Excel 序列号会退化成
    "45158" 这样的字符串，这里对它同样走一次还原，保证 csv 与 xlsx 两条路径行为一致。
    """
    if value is None or value == "":
        return None, ""
    if isinstance(value, bool):
        return str(value), ""
    if isinstance(value, (int, float)):
        return restore_number(value)
    if isinstance(value, (datetime, date)):
        return _format_moment(value), ""
    text = str(value)
    if text.startswith(ZERO_DATE_PREFIX):
        return text[len(ZERO_DATE_PREFIX) :], ""
    try:
        number = float(text)
    except ValueError:
        return text, ""
    if math.isfinite(number):
        return restore_number(number)
    return text, ""


def to_answer_time(value: object) -> tuple[str | None, str]:
    """提交时间：Excel 序列号、datetime、字符串三种形态统一成 v1 的时间格式。"""
    if value is None or value == "":
        return None, ""
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y-%m-%d %H:%M:%S"), ""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        moment = EXCEL_EPOCH + timedelta(days=float(value))
        return moment.strftime("%Y-%m-%d %H:%M:%S"), ""
    text = str(value)
    for fmt in TIME_FORMATS:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d %H:%M:%S"), ""
        except ValueError:
            continue
    return text, f"提交时间无法解析: {text!r}"


def _format_moment(value: datetime | date) -> str:
    """答案里的 datetime：只填时间的格子落在 EXCEL_DATE_ZERO 这天，只输出时间部分。"""
    if isinstance(value, datetime) and value.date() <= EXCEL_DATE_ZERO:
        return value.strftime("%H:%M:%S")
    return value.strftime("%Y-%m-%d %H:%M:%S")


def split_school(value: object) -> tuple[str | None, str]:
    """源院校列是 `省|市|校名`，取末段作为 v1 的校名。"""
    text = "" if value is None else str(value).strip()
    if not text:
        return None, "院校为空"
    parts = [p.strip() for p in text.split(SCHOOL_SEP) if p.strip()]
    if len(parts) != 3:
        return parts[-1] if parts else None, f"院校格式异常: {text}"
    return parts[-1], ""


def build_rows(rows: list[list], start_id: int) -> tuple[list[dict], list[str]]:
    """按列映射生成 v1 行，同时收集需要人工复核的格子。"""
    result: list[dict] = []
    review: list[str] = []
    for offset, row in enumerate(rows):
        if len(row) != SOURCE_COLS:
            review.append(f"源行 {row[COL_ID]}: 列数 {len(row)} != {SOURCE_COLS}")
            row = list(row) + [None] * (SOURCE_COLS - len(row))

        record: dict[str, object] = {c: None for c in V1_COLUMNS}
        record["答题序号"] = start_id + offset
        record["来源"] = SOURCE_NAME

        q1, why = to_cell(row[COL_Q1])
        record["Q1"] = Q1_OPTIONS.get(q1 or "")
        if q1 and record["Q1"] is None:
            review.append(f"源行 {row[COL_ID]}: Q1 选项未识别 {q1!r}")
        # 邮箱一律脱敏，v1.csv 不存真实邮箱
        record["Q2"] = DESENSITIZED if row[COL_Q2] else None
        q3, why = to_cell(row[COL_Q3])
        record["Q3"] = Q3_OPTIONS.get(q3 or "")
        if q3 and record["Q3"] is None:
            review.append(f"源行 {row[COL_ID]}: Q3 选项未识别 {q3!r}")

        school, why = split_school(row[COL_SCHOOL])
        if why:
            review.append(f"源行 {row[COL_ID]}: {why}")
        record["Q4"] = school

        for src_col in range(COL_BODY_START, COL_BODY_END + 1):
            v1_num = src_col - 1  # 源 Q6 -> v1 Q5
            value, why = to_cell(row[src_col])
            if why:
                review.append(
                    f"源行 {row[COL_ID]}: 源Q{src_col}(v1 Q{v1_num}) = "
                    f"{row[src_col]!r} -> {value!r}（{why}）"
                )
            record[f"Q{v1_num}"] = value

        value, why = to_cell(row[COL_FREE])
        record["Q30"] = value

        time_value, why = to_answer_time(row[COL_TIME])
        if why:
            review.append(f"源行 {row[COL_ID]}: {why}")
        record["开始时间"] = time_value

        # 源 Q5「入学时间」在 v1 无对应题，统一丢弃（不逐条进复核清单）
        result.append(record)
    return result, review


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("raw", type=Path, help="源表格导出的 csv 或 xlsx")
parser.add_argument(
    "-o", "--output", type=Path, help="输出 csv，默认写到当前目录 v1.additions.csv"
)
parser.add_argument(
    "--start-id",
    type=int,
    default=100_000_000,
    help="起始答题序号，默认 100000000（= IMPORTED_NUM_FROM，v1.csv 现有最大 34646）",
)
args = parser.parse_args()

rows, skipped = load_raw(args.raw)
records, review = build_rows(rows, args.start_id)

schema = {
    **{c: pl.String for c in V1_COLUMNS},
    "答题序号": pl.Int64,
    "Q1": pl.Int64,
    "Q3": pl.Float64,
}
df = pl.DataFrame(records, schema=schema)

out = args.output or Path("v1.additions.csv")
out.parent.mkdir(parents=True, exist_ok=True)
df.write_csv(out)

emails = sum(1 for r in rows if r[COL_Q2])
enroll = sum(1 for r in rows if r[COL_ENROLL])
last_id = args.start_id + len(rows) - 1
print(f"已写入 {out}：本次 {df.height} 行，答题序号 {args.start_id}-{last_id}")
print(f"脱敏邮箱 {emails} 条，丢弃入学时间 {enroll} 条")
if skipped:
    preview = "、".join(s[:20] for s in skipped[:5])
    print(f"跳过非数据行 {len(skipped)} 行：{preview}")
print(f"待人工复核 {len(review)} 条：")
for line in review:
    print(f"  - {line}")
