"""debug 模式专用：本地 v1 数据 + 本地/mock 的 v2 数据，预览 v1/v2 合并渲染效果。

用法::

    python -m generator debug [答卷.csv|答卷.xlsx]

不传文件时 v2 答卷由 ``_mock.py`` 随机生成；传文件时该文件的答卷会按学校合并进去。
生产构建不走这里，见 ``__main__.py`` 的 else 分支。
"""

import sys
from pathlib import Path

import polars as pl
from loguru import logger
from wenjuanxing_parser import QuestionnaireData
from wenjuanxing_parser.models import Questionnaire, QuestionnaireResponse

from ._mock import generate_mock_v2_data
from .parser import legacy_meta_extractor, new_meta_extractor, qnum_extractor
from .pipeline import (
    V2_UNI_Q_NUM,
    build_university_pages,
    collect_universities,
    load_questionnaire,
)

V1_YAML_PATH = Path("/mnt/data/Project/questionnaire/v1.yaml")
V2_YAML_PATH = Path("/mnt/data/Project/questionnaire/v2.yaml")
V1_DATA_PATH = (
    Path(__file__).resolve().parent.parent / "required" / "results_desensitized.csv"
)


def load_debug_responses(
    path: Path, questionnaire: Questionnaire
) -> list[QuestionnaireResponse]:
    """读取 debug 模式下额外提供的 CSV/XLSX 答卷。"""
    suffix = path.suffix.lower()
    match suffix:
        case ".csv":
            df = pl.read_csv(path, truncate_ragged_lines=True)
        case ".xlsx":
            df = pl.read_excel(path)
        case _:
            raise ValueError(
                f"不支持的 debug 数据文件格式: {path.suffix or '(无扩展名)'}，"
                "仅支持 .csv、.xlsx"
            )

    responses = QuestionnaireData.from_dataframe(
        df,
        questionnaire,
        q_num_extractor=qnum_extractor,
        meta_extractor=new_meta_extractor,
    )
    return list(responses)


def run_debug() -> None:
    """跑一遍 debug 构建：本地 v1 数据 + mock/文件提供的 v2 数据，全部写进 active。"""
    logger.info("Debug mode: 使用本地 v1 数据 + 随机 mock v2 数据")
    debug_args = sys.argv[sys.argv.index("debug") + 1 :]
    if len(debug_args) > 1:
        raise SystemExit("用法: python -m generator debug [答卷.csv|答卷.xlsx]")

    v1_questionnaire = load_questionnaire(V1_YAML_PATH)
    v2_questionnaire = load_questionnaire(V2_YAML_PATH)
    if v1_questionnaire is None or v2_questionnaire is None:
        raise SystemExit("本地问卷定义加载失败，请检查 _debug.py 里的 V1/V2_YAML_PATH")

    v1_df = pl.read_csv(V1_DATA_PATH, truncate_ragged_lines=True)
    v1_survey_data = QuestionnaireData.from_dataframe(
        v1_df,
        v1_questionnaire,
        meta_extractor=legacy_meta_extractor,
        q_num_extractor=qnum_extractor,
    )

    mock_responses, province_mapping = generate_mock_v2_data(v2_questionnaire)
    file_responses: list[QuestionnaireResponse] = []
    if debug_args:
        debug_path = Path(debug_args[0]).expanduser()
        if not debug_path.is_file():
            raise SystemExit(f"debug 数据文件不存在: {debug_path}")
        file_responses = load_debug_responses(debug_path, v2_questionnaire)
        logger.info(f"Loaded {len(file_responses)} responses from {debug_path}")

    v2_responses = [*mock_responses, *file_responses]
    v2_active, v2_archived = collect_universities(v2_responses, V2_UNI_Q_NUM)
    logger.info(
        "v2 universities: " + ", ".join(sorted(set(v2_active) | set(v2_archived)))
    )

    build_university_pages(
        list(v1_survey_data),
        v2_responses,
        v1_questionnaire,
        v2_questionnaire,
        province_mapping,
        split_archived=False,
    )
