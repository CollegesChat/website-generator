"""入口：debug 预览 / 正式构建。生成逻辑在 pipeline.py，debug 数据在 _debug.py。"""

import sys
from pathlib import Path
from typing import cast

import niquests
from loguru import logger

from .config import (
    CSV_URL,
    DATA_URL,
    DOC_URL,
    PATCHES_URL,
    QUESTIONNAIRES_URL,
    REQUIRED_DOCS,
    SITE_DIR,
    V1_ADDITIONS_URL,
)
from .parser import legacy_meta_extractor, new_meta_extractor
from .pipeline import (
    apply_answer_patches,
    build_university_pages,
    load_questionnaire,
    load_remote_survey_data,
)
from .province import load_province_mapping


def download_files(names: list[str], base_url: str, root: Path) -> None:
    """下载缺失的文件到 root 目录"""
    root.mkdir(parents=True, exist_ok=True)
    for name in names:
        local_file = Path(
            root / name if not name.startswith("http") else name.split("/")[-1]
        )
        if not local_file.exists():
            if not name.startswith("http"):
                url = base_url + name
            else:
                url = name
            logger.info(f"Downloading {local_file} from {url}...")
            r = niquests.get(url)
            if r.status_code == 200:
                local_file.write_bytes(cast(bytes, r.content))
                logger.info(f"Saved {name}")
            else:
                logger.error(f"Failed to download {name}, status code: {r.status_code}")
        else:
            logger.warning(f"Skipped: {local_file.relative_to(root)} already exists")


if "debug" in sys.argv:
    from ._debug import run_debug

    run_debug()
else:
    logger.info("开始检查并同步远程资源文件...")
    download_files(
        REQUIRED_DOCS, DOC_URL, SITE_DIR / "content" / "docs" / "choose-a-college"
    )
    v1_questionnaire = load_questionnaire(QUESTIONNAIRES_URL[0])
    if v1_questionnaire is None:
        raise SystemExit("v1 问卷加载失败，终止构建")
    v1_survey_data = load_remote_survey_data(
        DATA_URL[0], v1_questionnaire, legacy_meta_extractor
    )
    if v1_survey_data is None:
        raise SystemExit("v1 答卷数据加载失败，终止构建")

    # 人工导入批次不在 v1.csv 里，单独一份；拿不到（尚未上传 / 404）就跳过
    imported = load_remote_survey_data(
        V1_ADDITIONS_URL, v1_questionnaire, legacy_meta_extractor
    )
    if imported:
        logger.info(f"合并人工导入批次 {len(imported)} 条")
        v1_survey_data = [*v1_survey_data, *imported]

    province_mapping = load_province_mapping(niquests.get(CSV_URL).content or b"")

    # v2 问卷/答卷可能尚未上线，拿不到就只生成 v1，不影响整站构建
    v2_questionnaire = load_questionnaire(QUESTIONNAIRES_URL[1])
    v2_survey_data = (
        load_remote_survey_data(DATA_URL[1], v2_questionnaire, new_meta_extractor)
        if v2_questionnaire is not None
        else None
    )
    if v2_survey_data is None:
        logger.warning("v2 数据不可用，本次仅生成 v1 内容")
    else:
        v2_survey_data, patched = apply_answer_patches(v2_survey_data, PATCHES_URL)
        if patched:
            logger.info(f"按 patch 配置作废 {patched} 条答案")

    build_university_pages(
        v1_survey_data,
        v2_survey_data or [],
        v1_questionnaire,
        v2_questionnaire,
        province_mapping,
    )
