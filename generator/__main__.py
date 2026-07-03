import sys
from collections import defaultdict
from io import BytesIO
from pathlib import Path
from random import sample
from typing import cast

import niquests
import polars as pl
from loguru import logger
from wenjuanxing_parser import QuestionnaireData, load_questions_from_yaml
from yaml12 import parse_yaml

from .config import (
    ARCHIVE_TIME,
    CSV_URL,
    DATA_URL,
    DOC_URL,
    NAME_PREPROCESS,
    QUESTIONNAIRES_URL,
    REQUIRED_DOCS,
    SITE_DIR,
)
from .parsers.legacy import meta_extractor as legacy_meta_extractor
from .parsers.legacy import qnum_extractor as legacy_qnum_extractor
from .province import NORMAL_NAME_MATCHER, find_province, load_province_mapping
from .renderer import FileNameMap, write_markdown_for_universities


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


def collect_universities(
    survey_data: QuestionnaireData,
    uni_q_num: int,
) -> tuple[dict[str, list], dict[str, list]]:
    """遍历问卷数据，按学校名分组，并按 ARCHIVE_TIME 分为 active / archived"""
    universities: defaultdict[str, list] = defaultdict(list)
    universities_archived: defaultdict[str, list] = defaultdict(list)
    for resp in survey_data:
        uni_answer = resp.answers.get(uni_q_num)
        if uni_answer is None:
            continue
        name = NAME_PREPROCESS.sub("", str(uni_answer.value)).strip()
        if not name:
            continue
        if resp.metadata and resp.metadata.answer_date < ARCHIVE_TIME:
            universities_archived[name].append(resp)
        else:
            universities[name].append(resp)
    return dict(universities), dict(universities_archived)


logger.info("开始检查并同步远程资源文件...")
download_files(
    REQUIRED_DOCS, DOC_URL, SITE_DIR / "content" / "docs" / "choose-a-college"
)
legacy_questionnaire, new_questionnaire = (
    load_questions_from_yaml(parse_yaml(niquests.get(QUESTIONNAIRES_URL[0]).text)),  # type: ignore
    None,  # type: ignore
)

legacy_df = pl.read_csv(
    BytesIO(niquests.get(DATA_URL[0]).content), truncate_ragged_lines=True
)
legacy_survey_data = QuestionnaireData.from_dataframe(
    legacy_df,
    legacy_questionnaire,
    meta_extractor=legacy_meta_extractor,
    q_num_extractor=legacy_qnum_extractor,
)

province_mapping = load_province_mapping(niquests.get(CSV_URL).content)
active, archived = collect_universities(legacy_survey_data, uni_q_num=4)

if "debug" in sys.argv:
    active = dict(sample(list(active.items()), min(100, len(active))))
    archived = dict(sample(list(archived.items()), min(100, len(archived))))
    logger.info(f"Debug mode: {len(active)} active, {len(archived)} archived")

for name in list(active) + list(archived):
    if not NORMAL_NAME_MATCHER.search(name):
        logger.warning(f"maybe invalid: {name}")

filename_map = FileNameMap()
write_markdown_for_universities(
    active,
    legacy_questionnaire,
    filename_map,
    province_mapping,
    archived=False,
    uni_q_num=4,
)
write_markdown_for_universities(
    archived,
    legacy_questionnaire,
    filename_map,
    province_mapping,
    archived=True,
    uni_q_num=4,
)
