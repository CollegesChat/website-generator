import sys
from collections import defaultdict
from collections.abc import Iterable, Mapping
from io import BytesIO
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo

import niquests
import polars as pl
from loguru import logger
from wenjuanxing_parser import QuestionnaireData, load_questions_from_yaml
from wenjuanxing_parser.models import QuestionnaireResponse
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
from .parser import legacy_meta_extractor, new_meta_extractor, qnum_extractor
from .province import find_province, load_province_mapping
from .render import (
    FileNameMap,
    render_combined_markdown,
    render_legacy,
    sanitize_filename,
    write_markdown_for_universities,
)
from .render.common import _to_simplified, generate_markdown_path


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


def collect_universities(
    survey_data: Iterable[QuestionnaireResponse],
    uni_q_num: int,
) -> tuple[dict[str, list], dict[str, list]]:
    """遍历问卷数据，按学校名分组，并按 ARCHIVE_TIME 分为 active / archived"""
    archive_time = ARCHIVE_TIME.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    universities: defaultdict[str, list] = defaultdict(list)
    universities_archived: defaultdict[str, list] = defaultdict(list)
    for resp in survey_data:
        uni_answer = resp.answers.get(uni_q_num)
        if uni_answer is None:
            continue
        name = NAME_PREPROCESS.sub("", str(uni_answer.value)).strip()
        name = _to_simplified(name)
        if not name:
            continue
        if resp.metadata:
            answer_date = resp.metadata.answer_date
            if answer_date.tzinfo is None:
                answer_date = answer_date.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
            if answer_date < archive_time:
                universities_archived[name].append(resp)
                continue
        universities[name].append(resp)
    return dict(universities), dict(universities_archived)


def combine_university_groups(
    *groups: dict[str, list[QuestionnaireResponse]],
) -> dict[str, list[QuestionnaireResponse]]:
    combined: defaultdict[str, list[QuestionnaireResponse]] = defaultdict(list)
    for group in groups:
        for name, responses in group.items():
            combined[name].extend(responses)
    return dict(combined)


V2_YAML_PATH = Path("/mnt/data/Project/questionnaire/v2.yaml")
V1_YAML_PATH = Path("/mnt/data/Project/questionnaire/v1.yaml")


def load_debug_responses(
    path: Path, questionnaire: Mapping[int, Any]
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


if "debug" in sys.argv:
    logger.info("Debug mode: 使用本地 v1/v2 数据 + 随机 mock 数据")
    with open(V1_YAML_PATH, encoding="utf-8") as f:
        v1_questionnaire = load_questions_from_yaml(parse_yaml(f.read()))  # type: ignore
    with open(V2_YAML_PATH, encoding="utf-8") as f:
        v2_questionnaire = load_questions_from_yaml(parse_yaml(f.read()))  # type: ignore
    v1_path = Path(__file__).resolve().parent.parent / "required" / "results_desensitized.csv"
    v1_df = pl.read_csv(v1_path, truncate_ragged_lines=True)
    v1_survey_data = QuestionnaireData.from_dataframe(
        v1_df,
        v1_questionnaire,
        meta_extractor=legacy_meta_extractor,
        q_num_extractor=qnum_extractor,
    )
    v1_active, v1_archived = collect_universities(v1_survey_data, uni_q_num=4)
    v1_universities = combine_university_groups(v1_active, v1_archived)
    from .mock import generate_mock_v2_data

    mock_responses, province_mapping = generate_mock_v2_data(v2_questionnaire)
    file_responses: list[QuestionnaireResponse] = []
    debug_args = sys.argv[sys.argv.index("debug") + 1 :]
    if len(debug_args) > 1:
        raise SystemExit("用法: python -m generator debug [答卷.csv|答卷.xlsx]")
    if debug_args:
        debug_path = Path(debug_args[0]).expanduser()
        if not debug_path.is_file():
            raise SystemExit(f"debug 数据文件不存在: {debug_path}")
        file_responses = load_debug_responses(debug_path, v2_questionnaire)
        logger.info(f"Loaded {len(file_responses)} responses from {debug_path}")
    active, archived = collect_universities(
        [*mock_responses, *file_responses], uni_q_num=2
    )
    v2_universities = combine_university_groups(active, archived)
    university_names = set(v1_universities) | set(v2_universities)
    logger.info(
        f"Debug data: {len(v1_universities)} v1 + {len(v2_universities)} v2 universities; "
        f"{len(university_names)} total"
    )
    logger.info("v2 universities: " + ", ".join(sorted(v2_universities)))
    filename_map = FileNameMap()
    for name in sorted(university_names):
        cleaned_name = sanitize_filename(name)
        slug = filename_map[cleaned_name]
        province = find_province(cleaned_name, province_mapping)
        target = generate_markdown_path(province, cleaned_name, archived=False)
        target.parent.mkdir(parents=True, exist_ok=True)
        (target.parent / "_index.md").write_text(
            data="---\nbookCollapseSection: true\n---", encoding="utf-8"
        )
        target.write_text(
            render_combined_markdown(
                cleaned_name,
                v1_universities.get(name, []),
                v2_universities.get(name, []),
                v1_questionnaire,
                v2_questionnaire,
                slug,
                archived=False,
            ),
            encoding="utf-8",
        )
else:
    logger.info("开始检查并同步远程资源文件...")
    download_files(
        REQUIRED_DOCS, DOC_URL, SITE_DIR / "content" / "docs" / "choose-a-college"
    )
    legacy_questionnaire, new_questionnaire = (
        load_questions_from_yaml(parse_yaml(niquests.get(QUESTIONNAIRES_URL[0]).text)),  # type: ignore
        None,
    )

    legacy_df = pl.read_csv(
        BytesIO(niquests.get(DATA_URL[0]).content or b""), truncate_ragged_lines=True
    )
    legacy_survey_data = QuestionnaireData.from_dataframe(
        legacy_df,
        legacy_questionnaire,
        meta_extractor=legacy_meta_extractor,
        q_num_extractor=qnum_extractor,
    )

    province_mapping = load_province_mapping(niquests.get(CSV_URL).content or b"")
    active, archived = collect_universities(legacy_survey_data, uni_q_num=4)
    filename_map = FileNameMap()
    write_markdown_for_universities(
        active,
        legacy_questionnaire,
        filename_map,
        province_mapping,
        archived=False,
        uni_q_num=4,
        render_fn=render_legacy,
    )
    write_markdown_for_universities(
        archived,
        legacy_questionnaire,
        filename_map,
        province_mapping,
        archived=True,
        uni_q_num=4,
        render_fn=render_legacy,
    )
