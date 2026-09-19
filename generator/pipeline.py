"""问卷数据 -> Hugo markdown 的生成管线，debug 与生产构建共用。"""

from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo

import niquests
import polars as pl
from loguru import logger
from wenjuanxing_parser import QuestionnaireData, load_questions_from_yaml
from wenjuanxing_parser.models import Questionnaire, QuestionnaireResponse
from yaml12 import parse_yaml

from .config import ARCHIVE_YEARS, NAME_PREPROCESS
from .parser import qnum_extractor
from .province import find_province
from .render import FileNameMap, render_combined_markdown, sanitize_filename
from .render.common import _answer_time, _to_simplified, generate_markdown_path

V1_UNI_Q_NUM = 4
V2_UNI_Q_NUM = 2


def collect_universities(
    survey_data: Iterable[QuestionnaireResponse],
    uni_q_num: int,
) -> tuple[dict[str, list], dict[str, list]]:
    """遍历问卷数据，按学校名分组，最近 ARCHIVE_YEARS 年内为 active，更早为 archived"""
    archive_time = datetime.now(ZoneInfo("Asia/Shanghai")) - timedelta(
        days=ARCHIVE_YEARS * 365
    )
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
    for entries in universities.values():
        entries.reverse()
    for entries in universities_archived.values():
        entries.reverse()
    return dict(universities), dict(universities_archived)


def combine_university_groups(
    *groups: dict[str, list[QuestionnaireResponse]],
) -> dict[str, list[QuestionnaireResponse]]:
    """按学校名合并多份分组结果（如 active + archived / v1 + v2），并按时间倒序。"""
    combined: defaultdict[str, list[QuestionnaireResponse]] = defaultdict(list)
    for group in groups:
        for name, responses in group.items():
            combined[name].extend(responses)
    for responses in combined.values():
        responses.sort(key=_answer_time, reverse=True)
    return dict(combined)


def write_combined_universities(
    v1_universities: dict[str, list[QuestionnaireResponse]],
    v2_universities: dict[str, list[QuestionnaireResponse]],
    v1_questionnaire: Questionnaire,
    v2_questionnaire: Questionnaire | None,
    province_mapping: list[tuple[str, str]],
    archived: bool,
) -> None:
    """按学校名把 v1/v2 答卷合并渲染到同一个 md 文件。

    只有 v1 或只有 v2 的学校也照样生成，缺失的那一份传空列表即可。
    """
    university_names = set(v1_universities) | set(v2_universities)
    section = "archived" if archived else "active"
    total = len(university_names)
    logger.info(
        f"Start generating {section} markdown files: {total} "
        f"(v1: {len(v1_universities)}, v2: {len(v2_universities)})"
    )
    filename_map = FileNameMap()
    written_dirs: set[Path] = set()
    for name in sorted(university_names):
        cleaned_name = sanitize_filename(name)
        slug = filename_map[cleaned_name]
        province = find_province(cleaned_name, province_mapping)
        target = generate_markdown_path(province, cleaned_name, archived)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.parent not in written_dirs:
            weight_str = "\nweight: 10" if province in ("国外", "不予收录") else ""
            (target.parent / "_index.md").write_text(
                data=f"---\nbookCollapseSection: true{weight_str}\n---",
                encoding="utf-8",
            )
            written_dirs.add(target.parent)
        target.write_text(
            render_combined_markdown(
                cleaned_name,
                v1_universities.get(name, []),
                v2_universities.get(name, []),
                v1_questionnaire,
                v2_questionnaire,
                slug,
                archived,
            ),
            encoding="utf-8",
        )
    logger.info(f"Finished generating {section} markdown files: {total}")


def build_university_pages(
    v1_survey_data: Iterable[QuestionnaireResponse],
    v2_survey_data: Iterable[QuestionnaireResponse],
    v1_questionnaire: Questionnaire,
    v2_questionnaire: Questionnaire | None,
    province_mapping: list[tuple[str, str]],
    split_archived: bool = True,
) -> None:
    """把 v1/v2 答卷分组后写盘。

    split_archived=False 时 active 与 archived 合并进同一个页面（debug 预览用）。
    """
    v1_active, v1_archived = collect_universities(v1_survey_data, V1_UNI_Q_NUM)
    v2_active, v2_archived = collect_universities(v2_survey_data, V2_UNI_Q_NUM)
    logger.info(
        f"v1: {len(set(v1_active) | set(v1_archived))} 所学校, "
        f"v2: {len(set(v2_active) | set(v2_archived))} 所学校"
    )
    if not split_archived:
        write_combined_universities(
            combine_university_groups(v1_active, v1_archived),
            combine_university_groups(v2_active, v2_archived),
            v1_questionnaire,
            v2_questionnaire,
            province_mapping,
            archived=False,
        )
        return
    write_combined_universities(
        v1_active,
        v2_active,
        v1_questionnaire,
        v2_questionnaire,
        province_mapping,
        archived=False,
    )
    write_combined_universities(
        v1_archived,
        v2_archived,
        v1_questionnaire,
        v2_questionnaire,
        province_mapping,
        archived=True,
    )


def load_questionnaire(source: Path | str) -> Questionnaire | None:
    """从本地 yaml 文件或远程 URL 加载问卷定义，失败只告警并返回 None。"""
    try:
        if isinstance(source, Path):
            text = source.read_text(encoding="utf-8")
        else:
            r = niquests.get(source)
            if r.status_code != 200:
                logger.warning(f"问卷下载失败（HTTP {r.status_code}）: {source}")
                return None
            text = r.text
        return load_questions_from_yaml(parse_yaml(text))  # type: ignore[no-any-return]
    except Exception as e:  # noqa: BLE001
        logger.warning(f"问卷加载失败，已跳过: {source} {e!r}")
        return None


def load_remote_survey_data(
    url: str, questionnaire: Questionnaire, meta_extractor: object
) -> list[QuestionnaireResponse] | None:
    """拉取远程答卷 CSV，不存在或解析失败时返回 None。"""
    try:
        r = niquests.get(url)
        if r.status_code != 200:
            logger.warning(f"答卷数据不可用（HTTP {r.status_code}）: {url}")
            return None
        content = r.content or b""
        if not content.strip():
            logger.warning(f"答卷数据为空: {url}")
            return None
        df = pl.read_csv(BytesIO(content), truncate_ragged_lines=True)
        data = QuestionnaireData.from_dataframe(
            df,
            questionnaire,
            meta_extractor=meta_extractor,  # type: ignore[arg-type]
            q_num_extractor=qnum_extractor,
        )
        return list(data)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"答卷解析失败，已跳过: {url} {e!r}")
        return None
