import os
import re
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from loguru import logger
from wenjuanxing_parser.models import (
    AnswerValue,
    ChosenOption,
    QuestionnaireResponse,
    ResponseStatus,
)
from wenjuanxing_parser._models.questions import AnyQuestion

from .config import FILENAME_PREPROCESS, SITE_DIR
from .province import find_province
from .slug import FileNameMap


def sanitize_filename(filename: str) -> str:
    """清理文件名：替换非法字符，权重截断（中文=2, 英文=1, 上限32）"""
    cleaned = re.sub(r"[\\/]", "_", filename)
    cleaned = re.sub(r'[:*?"<>|\0\r\n\t]', "", cleaned)

    max_weight = 32
    current_weight = 0
    truncated_index = len(cleaned)
    for i, char in enumerate(cleaned):
        weight = 2 if ord(char) > 127 else 1
        if current_weight + weight > max_weight:
            truncated_index = i
            break
        current_weight += weight

    return cleaned[:truncated_index]


def format_answer_value(value: AnswerValue) -> str:
    """将 AnswerValue 转为显示文本"""
    if value is None:
        return ""
    if isinstance(value, ResponseStatus):
        return str(value)
    if isinstance(value, str):
        return value
    if isinstance(value, ChosenOption):
        if value.additional_text:
            return f"{value.text}〖{value.additional_text}〗"
        return value.text
    if isinstance(value, list):
        parts = []
        for item in value:
            if isinstance(item, ChosenOption):
                if item.additional_text:
                    parts.append(f"{item.text}〖{item.additional_text}〗")
                else:
                    parts.append(item.text)
            else:
                parts.append(str(item))
        if all(isinstance(item, (str, ResponseStatus)) for item in value):
            return "┋".join(parts)
        return ", ".join(parts)
    return str(value)


def _markdown_escape(text: str) -> str:
    return text.replace("*", "\\*").replace("~", "\\~").replace("_", "\\_")


def generate_markdown_path(province: str, filename: str, archived: bool) -> Path:
    base = SITE_DIR / "content" / "docs"
    if archived:
        base = base / "archived"
    return base / "universities" / province / f"{filename}.md"


def render_university_markdown(
    name: str,
    responses: list[QuestionnaireResponse],
    questions_map: Mapping[int, AnyQuestion],
    slug: str,
    archived: bool,
    uni_q_num: int,
) -> str:
    lines: list[str] = [
        "---\n",
        f'title: "{name}{" (已归档)" if archived else ""}"\n',
        f'slug: "{slug}"\n',
        f"description: 来自 colleges.chat 的{name} 问卷调查信息\n",
        "---\n\n",
    ]
    lines.append("> 本页面内容来源于问卷，仅供参考。\n\n")
    lines.append("> 数据来源：\n<details><summary>展开</summary>\n<ul>\n")
    for resp in responses:
        if resp.metadata:
            lines.append(
                f"<li>A{resp.metadata.num} ({resp.metadata.answer_date:%Y年%m月})</li>\n"
            )
    lines.append("</ul>\n</details>\n\n")

    for q_num, question in sorted(questions_map.items()):
        if q_num <= uni_q_num:
            continue
        lines.append(f"## Q: {question.title}\n\n")
        for resp in responses:
            answer = resp.answers.get(q_num)
            if answer is not None and resp.metadata is not None:
                text = format_answer_value(answer.value)
                if text:
                    lines.append(f"- A{resp.metadata.num}: {_markdown_escape(text)}\n")
        lines.append("\n")

    return "".join(lines)


def _write_one(
    name: str,
    responses: list[QuestionnaireResponse],
    questions_map: Mapping[int, AnyQuestion],
    slug: str,
    target: Path,
    archived: bool,
    uni_q_num: int,
) -> None:
    target.write_text(
        render_university_markdown(
            name, responses, questions_map, slug, archived, uni_q_num
        ),
        encoding="utf-8",
    )


def write_markdown_for_universities(
    universities: dict[str, list[QuestionnaireResponse]],
    questions_map: Mapping[int, AnyQuestion],
    filename_map: FileNameMap,
    province_mapping: list[tuple[str, str]],
    archived: bool,
    uni_q_num: int,
) -> None:
    tasks: list[tuple[str, list[QuestionnaireResponse], str, Path]] = []
    for name, responses in universities.items():
        cleaned_name = sanitize_filename(name)
        slug = filename_map[cleaned_name]
        province = find_province(cleaned_name, province_mapping)
        target = generate_markdown_path(province, cleaned_name, archived)
        tasks.append((cleaned_name, responses, slug, target))

    for parent in {target.parent for _, _, _, target in tasks}:
        parent.mkdir(parents=True, exist_ok=True)
        (parent / "_index.md").write_text("---\nbookCollapseSection: true\n---")

    max_workers = min(32, max(1, (os.cpu_count() or 1) * 4))
    section = "archived" if archived else "active"
    total = len(tasks)
    logger.info(f"Start generating {section} markdown files: {total}")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _write_one,
                name,
                responses,
                questions_map,
                slug,
                target,
                archived,
                uni_q_num,
            )
            for name, responses, slug, target in tasks
        ]
        completed = 0
        for future in as_completed(futures):
            future.result()
            completed += 1
            progress = completed / total * 100 if total else 100.0
            logger.info(
                f"[progress] {section}: {completed}/{total} ({progress:.1f}%)",
            )
    logger.info("Finished generating markdown files.")
