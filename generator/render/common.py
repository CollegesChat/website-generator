import os
import re
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from loguru import logger
from wenjuanxing_parser.models import AnswerValue, QuestionnaireResponse

from ..config import SITE_DIR
from ..province import find_province
from ..slug import FileNameMap


@dataclass(frozen=True)
class FormattedAnswer:
    summary: str
    detail: str | None = None


@dataclass
class _ResponseEntry:
    num: int
    detail: str | None = None


type FormatFn = Callable[[AnswerValue], FormattedAnswer | list[FormattedAnswer] | None]
type RenderFn = Callable[
    [str, list[QuestionnaireResponse], Mapping, str, bool, int],
    str,
]


def sanitize_filename(filename: str) -> str:
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


def _markdown_escape(text: str) -> str:
    return text.replace("*", "\\*").replace("~", "\\~").replace("_", "\\_")


def generate_markdown_path(province: str, filename: str, archived: bool) -> Path:
    base = SITE_DIR / "content" / "docs"
    if archived:
        base = base / "archived"
    return base / "universities" / province / f"{filename}.md"


def render_question_groups(
    responses: list[QuestionnaireResponse],
    questions_map: Mapping,
    uni_q_num: int,
    format_fn: FormatFn,
) -> list[str]:
    lines: list[str] = []
    for q_num, question in sorted(questions_map.items()):
        if q_num <= uni_q_num:
            continue

        groups: dict[str, list[_ResponseEntry]] = {}
        for resp in responses:
            answer = resp.answers.get(q_num)
            if answer is None or resp.metadata is None:
                continue
            formatted = format_fn(answer.value)
            if formatted is None:
                continue
            if isinstance(formatted, list):
                formatted_list = cast("list[FormattedAnswer]", formatted)
            else:
                formatted_list = [formatted]
            for fmt in formatted_list:
                groups.setdefault(fmt.summary, []).append(
                    _ResponseEntry(num=resp.metadata.num, detail=fmt.detail),
                )

        if not groups:
            continue

        lines.append(f"## Q: {question.title}\n\n")
        for summary_text, entries in groups.items():
            count = len(entries)
            escaped = _markdown_escape(summary_text)

            if count == 1:
                entry = entries[0]
                if entry.detail:
                    lines.append(
                        f"- A{entry.num}: {escaped}: {_markdown_escape(entry.detail)}\n"
                    )
                else:
                    lines.append(f"- A{entry.num}: {escaped}\n")
            else:
                lines.append(f'- {{{{% details title="{escaped} x {count}" %}}}}\n\n')
                no_detail_nums: list[str] = []
                detail_lines: list[str] = []
                for entry in entries:
                    if entry.detail:
                        detail_lines.append(
                            f"  - A{entry.num}: {_markdown_escape(entry.detail)}"
                        )
                    else:
                        no_detail_nums.append(f"A{entry.num}")
                if no_detail_nums:
                    lines.append("  " + " ".join(no_detail_nums) + "\n")
                if no_detail_nums and detail_lines:
                    lines.append("\n  ---\n")
                for dl in detail_lines:
                    lines.append(dl + "\n")
                lines.append("\n  {{% /details %}}\n")
        lines.append("\n")

    return lines


def _build_header(
    name: str, slug: str, archived: bool, responses: list[QuestionnaireResponse]
) -> list[str]:
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
    return lines


def _write_one(
    name: str,
    responses: list[QuestionnaireResponse],
    questions_map: Mapping,
    slug: str,
    target: Path,
    archived: bool,
    uni_q_num: int,
    render_fn: RenderFn,
) -> None:
    target.write_text(
        render_fn(name, responses, questions_map, slug, archived, uni_q_num),
        encoding="utf-8",
    )


def write_markdown_for_universities(
    universities: dict[str, list[QuestionnaireResponse]],
    questions_map: Mapping,
    filename_map: FileNameMap,
    province_mapping: list[tuple[str, str]],
    archived: bool,
    uni_q_num: int,
    render_fn: RenderFn,
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
                render_fn,
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
