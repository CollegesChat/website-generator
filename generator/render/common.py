import os
import re
import sys
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import zhconv
from loguru import logger
from wenjuanxing_parser.models import AnswerValue, Questionnaire, QuestionnaireResponse

from ..config import SITE_DIR
from ..province import find_province
from ..slug import FileNameMap


def _is_ci() -> bool:
    return not sys.stdout.isatty() or bool(os.environ.get("CI"))


def _to_simplified(text: str) -> str:
    return zhconv.convert(text, "zh-cn")


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
    [str, list[QuestionnaireResponse], Questionnaire, str, bool, int],
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
# FIXME: 考虑替换HTML标签 + 更多
# 考虑替换为HTML实体标签


def _indent_multiline(text: str, indent: str = "\t") -> str:
    """把多行文本的换行替换为换行+缩进，避免换行导致 markdown 列表项被截断。"""
    return text.replace("\n", "\n" + indent)


def generate_markdown_path(province: str, filename: str, archived: bool) -> Path:
    base = SITE_DIR / "content" / "docs"
    if archived:
        base = base / "archived"
    return base / "universities" / province / f"{filename}.md"


def render_question_groups(
    responses: list[QuestionnaireResponse],
    questions_map: Questionnaire,
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
        if '还有什么要说的吗' in question.title or '自由补充' in question.title:
            lines.append("## 自由补充\n\n")
        else:
            lines.append(f"## Q: {question.title}\n\n")
        for summary_text, entries in groups.items():
            count = len(entries)
            escaped = _markdown_escape(summary_text)

            if count == 1:
                entry = entries[0]
                if entry.detail:
                    lines.append(
                        f"- A{entry.num}: {escaped}: "
                        f"{_indent_multiline(_markdown_escape(entry.detail))}\n"
                    )
                else:
                    lines.append(
                        f"- A{entry.num}: {_indent_multiline(escaped)}\n"
                    )
            else:
                title_escaped = re.sub(r'["\r\n]', "", escaped)
                lines.append(
                    f'- {{{{< details title="{title_escaped} x {count}" >}}}}\n\n'
                )  # UPSTREAM: https://github.com/alex-shpak/hugo-book/issues/830
                no_detail_nums: list[str] = []
                detail_lines: list[str] = []
                for entry in entries:
                    if entry.detail:
                        detail_lines.append(
                            f"  - A{entry.num}: "
                            f"{_indent_multiline(_markdown_escape(entry.detail), '    ')}"
                        )
                    else:
                        no_detail_nums.append(f"A{entry.num}")
                if no_detail_nums:
                    lines.append("  " + " ".join(no_detail_nums) + "\n")
                if no_detail_nums and detail_lines:
                    lines.append("\n  ---\n")
                for dl in detail_lines:
                    lines.append(dl + "\n")
                lines.append("\n  {{< /details >}}\n")
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
    lines.append("> 数据来源：\n\n")
    lines.append('{{% details title="展开" %}}\n\n')
    for resp in responses:
        if resp.metadata:
            lines.append(
                f"- A{resp.metadata.num} ({resp.metadata.answer_date:%Y年%m月})\n"
            )
    lines.append("\n{{% /details %}}\n\n")
    return lines


def _write_one(
    name: str,
    responses: list[QuestionnaireResponse],
    questions_map: Questionnaire,
    slug: str,
    target: Path,
    archived: bool,
    uni_q_num: int,
    render_fn: RenderFn,
) -> str:
    content = render_fn(name, responses, questions_map, slug, archived, uni_q_num)
    target.write_text(content, encoding="utf-8")
    return name


def render_combined_markdown(
    name: str,
    v1_responses: list[QuestionnaireResponse],
    v2_responses: list[QuestionnaireResponse],
    v1_questions: Questionnaire,
    v2_questions: Questionnaire,
    slug: str,
    archived: bool,
) -> str:
    """debug 模式下将 v1/v2 内容合并到同一页面。"""
    from .legacy import render_university_body as render_v1_body
    from .legacy import render_university_markdown as render_v1
    from .new import V2_META_Q_NUMS, _build_header_v2
    from .new import render_university_body as render_v2_body
    from .new import render_university_markdown as render_v2

    if not v1_responses:
        return render_v2(name, v2_responses, v2_questions, slug, archived, 2)
    if not v2_responses:
        return render_v1(name, v1_responses, v1_questions, slug, archived, 4)

    lines = _build_header_v2(name, slug, archived, v2_responses, V2_META_Q_NUMS)
    source_end = lines.index("\n{{% /details %}}\n\n")
    v1_sources = [
        f"- A{response.metadata.num} ({response.metadata.answer_date:%Y年%m月})\n"
        for response in v1_responses
        if response.metadata is not None
    ]
    lines[source_end:source_end] = v1_sources
    lines.extend(
        [
            "{{< tabs >}}\n\n",
            '{{% tab "v1" %}}\n\n',
            *render_v1_body(v1_responses, v1_questions, 4),
            "\n{{% /tab %}}\n\n",
            '{{% tab "v2" %}}\n\n',
            *render_v2_body(v2_responses, v2_questions, 2),
            "\n{{% /tab %}}\n\n",
            "{{< /tabs >}}\n",
        ]
    )
    return "".join(lines)


def write_markdown_for_universities(
    universities: dict[str, list[QuestionnaireResponse]],
    questions_map: Questionnaire,
    filename_map: FileNameMap,
    province_mapping: list[tuple[str, str]],
    archived: bool,
    uni_q_num: int,
    render_fn: RenderFn,
) -> None:
    # 在 task 元组中加入 province
    tasks: list[tuple[str, list[QuestionnaireResponse], str, Path, str]] = []
    for name, responses in universities.items():
        cleaned_name = sanitize_filename(name)
        slug = filename_map[cleaned_name]
        province = find_province(cleaned_name, province_mapping)

        target = generate_markdown_path(province, cleaned_name, archived)
        tasks.append((cleaned_name, responses, slug, target, province))

    written_dirs: set[Path] = set()
    for _, _, _, target, province in tasks:
        parent = target.parent
        if parent not in written_dirs:
            parent.mkdir(parents=True, exist_ok=True)
            weight_str = "\nweight: 10" if province in ["国外", "不予收录"] else ""
            (parent / "_index.md").write_text(
                data=f"---\nbookCollapseSection: true{weight_str}\n---",
                encoding="utf-8",
            )
            written_dirs.add(parent)

    max_workers = os.cpu_count() or 1
    section = "archived" if archived else "active"
    total = len(tasks)
    logger.info(f"Start generating {section} markdown files: {total}")
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
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
            ): name
            for name, responses, slug, target, _ in tasks
        }
        completed = 0
        if _is_ci():
            bar_width = 30
            for future in as_completed(futures):
                name = futures[future]
                try:
                    future.result()
                except OSError as e:
                    logger.exception(f"Failed to render {name}, {e!r}")
                completed += 1
                filled = int(bar_width * completed / total) if total else bar_width
                bar = "=" * filled + ">" + " " * (bar_width - filled)
                sys.stdout.write(f"\r[{section}] [{bar}] {completed}/{total}")
                sys.stdout.flush()
            sys.stdout.write("\n")
        else:
            from rich.progress import (
                BarColumn,
                Progress,
                TaskProgressColumn,
                TextColumn,
                TimeRemainingColumn,
            )

            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeRemainingColumn(),
            ) as progress:
                task_id = progress.add_task(f"[cyan]{section}", total=total)
                for future in as_completed(futures):
                    name = futures[future]
                    try:
                        future.result()
                    except OSError as e:
                        logger.exception(f"Failed to render {name}, {e!r}")
                    completed += 1
                    progress.update(task_id, completed=completed)
    logger.info("Finished generating markdown files.")
