import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

import zhconv
from loguru import logger
from wenjuanxing_parser.models import (
    AnswerValue,
    Questionnaire,
    QuestionnaireResponse,
    ResponseStatus,
    SelectedOption,
)

from ..config import (
    ANONYMOUS,
    IMPORTED_NUM_FROM,
    MARKDOWN_ESCAPE_RE,
    NICKNAME_NUM,
    SITE_DIR,
)


def _to_simplified(text: str) -> str:
    return zhconv.convert(text, "zh-cn")


_EPOCH = datetime.min.replace(tzinfo=ZoneInfo("Asia/Shanghai"))


def _answer_time(resp: QuestionnaireResponse) -> datetime:
    """答卷时间，统一成带时区后再比较（v1 元数据是 naive，v2 是 Asia/Shanghai）。"""
    if resp.metadata is None:
        return _EPOCH
    answer_date = resp.metadata.answer_date
    if answer_date.tzinfo is None:
        return answer_date.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    return answer_date


@dataclass(frozen=True)
class FormattedAnswer:
    summary: str
    detail: str | None = None


@dataclass
class _ResponseEntry:
    num: int
    detail: str | None = None


@dataclass(frozen=True)
class HeaderSource:
    """页面头部里来自某份问卷的一批答卷来源。

    meta_q_nums 为空表示只列出「编号 + 年月」（v1 答卷没有可展示的附加信息）。
    enroll_by_num 是「答题序号 -> 入学时间」映射，为空表示本批次无入学时间。
    """

    responses: list[QuestionnaireResponse]
    meta_q_nums: Sequence[int] = ()
    enroll_by_num: dict[int, str] | None = None


type FormatFn = Callable[[AnswerValue], FormattedAnswer | list[FormattedAnswer] | None]


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
    return MARKDOWN_ESCAPE_RE.sub(r"\\\1", text)


def _indent_multiline(text: str, indent: str = "\t") -> str:
    """把多行文本的换行替换为换行+缩进，避免换行导致 markdown 列表项被截断。"""
    return text.replace("\n", "\n" + indent)


def _answer_label(num: int) -> str:
    """答卷编号：人工导入批次用 M 前缀并从 1 重新计数，问卷星导出的沿用 A + 原序号。"""
    if num >= IMPORTED_NUM_FROM:
        return f"M{num - IMPORTED_NUM_FROM + 1}"
    return f"A{num}"


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
        if "还有什么要说的吗" in question.title:
            lines.append("## 自由补充 {#自由补充-v1}\n\n")
        elif "自由补充" in question.title:
            lines.append("## 自由补充 {#自由补充-v2}\n\n")
        else:
            lines.append(f"## Q: {question.title}\n")
        if question.prompt:
            lines.append(f'<small>{question.prompt}</small>\n\n')
        for summary_text, entries in groups.items():
            count = len(entries)
            escaped = _markdown_escape(summary_text)

            if count == 1:
                entry = entries[0]
                if entry.detail:
                    lines.append(
                        f"- {_answer_label(entry.num)}: {escaped}: "
                        f"{_indent_multiline(_markdown_escape(entry.detail))}\n"
                    )
                else:
                    lines.append(
                        f"- {_answer_label(entry.num)}: {_indent_multiline(escaped)}\n"
                    )
            else:
                title_escaped = re.sub(r'["\r\n]', "", escaped)
                details_title = f"{title_escaped} x {count}"
                lines.append(
                    '- {{< details title="' + details_title + '" >}}\n\n'
                )  # UPSTREAM: https://github.com/alex-shpak/hugo-book/issues/830
                no_detail_nums: list[str] = []
                detail_lines: list[str] = []
                for entry in entries:
                    if entry.detail:
                        escaped_detail = _markdown_escape(entry.detail)
                        detail_lines.append(
                            f"  - {_answer_label(entry.num)}: "
                            f"{_indent_multiline(escaped_detail, '    ')}"
                        )
                    else:
                        no_detail_nums.append(_answer_label(entry.num))
                if no_detail_nums:
                    lines.append("  " + " ".join(no_detail_nums) + "\n")
                if no_detail_nums and detail_lines:
                    lines.append("\n  ---\n")
                for dl in detail_lines:
                    lines.append(dl + "\n")
                lines.append("\n  {{< /details >}}\n")
        lines.append("\n")

    return lines


def render_university_body(
    responses: list[QuestionnaireResponse],
    questions_map: Questionnaire,
    uni_q_num: int,
    format_fn: FormatFn,
    *,
    meta_q_nums: Sequence[int] = (),
) -> list[str]:
    skip = set(range(1, uni_q_num + 1)) | set(meta_q_nums)
    filtered_map = {k: v for k, v in questions_map.items() if k not in skip}
    return render_question_groups(responses, filtered_map, 0, format_fn)


def render_university_markdown(
    name: str,
    responses: list[QuestionnaireResponse],
    questions_map: Questionnaire,
    slug: str,
    archived: bool,
    uni_q_num: int,
    format_fn: FormatFn,
    *,
    meta_q_nums: Sequence[int] = (),
    enroll_by_num: dict[int, str] | None = None,
) -> str:
    lines = _build_header(
        name, slug, archived, [HeaderSource(responses, meta_q_nums, enroll_by_num)]
    )
    lines.extend(
        render_university_body(
            responses, questions_map, uni_q_num, format_fn, meta_q_nums=meta_q_nums
        )
    )
    return "".join(lines)


def _format_meta_value(value: AnswerValue) -> str:
    if value is None or isinstance(value, ResponseStatus):
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, SelectedOption):
        return value.text
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, SelectedOption):
                parts.append(item.text)
            elif isinstance(item, str) and item.strip():
                parts.append(item)
        return ", ".join(parts)
    return str(value)


def _build_header(
    name: str, slug: str, archived: bool, sources: Sequence[HeaderSource]
) -> list[str]:
    """页面头部。多份来源（v1/v2）的答卷会按时间统一倒序交错列出。"""
    lines: list[str] = [
        "---\n",
        f'title: "{name}{" (已归档)" if archived else ""}"\n',
        f'slug: "{slug}"\n',
        f"description: 来自 colleges.chat 的{name} 问卷调查信息\n",
        "---\n\n",
    ]
    lines.append("> 本页面内容来源于问卷，仅供参考。\n\n")
    lines.append("> 数据来源：\n\n")
    lines.append('{{% details title="展开" %}}\n\n')  # See `UPSTREAM:`
    ordered = sorted(
        (
            (resp, source.meta_q_nums, source.enroll_by_num)
            for source in sources
            for resp in source.responses
        ),
        key=lambda t: _answer_time(t[0]),
        reverse=True,
    )
    for resp, meta_q_nums, enroll_by_num in ordered:
        if resp.metadata is None:
            continue
        meta_parts: list[str] = []
        if enroll_by_num and resp.metadata.num in enroll_by_num:
            # 源列是纯年份（如 2021年），补「入学」才不被误读成答卷日期的一部分
            meta_parts.append(f"{enroll_by_num[resp.metadata.num]}入学")
        for q_num in meta_q_nums:
            answer = resp.answers.get(q_num)
            if answer is not None:
                text = _format_meta_value(answer.value)
                if text:
                    meta_parts.append(text)
        nickname = _format_meta_value(resp.answers[NICKNAME_NUM].value)
        meta_str = ", ".join(meta_parts)
        is_v2_version = nickname not in ('1', '2')
        if nickname != ANONYMOUS and is_v2_version:
            meta_str += ('{{< nickname ",' + _markdown_escape(nickname) + '" >}}')
        num_label = _answer_label(resp.metadata.num)
        if meta_str:
            lines.append(
                f"- {num_label} ({resp.metadata.answer_date:%Y年%m月}): "
                f"{meta_str}\n"
            )
        else:
            lines.append(f"- {num_label} ({resp.metadata.answer_date:%Y年%m月})\n")
    lines.append("\n{{% /details %}}\n\n")
    return lines


def render_combined_markdown(
    name: str,
    v1_responses: list[QuestionnaireResponse],
    v2_responses: list[QuestionnaireResponse],
    v1_questions: Questionnaire,
    v2_questions: Questionnaire | None,
    slug: str,
    archived: bool,
    v1_enroll: dict[int, str] | None = None,
) -> str:
    """将同一所学校的 v1/v2 答卷合并渲染到同一页面（用 tabs 分开）。

    只有一份问卷数据时直接走对应的单版本渲染。v1_enroll 是
    「答题序号 -> 入学时间」映射（仅 v1 腾讯导入批次有），渲染器按序号伺机读取，缺失即不显示。
    """
    from .legacy import format_answer_legacy
    from .new import V2_META_Q_NUMS
    from .new import format_answer_new

    if v2_questions is None:
        if v2_responses:
            logger.warning(
                f"{name}: 缺少 v2 问卷定义，忽略 {len(v2_responses)} 份 v2 答卷"
            )
        return render_university_markdown(
            name, v1_responses, v1_questions, slug, archived, 4,
            format_answer_legacy, enroll_by_num=v1_enroll,
        )
    if not v2_responses:
        return render_university_markdown(
            name, v1_responses, v1_questions, slug, archived, 4,
            format_answer_legacy, enroll_by_num=v1_enroll,
        )
    if not v1_responses:
        return render_university_markdown(
            name, v2_responses, v2_questions, slug, archived, 2,
            format_answer_new, meta_q_nums=V2_META_Q_NUMS,
        )

    lines = _build_header(
        name,
        slug,
        archived,
        [
            HeaderSource(v2_responses, V2_META_Q_NUMS),
            HeaderSource(v1_responses, (), v1_enroll),
        ],
    )
    v1_label = f"v1 ({len(v1_responses)} 份)"
    v2_label = f"v2 ({len(v2_responses)} 份)"
    lines.extend(
        [
            "{{< tabs >}}\n\n",
            # shortcode 保持字面量，只把变量拼进去：
            # f-string 会折叠 {{，% 格式化又要把 % 写成 %%，两头都不划算
            '{{% tab "' + v1_label + '" %}}\n\n',
            *render_university_body(v1_responses, v1_questions, 4, format_answer_legacy),
            "\n{{% /tab %}}\n\n",
            '{{% tab "' + v2_label + '" %}}\n\n',
            *render_university_body(
                v2_responses, v2_questions, 2, format_answer_new,
                meta_q_nums=V2_META_Q_NUMS,
            ),
            "\n{{% /tab %}}\n\n",
            "{{< /tabs >}}\n",
        ]
    )
    return "".join(lines)
