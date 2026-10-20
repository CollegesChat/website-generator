"""渲染层：问卷模型 -> Hugo Markdown（转义 / 分组 / 页头 / 单版本与合并渲染）。"""

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import zhconv
from loguru import logger
from wenjuanxing_parser.models import (
    AnswerValue,
    AnyQuestion,
    CheckboxQuestion,
    Questionnaire,
    QuestionnaireResponse,
    RadioQuestion,
    ResponseStatus,
    SelectedOption,
)

from ..config import (
    ANONYMOUS,
    IMPORTED_NUM_FROM,
    MARKDOWN_ESCAPE_RE,
    NICKNAME_NUM,
    SITE_DIR,
    V1_NICKNAME_CHOICES,
    V1_UNI_Q_NUM,
    V2_UNI_Q_NUM,
)


def _to_simplified(text: str) -> str:
    return zhconv.convert(text, "zh-cn")


_TZ = ZoneInfo("Asia/Shanghai")
_EPOCH = datetime.min.replace(tzinfo=_TZ)


def _answer_time(resp: QuestionnaireResponse) -> datetime:
    """答卷时间，统一成带时区后再比较（v1 元数据是 naive，v2 是 Asia/Shanghai）。"""
    if resp.metadata is None:
        return _EPOCH
    answer_date = resp.metadata.answer_date
    if answer_date.tzinfo is None:
        return answer_date.replace(tzinfo=_TZ)
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
    """清洗非法字符，并按显示宽度截断：ASCII 记 1、其余记 2，上限 32。"""
    cleaned = re.sub(r"[\\/]", "_", filename)
    cleaned = re.sub(r'[:*?"<>|\0\r\n\t]', "", cleaned)

    width = 0
    for i, char in enumerate(cleaned):
        width += 2 if ord(char) > 127 else 1
        if width > 32:
            return cleaned[:i]
    return cleaned


def _markdown_escape(text: str) -> str:
    return MARKDOWN_ESCAPE_RE.sub(r"\\\1", text)


def _html_escape(text: str) -> str:
    # 仅转义会破坏 HTML 结构的字符；引号故意保留，
    # 因为走 shortcode 源风格（Hugo 不对 shortcode 内部做引号转义）。
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _small_shortcode(text: str, cls: str | None = None) -> str:
    open_tag = '{{< small' + (f' "{cls}"' if cls else "") + " >}}"
    return open_tag + _html_escape(text) + "{{< /small >}}"


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


def _option_prompts(question: AnyQuestion) -> dict[str, str]:
    """选项附加文本提示语，按选项文本索引（仅 radio/checkbox 有）。"""
    prompts: dict[str, str] = {}
    if isinstance(question, (RadioQuestion, CheckboxQuestion)):
        for opt in question.options:
            at = opt.additional_text
            if not isinstance(at, bool) and at.prompt is not None:
                prompts[opt.text] = at.prompt
    return prompts


def _collect_question_groups(
    responses: list[QuestionnaireResponse],
    q_num: int,
    format_fn: FormatFn,
) -> tuple[dict[str, list[_ResponseEntry]], int]:
    """按答案文本分组收集答卷，顺带统计整题作废条数。

    依赖 format_fn 对 ResponseStatus 一律返回 None：EMPTY 走 skipped 计数，
    其余状态自然落不到任何组，与原两遍遍历等价。
    """
    groups: dict[str, list[_ResponseEntry]] = {}
    skipped = 0
    for resp in responses:
        if resp.metadata is None:
            continue
        answer = resp.answers[q_num]
        if answer.value is ResponseStatus.EMPTY:
            skipped += 1
            continue
        formatted = format_fn(answer.value)
        if formatted is None:
            continue
        formatted_list = formatted if isinstance(formatted, list) else [formatted]
        for fmt in formatted_list:
            groups.setdefault(fmt.summary, []).append(
                _ResponseEntry(num=resp.metadata.num, detail=fmt.detail),
            )
    return groups, skipped


def _free_supplement_anchor(title: str) -> str | None:
    """自由补充题锚点：v1 标题含「还有什么要说的吗」，v2 标题即「自由补充」。"""
    if "还有什么要说的吗" in title:
        return "自由补充 {#自由补充-v1}"
    if "自由补充" in title:
        return "自由补充 {#自由补充-v2}"
    return None


def _render_single(entry: _ResponseEntry, summary: str) -> list[str]:
    escaped = _markdown_escape(summary)
    if entry.detail:
        detail = _indent_multiline(_markdown_escape(entry.detail))
        return [f"- {_answer_label(entry.num)}: {escaped}: {detail}\n"]
    return [f"- {_answer_label(entry.num)}: {_indent_multiline(escaped)}\n"]


def _render_details(
    summary: str, entries: list[_ResponseEntry], opt_prompt: str | None
) -> list[str]:
    count = len(entries)
    escaped = _markdown_escape(summary)
    title_escaped = re.sub(r'["\r\n]', "", escaped)
    lines = [
        # UPSTREAM: https://github.com/alex-shpak/hugo-book/issues/830
        '- {{< details title="' + f"{title_escaped} x {count}" + '" >}}\n\n'
    ]
    if opt_prompt:
        lines.append(f"  {_small_shortcode(opt_prompt, 'additional-text-prompt')}\n")
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
    return lines


def render_question_groups(
    responses: list[QuestionnaireResponse],
    questions_map: Questionnaire,
    format_fn: FormatFn,
) -> list[str]:
    lines: list[str] = []
    for q_num, question in sorted(questions_map.items()):
        option_prompt = _option_prompts(question)
        groups, skipped = _collect_question_groups(responses, q_num, format_fn)
        if not groups and not skipped:
            continue

        anchor = _free_supplement_anchor(question.title)
        lines.append(f"## {anchor}\n\n" if anchor else f"## Q: {question.title}\n")
        if question.prompt:
            lines.append(_small_shortcode(question.prompt) + "\n\n")
        if skipped:
            lines.append(
                _small_shortcode(f"{skipped} 位用户跳过", "response-status") + "\n\n"
            )
        for summary_text, entries in groups.items():
            if len(entries) == 1:
                lines.extend(_render_single(entries[0], summary_text))
            else:
                lines.extend(
                    _render_details(
                        summary_text, entries, option_prompt.get(summary_text)
                    )
                )
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
    return render_question_groups(responses, filtered_map, format_fn)


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
            text = _format_meta_value(resp.answers[q_num].value)
            if text:
                meta_parts.append(text)
        nickname = _format_meta_value(resp.answers[NICKNAME_NUM].value)
        meta_str = ", ".join(meta_parts)
        # v1 的昵称位（Q1 单选）存的是选项序号，不是真实昵称
        has_real_nickname = nickname not in V1_NICKNAME_CHOICES
        if nickname != ANONYMOUS and has_real_nickname:
            meta_str += ('{{< nickname ",' + _markdown_escape(nickname) + '" >}}')
        num_label = _answer_label(resp.metadata.num)
        suffix = f": {meta_str}" if meta_str else ""
        lines.append(f"- {num_label} ({resp.metadata.answer_date:%Y年%m月}){suffix}\n")
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
    from .new import V2_META_Q_NUMS, format_answer_new

    def _single_version(
        responses: list[QuestionnaireResponse],
        questions: Questionnaire,
        uni_q_num: int,
        fmt: FormatFn,
        meta: Sequence[int] = (),
        enroll: dict[int, str] | None = None,
    ) -> str:
        return render_university_markdown(
            name, responses, questions, slug, archived, uni_q_num, fmt,
            meta_q_nums=meta, enroll_by_num=enroll,
        )

    if v2_questions is None or not v2_responses:
        if v2_responses:
            logger.warning(
                f"{name}: 缺少 v2 问卷定义，忽略 {len(v2_responses)} 份 v2 答卷"
            )
        return _single_version(
            v1_responses, v1_questions, V1_UNI_Q_NUM, format_answer_legacy, enroll=v1_enroll
        )
    if not v1_responses:
        return _single_version(
            v2_responses, v2_questions, V2_UNI_Q_NUM, format_answer_new, meta=V2_META_Q_NUMS
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
            *render_university_body(v1_responses, v1_questions, V1_UNI_Q_NUM, format_answer_legacy),
            "\n{{% /tab %}}\n\n",
            '{{% tab "' + v2_label + '" %}}\n\n',
            *render_university_body(
                v2_responses, v2_questions, V2_UNI_Q_NUM, format_answer_new,
                meta_q_nums=V2_META_Q_NUMS,
            ),
            "\n{{% /tab %}}\n\n",
            "{{< /tabs >}}\n",
        ]
    )
    return "".join(lines)
