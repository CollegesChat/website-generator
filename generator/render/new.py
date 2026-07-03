from collections.abc import Mapping

from wenjuanxing_parser.models import (
    AnswerValue,
    QuestionnaireResponse,
    ResponseStatus,
    SelectedOption,
)

from .common import FormattedAnswer, _markdown_escape, render_question_groups

V2_META_Q_NUMS = [3, 4, 5]


def format_answer_new(value: AnswerValue) -> FormattedAnswer | None:
    if value is None or isinstance(value, ResponseStatus):
        return None
    if isinstance(value, str):
        if not value.strip():
            return None
        return FormattedAnswer(summary=value)
    if isinstance(value, SelectedOption):
        return FormattedAnswer(summary=value.text, detail=value.additional_text)
    if isinstance(value, list):
        summaries: list[str] = []
        details: list[str] = []
        for item in value:
            if isinstance(item, SelectedOption):
                summaries.append(item.text)
                if item.additional_text:
                    details.append(item.additional_text)
            elif isinstance(item, str) and item.strip():
                summaries.append(item)
        if not summaries:
            return None
        summary = ", ".join(summaries)
        detail = "，".join(details) if details else None
        return FormattedAnswer(summary=summary, detail=detail)
    return FormattedAnswer(summary=str(value))


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


def _build_header_v2(
    name: str,
    slug: str,
    archived: bool,
    responses: list[QuestionnaireResponse],
    meta_q_nums: list[int],
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
        if resp.metadata is None:
            continue
        meta_parts: list[str] = []
        for q_num in meta_q_nums:
            answer = resp.answers.get(q_num)
            if answer is not None:
                text = _format_meta_value(answer.value)
                if text:
                    meta_parts.append(text)
        meta_str = ", ".join(meta_parts)
        if meta_str:
            lines.append(
                f"<li>A{resp.metadata.num} ({resp.metadata.answer_date:%Y年%m月}): "
                f"{_markdown_escape(meta_str)}</li>\n"
            )
        else:
            lines.append(
                f"<li>A{resp.metadata.num} ({resp.metadata.answer_date:%Y年%m月})</li>\n"
            )
    lines.append("</ul>\n</details>\n\n")
    return lines


def render_university_markdown(
    name: str,
    responses: list[QuestionnaireResponse],
    questions_map: Mapping,
    slug: str,
    archived: bool,
    uni_q_num: int,
    meta_q_nums: list[int] | None = None,
) -> str:
    if meta_q_nums is None:
        meta_q_nums = V2_META_Q_NUMS
    skip_q_nums = set(range(1, uni_q_num + 1)) | set(meta_q_nums)

    lines = _build_header_v2(name, slug, archived, responses, meta_q_nums)

    filtered_map = {k: v for k, v in questions_map.items() if k not in skip_q_nums}
    lines.extend(render_question_groups(responses, filtered_map, 0, format_answer_new))
    return "".join(lines)
