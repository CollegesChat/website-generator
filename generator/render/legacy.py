from collections.abc import Mapping

from wenjuanxing_parser.models import (
    AnswerValue,
    QuestionnaireResponse,
    ResponseStatus,
    SelectedOption,
)

from .common import FormattedAnswer, _build_header, render_question_groups


def format_answer_legacy(value: AnswerValue) -> FormattedAnswer | None:
    if value is None or isinstance(value, ResponseStatus):
        return None
    if isinstance(value, str):
        if not value.strip():
            return None
        return FormattedAnswer(summary=value)
    if isinstance(value, SelectedOption):
        return FormattedAnswer(summary=value.text)
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, SelectedOption):
                parts.append(item.text)
            elif isinstance(item, str) and item.strip():
                parts.append(item)
        if not parts:
            return None
        if all(isinstance(item, (str, ResponseStatus)) for item in value):
            return FormattedAnswer(summary="┋".join(parts))
        return FormattedAnswer(summary=", ".join(parts))
    return FormattedAnswer(summary=str(value))


def render_university_markdown(
    name: str,
    responses: list[QuestionnaireResponse],
    questions_map: Mapping,
    slug: str,
    archived: bool,
    uni_q_num: int,
) -> str:
    lines = _build_header(name, slug, archived, responses)
    lines.extend(
        render_question_groups(
            responses, questions_map, uni_q_num, format_answer_legacy
        )
    )
    return "".join(lines)
