from wenjuanxing_parser.models import (
    AnswerValue,
    Questionnaire,
    QuestionnaireResponse,
    ResponseStatus,
    SelectedOption,
)

from .common import (
    FormattedAnswer,
    HeaderSource,
    _build_header,
    render_question_groups,
)

V2_META_Q_NUMS = [3, 4, 5]


def format_answer_new(
    value: AnswerValue,
) -> FormattedAnswer | list[FormattedAnswer] | None:
    if value is None or isinstance(value, ResponseStatus):
        return None
    if isinstance(value, str):
        if not value.strip():
            return None
        return FormattedAnswer(summary=value)
    if isinstance(value, SelectedOption):
        return FormattedAnswer(summary=value.text, detail=value.additional_text)
    if isinstance(value, list):
        results: list[FormattedAnswer] = []
        for item in value:
            if isinstance(item, SelectedOption):
                results.append(
                    FormattedAnswer(summary=item.text, detail=item.additional_text)
                )
            elif isinstance(item, str) and item.strip():
                results.append(FormattedAnswer(summary=item))
        return results or None
    return FormattedAnswer(summary=str(value))


def render_university_markdown(
    name: str,
    responses: list[QuestionnaireResponse],
    questions_map: Questionnaire,
    slug: str,
    archived: bool,
    uni_q_num: int,
    meta_q_nums: list[int] | None = None,
) -> str:
    if meta_q_nums is None:
        meta_q_nums = V2_META_Q_NUMS
    lines = _build_header(
        name, slug, archived, [HeaderSource(responses, meta_q_nums)]
    )
    lines.extend(
        render_university_body(
            responses, questions_map, uni_q_num, meta_q_nums
        )
    )
    return "".join(lines)


def render_university_body(
    responses: list[QuestionnaireResponse],
    questions_map: Questionnaire,
    uni_q_num: int,
    meta_q_nums: list[int] | None = None,
) -> list[str]:
    if meta_q_nums is None:
        meta_q_nums = V2_META_Q_NUMS
    skip_q_nums = set(range(1, uni_q_num + 1)) | set(meta_q_nums)
    filtered_map = {k: v for k, v in questions_map.items() if k not in skip_q_nums}
    return render_question_groups(responses, filtered_map, 0, format_answer_new)
