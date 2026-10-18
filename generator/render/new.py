from wenjuanxing_parser.models import (
    AnswerValue,
    ResponseStatus,
    SelectedOption,
)

from .common import FormattedAnswer

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
