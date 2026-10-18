from ..slug import FileNameMap
from .common import (
    FormattedAnswer,
    render_combined_markdown,
    render_university_markdown,
    sanitize_filename,
)
from .legacy import format_answer_legacy
from .new import V2_META_Q_NUMS
from .new import format_answer_new


def render_legacy(
    name: str,
    responses: list,
    questions_map: dict,
    slug: str,
    archived: bool,
    uni_q_num: int,
    enroll_by_num: dict[int, str] | None = None,
) -> str:
    return render_university_markdown(
        name, responses, questions_map, slug, archived, uni_q_num,
        format_answer_legacy, enroll_by_num=enroll_by_num,
    )


def render_new(
    name: str,
    responses: list,
    questions_map: dict,
    slug: str,
    archived: bool,
    uni_q_num: int,
    meta_q_nums: list[int] | None = None,
) -> str:
    if meta_q_nums is None:
        meta_q_nums = V2_META_Q_NUMS
    return render_university_markdown(
        name, responses, questions_map, slug, archived, uni_q_num,
        format_answer_new, meta_q_nums=meta_q_nums,
    )


__all__ = [
    "FileNameMap",
    "FormattedAnswer",
    "format_answer_legacy",
    "format_answer_new",
    "render_combined_markdown",
    "render_legacy",
    "render_new",
    "sanitize_filename",
]
