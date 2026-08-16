from .common import (
    FileNameMap,
    FormattedAnswer,
    write_markdown_for_universities,
)
from .legacy import format_answer_legacy
from .legacy import render_university_markdown as render_legacy
from .new import format_answer_new
from .new import render_university_markdown as render_new

__all__ = [
    "FileNameMap",
    "FormattedAnswer",
    "format_answer_legacy",
    "format_answer_new",
    "render_legacy",
    "render_new",
    "write_markdown_for_universities",
]
