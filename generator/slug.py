from pypinyin import Style, lazy_pinyin
from slugify import slugify

from .config import FILENAME_PREPROCESS


def make_pinyin_slug(text: str) -> str:
    """将中文文本转换为拼音 slug"""
    pinyin_str = " ".join(lazy_pinyin(text, style=Style.NORMAL))
    return slugify(pinyin_str)


class FileNameMap:
    """生成并缓存唯一的拼音 slug，重名时追加数字后缀"""

    def __init__(self) -> None:
        self._cache: dict[str, str] = {}
        self._counters: dict[str, int] = {}

    def __getitem__(self, name: str) -> str:
        if name in self._cache:
            return self._cache[name]
        slug = self._generate_unique(name)
        self._cache[name] = slug
        return slug

    def _generate_unique(self, name: str) -> str:
        base = make_pinyin_slug(FILENAME_PREPROCESS.sub("", name))
        if base not in self._counters:
            self._counters[base] = 1
            return base
        self._counters[base] += 1
        return f"{base}-{self._counters[base]}"
