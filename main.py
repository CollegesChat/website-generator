# rewrite from https://github.com/CollegesChat/university-information/blob/bf73bb9c5c57800672e9ea164742d202476bceff/questionnaires/main.py
import csv
import os
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from random import sample
from sys import argv
from typing import cast

import niquests
import zhconv
from loguru import logger
from pypinyin import Style, lazy_pinyin
from slugify import slugify

logger.remove()

# 2. 定義你指定的格式
# <green>、<level>、<cyan> 是 loguru 的顏色標籤，可以讓終端機輸出保持色彩
# :<8 表示向左對齊並固定佔用 8 個字元空間
log_format = (
    '<green>{time:HH:mm:ss.SSS}</green> | '
    '<level>{level:<6}</level> | '
    '<cyan>{name}:{line}</cyan> - <level>{message}</level>'
)

# 3. 重新添加配置到標準輸出（sys.stdout）
logger.add(sys.stdout, format=log_format, colorize=True)
# ================== 配置 ==================
ARCHIVE_TIME = '2023-01-01 00:00:00'

QUESTIONNAIRE: list[str] = [
    '宿舍是上床下桌吗？',
    '教室和宿舍有没有空调？',
    '有独立卫浴吗？没有独立浴室的话，澡堂离宿舍多远？',
    '有早自习、晚自习吗？',
    '有晨跑吗？',
    '每学期跑步打卡的要求是多少公里，可以骑车吗？',
    '寒暑假放多久，每年小学期有多长？',
    '学校允许点外卖吗，取外卖的地方离宿舍楼多远？',
    '学校交通便利吗，有地铁吗，在市区吗，不在的话进城要多久？',
    '宿舍楼有洗衣机吗？',
    '校园网怎么样？',
    '每天断电断网吗，几点开始断？',
    '食堂价格贵吗，会吃出异物吗？',
    '洗澡热水供应时间？',
    '校园内可以骑电瓶车吗，电池在哪能充电？',
    '宿舍限电情况？',
    '通宵自习有去处吗？',
    '大一能带电脑吗？',
    '学校里面用什么卡，饭堂怎样消费？',
    '学校会给学生发银行卡吗？',
    '学校的超市怎么样？',
    '学校的收发快递政策怎么样？',
    '学校里面的共享单车数目与种类如何？',
    '现阶段学校的门禁情况如何？',
    '宿舍晚上查寝吗，封寝吗，晚归能回去吗？',
]

NAME_PREPROCESS = re.compile(r'[\(\)（）【】#]')
FILENAME_PREPROCESS = re.compile(r'[/>|:&]')
NORMAL_NAME_MATCHER = re.compile(r'大学|学院|学校')

ROOT = Path('required')
SITE_DIR = Path(os.getenv('SITE_DIR', r'/mnt/data/Project/questionnaire-report-theme'))

REQUIRED_FILES = [
    'README_archived_template.md',
    'README_template.md',
    'alias.txt',
    'blacklist.txt',
    'colleges.csv',
    'results_desensitized.csv',
    'whitelist.txt',
]

REQUIRED_DOCS = [
    '出国受阻.md',
    '如何正义劝退？.md',
    '影响生活质量的一些方面.md',
]

BASE_URL = 'https://raw.githubusercontent.com/CollegesChat/university-information/refs/heads/master/questionnaires/'
DOC_URL = BASE_URL + 'site/docs/choose-a-college/'


# ================== 数据类 ==================
@dataclass
class IndexedContent:
    answer_id: int
    content: str

    def __str__(self) -> str:
        return f'A{self.answer_id}: {self.content}'


@dataclass
class AnswerGroup:
    answers: list[IndexedContent] = field(default_factory=list)

    def add_answer(self, ans: IndexedContent) -> None:
        self.answers.append(ans)

    def extend(self, other: 'AnswerGroup') -> None:
        self.answers.extend(other.answers)


@dataclass
class University:
    answers: list[AnswerGroup] = field(
        default_factory=lambda: [AnswerGroup() for _ in range(len(QUESTIONNAIRE))]
    )
    additional_answers: list[IndexedContent] = field(default_factory=list)
    credits: list[IndexedContent] = field(default_factory=list)

    def add_answer(self, index: int, answer: IndexedContent) -> None:
        self.answers[index].add_answer(answer)

    def add_additional_answer(self, answer: IndexedContent) -> None:
        if answer.content:
            self.additional_answers.append(answer)

    def add_credit(self, credit: IndexedContent) -> None:
        self.credits.append(credit)

    def combine_from(self, other: 'University') -> None:
        for mine, other_group in zip(self.answers, other.answers, strict=True):
            mine.extend(other_group)
        self.additional_answers.extend(other.additional_answers)
        self.credits.extend(other.credits)


def make_pinyin_slug(text):

    pinyin_list = lazy_pinyin(text, style=Style.NORMAL)

    pinyin_str = ' '.join(pinyin_list)

    return slugify(pinyin_str)


class FileNameMap:
    def __init__(self) -> None:
        self.mapping: dict[str, str] = {}
        self.used_counters: dict[str, int] = {}  # 核心优化：记录每个 base 被用了几次

    def __getitem__(self, name: str) -> str:
        # 1. 缓存命中，直接返回
        if name in self.mapping:
            return self.mapping[name]

        # 2. 缓存未命中，生成并注入
        slug = self._generate_unique_slug(name)
        self.mapping[name] = slug
        return slug

    def _generate_unique_slug(self, name: str) -> str:
        """負責生成乾淨、唯一 slug 的私有方法"""
        base = make_pinyin_slug(FILENAME_PREPROCESS.sub('', name))

        if base not in self.used_counters:
            self.used_counters[base] = 1
            return base

        self.used_counters[base] += 1
        return f'{base}-{self.used_counters[base]}'


# ================== 辅助函数（简体中文注释） ==================
def download_files(names: list[str], base_url: str, root: Path) -> None:
    """下载缺失的文件到 root 目录"""
    root.mkdir(parents=True, exist_ok=True)
    for name in names:
        local_file = root / name
        if not local_file.exists():
            url = base_url + name
            logger.info(f'Downloading {name} from {url}...')
            r = niquests.get(url)
            if r.status_code == 200:
                local_file.write_bytes(cast(bytes, r.content))
                logger.info(f'Saved {name}')
            else:
                logger.error(f'Failed to download {name}, status code: {r.status_code}')


def markdown_escape(text: str) -> str:
    return text.replace('*', '\\*').replace('~', '\\~').replace('_', '\\_')


def generate_markdown_path(province: str, university_name: str, archived: bool) -> Path:
    """生成 Hugo 页面 bundle 的目标路径"""
    base = SITE_DIR / 'content' / 'docs'
    if archived:
        base = base / 'archived'
    return base / 'universities' / province / f'{university_name}.md'


def load_colleges() -> tuple[dict[str, list], dict[str, str]]:
    provinces: dict[str, list] = {}
    colleges: dict[str, str] = {}
    csv_path = ROOT / 'colleges.csv'
    if not csv_path.exists():
        raise FileNotFoundError('colleges.csv not found')
    with csv_path.open('r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for province, college in reader:
            key = NAME_PREPROCESS.sub('', college).replace(' ', '')
            colleges[key] = province
            provinces.setdefault(province, [])
    provinces.setdefault('其他', [])
    return provinces, colleges


def load_to_universities(universities: defaultdict[str, University], row: list) -> None:
    """把一行问卷记录加载到 universities 字典"""
    aid, _, anonymous, email, show_email, name, *answers = row[:-9]
    additional_answer = IndexedContent(int(aid), row[-9])
    anonymous_flag = int(anonymous) == 2
    show_email_flag = not anonymous_flag and float(show_email) == 1.0
    name = zhconv.convert(name, 'zh-cn')
    name = NAME_PREPROCESS.sub('', name).strip()
    uni = universities[name]
    submit_time = datetime.strptime(row[-8], '%Y-%m-%d %H:%M:%S')
    credit_text = (
        f'{email} ({submit_time:%Y 年 %m 月})'
        if show_email_flag and email
        else f'匿名 ({submit_time:%Y 年 %m 月})'
    )
    uni.add_credit(IndexedContent(int(aid), credit_text))
    for i, ans in enumerate(answers):
        uni.add_answer(i, IndexedContent(int(aid), ans))
    uni.add_additional_answer(additional_answer)


def process_universities(universities: dict, colleges: dict) -> None:
    """处理别名、黑名单与可能无效的名称提示"""
    alias_path = ROOT / 'alias.txt'
    if alias_path.exists():
        with alias_path.open('r', encoding='utf-8') as f:
            for line in f:
                name, *aliases = line.rstrip('\n').split('🚮')
                primary = universities.get(name)
                if primary is None:
                    # Debug mode may only load a subset; skip missing primary names.
                    logger.warning(f'alias primary missing: {name}')
                    continue
                for alias in aliases:
                    if alias in universities:
                        primary.combine_from(universities[alias])
                        del universities[alias]
    blacklist = ROOT / 'blacklist.txt'
    if blacklist.exists():
        with blacklist.open('r', encoding='utf-8') as f:
            for line in f:
                universities.pop(line.strip(), None)
    wl = ROOT / 'whitelist.txt'
    whitelist = (
        {_.strip() for _ in wl.read_text(encoding='utf-8').splitlines()}
        if wl.exists()
        else set()
    )
    for name in list(universities.keys()):
        if NORMAL_NAME_MATCHER.search(name) is None and name not in whitelist:
            logger.warning(
                f'maybe invalid: {name} '
                + ','.join(f'A{_.answer_id}' for _ in universities[name].credits)
            )


def ensure_dirs() -> None:
    (SITE_DIR / 'content' / 'docs' / 'universities').mkdir(parents=True, exist_ok=True)
    (SITE_DIR / 'content' / 'docs' / 'archived' / 'universities').mkdir(
        parents=True, exist_ok=True
    )
    (SITE_DIR / 'content' / 'docs' / 'choose-a-college').mkdir(
        parents=True, exist_ok=True
    )


def sanitize_filename(filename: str) -> str:
    """
    清理檔名：
    1. 將正斜線 '/' 與 反斜線 '\' 取代為下底線 '_'
    2. 清除其他非法字元與換行符號
    3. 限制長度（中文算 2 點，英文算 1 點），總長不超過 32 點（相當於 16 個中文，放過英文）
    """
    # 1. 先單獨將 / 和 \ 取代為下底線 _
    # 註：在正則表達式中，反斜線需要四個 \\\\ 或在 r-string 中用 \\ 來表示
    cleaned = re.sub(r'[\\/]', '_', filename)

    # 2. 清理其他非法字元與換行符號（替換為空字串，避免檔名有太多底線）
    # 如果你也希望這些變底線，可以把它們合併到上面的步驟
    other_illegal_pattern = r'[:*?"<>|\0\r\n\t]'
    cleaned = re.sub(other_illegal_pattern, '', cleaned)

    # 3. 權重截斷（上限 32 點 = 16 個中文 或 32 個英文）
    max_weight = 32
    current_weight = 0
    truncated_index = len(cleaned)

    for i, char in enumerate(cleaned):
        # 判斷是否為中文字元/全形字元（ASCII 127 以上算中文/全形）
        weight = 2 if ord(char) > 127 else 1
        if current_weight + weight > max_weight:
            truncated_index = i
            break
        current_weight += weight

    cleaned = cleaned[:truncated_index]
    if cleaned != filename:
        logger.error(f'{filename} 文件名可能非法')
    return cleaned


def find_province(name: str, colleges: dict[str, str]) -> str:
    for key, prov in colleges.items():
        if name.find(key) >= 0:
            return prov
    return '其他'


def render_university_markdown(
    name: str, uni: University, slug: str, archived: bool
) -> str:
    lines: list[str] = [
        '---\n',
        f'title: "{name}{" (已归档)" if archived else ""}"\n',
        f'slug: "{slug}"\n',
        f'description: 来自 colleges.chat 的{name} 问卷调查信息\n',
        '---\n\n',
    ]
    lines.append('> 本页面内容来源于问卷，仅供参考。\n\n')
    lines.append('> 数据来源：\n<details><summary>展开</summary>\n<ul>\n')
    for c in uni.credits:
        lines.append(f'<li>{c}</li>\n')
    lines.append('</ul>\n</details>\n\n')
    for q, group in zip(QUESTIONNAIRE, uni.answers, strict=True):
        lines.append(f'## Q: {q}\n\n')
        for ans in group.answers:
            lines.append(f'- {markdown_escape(str(ans))}\n')
    if uni.additional_answers:
        lines.append('\n## 自由补充\n\n')
        for a in uni.additional_answers:
            lines.append(markdown_escape(str(a)) + '\n\n')
    return ''.join(lines)


def write_university_markdown(
    name: str,
    uni: University,
    slug: str,
    target: Path,
    archived: bool,
) -> None:
    target.write_text(
        render_university_markdown(name, uni, slug, archived), encoding='utf-8'
    )


def write_markdown_for_universities(
    universities: dict[str, University],
    filename_map: FileNameMap,
    colleges: dict[str, str],
    archived: bool,
) -> None:
    """把 universities 并发写成 Hugo 的 markdown 页面"""
    tasks: list[tuple[str, University, str, Path]] = []
    for name, uni in universities.items():
        cleaned_name = sanitize_filename(name)
        slug = filename_map[cleaned_name]
        province = find_province(cleaned_name, colleges)
        target = generate_markdown_path(province, cleaned_name, archived)
        tasks.append((cleaned_name, uni, slug, target))

    for parent in {target.parent for _, _, _, target in tasks}:
        parent.mkdir(parents=True, exist_ok=True)
        # (parent / '_index.md').touch()
        (parent / '_index.md').write_text('---\nbookCollapseSection: true\n---')

    max_workers = min(32, max(1, (os.cpu_count() or 1) * 4))
    section = 'archived' if archived else 'active'
    total = len(tasks)
    logger.info(f'Start generating {section} markdown files: {total}')
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                write_university_markdown, cleaned_name, uni, slug, target, archived
            )
            for cleaned_name, uni, slug, target in tasks
        ]
        completed = 0
        for future in as_completed(futures):
            future.result()
            completed += 1
            progress = completed / total * 100 if total else 100.0
            logger.info(
                f'[progress] {section}: {completed}/{total} ({progress:.1f}%)',
                end='',
                flush=True,
            )
    logger.info('Finished generating markdown files.')


ensure_dirs()
download_files(REQUIRED_FILES, BASE_URL, ROOT)
download_files(
    REQUIRED_DOCS, DOC_URL, SITE_DIR / 'content' / 'docs' / 'choose-a-college'
)
download_files(['index.md'], BASE_URL + '/site/docs/', SITE_DIR / 'content' / 'docs')

target_file = SITE_DIR / 'content' / 'docs' / 'index.md'
new_file = target_file.with_name('_index.md')
target_file.rename(new_file)
header = '---\ntitle: 首页\nurl: /\n---\n\n'
content = new_file.read_text(encoding='utf-8')
new_file.write_text(header + content, encoding='utf-8')

provinces, colleges = load_colleges()
archive_cut = datetime.strptime(ARCHIVE_TIME, '%Y-%m-%d %H:%M:%S')

universities: defaultdict[str, University] = defaultdict(University)
universities_archived: defaultdict[str, University] = defaultdict(University)

with (ROOT / 'results_desensitized.csv').open('r', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader, None)
    for row in reader:
        t = datetime.strptime(row[-8], '%Y-%m-%d %H:%M:%S')
        target = universities_archived if t < archive_cut else universities
        load_to_universities(target, row)

if 'debug' in argv:
    universities: dict[str, University] = dict(sample(list(universities.items()), 100))
    universities_archived: dict[str, University] = dict(
        sample(list(universities_archived.items()), 100)
    )
    logger.info(
        f'Debug mode: only processing 100 universities each <{len(universities)} and {len(universities_archived)} >.'
    )
process_universities(universities, colleges)
process_universities(universities_archived, colleges)


write_markdown_for_universities(universities, FileNameMap(), colleges, archived=False)
write_markdown_for_universities(
    universities_archived, FileNameMap(), colleges, archived=True
)
