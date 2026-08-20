import random
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from wenjuanxing_parser.constants import MISSING_BASIC_DATA_KWARGS
from wenjuanxing_parser.models import (
    BasicData,
    CheckboxQuestion,
    FillBlankQuestion,
    QuestionnaireResponse,
    RadioQuestion,
    ResponseStatus,
    SelectedOption,
    TextAreaQuestion,
    UserAnswer,
)

from .province import load_province_mapping

SCHOOLS = [
    ("同济大学", "四平路校区"),
    ("浙江大学", "紫金港校区"),
    ("中山大学", "广州校区"),
    ("北京航空航天大学", "学院路校区"),
    ("南京大学", "鼓楼校区"),
    ("武汉大学", "珞珈山校区"),
    ("四川大学", "望江校区"),
    ("哈尔滨工业大学", "哈尔滨校区"),
    ("西安交通大学", "兴庆校区"),
    ("同济大学浙江学院", "嘉兴校区"),
    ("济南大学", "主校区"),
    ("暨南大学", "石牌校区"),
    ("天津大学", "卫津路校区"),
    ("厦门大学", "思明校区"),
    ("郑州大学", "主校区"),
]

NICKNAMES = [
    "匿名咸鱼",
    "路过的猫",
    "大四老学长",
    "考研狗",
    "摸鱼达人",
    "干饭人",
    "卷王",
    "摆烂选手",
    "图书馆常驻",
    "实验室打工人",
    "毕业ing",
    "学术垃圾",
    "早睡早起",
    "夜猫子",
    "咖啡续命",
]

MAJORS = [
    "计算机科学与技术",
    "机械工程",
    "法学",
    "临床医学",
    "电子信息工程",
    "土木工程",
    "金融学",
    "汉语言文学",
    "数学与应用数学",
    "生物工程",
    "建筑学",
    "新闻传播学",
    "自动化",
    "化学",
    "工商管理",
]

YEARS = ["2022-09", "2023-09", "2024-09", "2025-09"]

GENDERS = ["男", "女", "（不愿透露）"]

SHORT_TEXTS = [
    "还可以",
    "一般般",
    "挺好的",
    "不太行",
    "凑合",
    "没感觉",
    "还行吧",
    "蛮好的",
    "差强人意",
    "就那样",
    "不太好",
    "非常棒",
    "勉强可以",
    "看情况",
    "说不准",
]

LONG_TEXTS = [
    "总体来说还不错，就是有些细节需要改善",
    "大一的时候感觉很好，高年级就一般了",
    "看具体校区和宿舍楼，差异很大",
    "建议新生提前了解，不同楼栋配置完全不同",
    "这几年改善了不少，比以前好多了",
    "旧楼比较拉胯，新楼还不错",
    "因人而异吧，我个人的体验一般",
    "学校在这方面投入还是挺多的",
    "管理比较严格，但也是为了学生好",
    "没什么特别的，和其他学校差不多",
    "食堂很好吃，其他一般",
    "网络不太稳定，高峰期经常断",
    "图书馆座位不够用，期末要抢",
    "宿舍限电太严了，吹风机都不让用",
    "校园网还行，ipv6支持不错",
]

EDU_LEVELS = ["大专", "本科", "硕士研究生", "博士研究生"]
EDU_GRADES = ["大一", "大二", "大三", "大四", "研一", "研二", "研三"]


def _random_date(start_year: int = 2024, end_year: int = 2026) -> datetime:
    start = datetime(start_year, 1, 1, tzinfo=ZoneInfo("Asia/Shanghai"))
    end = datetime(end_year, 6, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def _make_metadata(num: int) -> BasicData:
    return BasicData(
        answer_date=_random_date(),
        num=num,
        **MISSING_BASIC_DATA_KWARGS
    )


def _has_additional(option) -> bool:
    return bool(option.additional_text)


def _random_additional(option) -> str | None:
    if not _has_additional(option):
        return None
    if random.random() < 0.4:
        return None
    if isinstance(option.additional_text, bool):
        return random.choice(SHORT_TEXTS + LONG_TEXTS)
    return random.choice(SHORT_TEXTS + LONG_TEXTS)


def _gen_radio(question: RadioQuestion) -> UserAnswer:
    opt = random.choice(question.options)
    additional = _random_additional(opt)
    return UserAnswer(value=SelectedOption(text=opt.text, additional_text=additional))


def _gen_checkbox(question: CheckboxQuestion) -> UserAnswer:
    k = random.randint(1, min(3, len(question.options)))
    chosen = random.sample(question.options, k)
    result: list[SelectedOption] = []
    for opt in chosen:
        additional = _random_additional(opt)
        result.append(SelectedOption(text=opt.text, additional_text=additional))
    return UserAnswer(value=result)


def _gen_text_area(
    question: TextAreaQuestion, q_num: int, school_name: str, campus: str
) -> UserAnswer:
    if q_num == 1:
        return UserAnswer(value=random.choice(NICKNAMES))
    if q_num == 2:
        return UserAnswer(value=school_name)
    if q_num == 5:
        return UserAnswer(value=campus)
    pool = SHORT_TEXTS if random.random() < 0.4 else LONG_TEXTS
    return UserAnswer(value=random.choice(pool))


def _gen_fill_blank(question: FillBlankQuestion) -> UserAnswer:
    blanks: list[str | ResponseStatus] = []
    for i in range(1, question.blank_count + 1):
        if i == 1:
            blanks.append(random.choice(YEARS))
        elif i == 2:
            blanks.append(random.choice(MAJORS))
        elif i == 3:
            if random.random() < 0.2:
                default = question.default_blank_text
                if isinstance(default, dict) and i in default:
                    blanks.append(default[i])
                elif isinstance(default, list) and i - 1 < len(default):
                    value = default[i - 1]
                    if isinstance(value, str):
                        blanks.append(value)
                    else:
                        blanks.append(ResponseStatus.SKIPPED)
                else:
                    blanks.append(ResponseStatus.SKIPPED)
            else:
                blanks.append(random.choice(GENDERS))
        else:
            blanks.append(random.choice(SHORT_TEXTS))
    return UserAnswer(value=blanks)


def _gen_answer(question, q_num: int, school_name: str, campus: str) -> UserAnswer:
    if isinstance(question, RadioQuestion):
        return _gen_radio(question)
    if isinstance(question, CheckboxQuestion):
        return _gen_checkbox(question)
    if isinstance(question, TextAreaQuestion):
        return _gen_text_area(question, q_num, school_name, campus)
    if isinstance(question, FillBlankQuestion):
        return _gen_fill_blank(question)
    return UserAnswer(value=random.choice(SHORT_TEXTS))


def generate_mock_v2_data(
    questions_map: Mapping[int, Any],
) -> tuple[
    list[QuestionnaireResponse],
    list[tuple[str, str]],
]:
    num_schools = random.randint(5, 10)
    selected = random.sample(SCHOOLS, min(num_schools, len(SCHOOLS)))
    province_file = Path(__file__).resolve().parent.parent / "required" / "colleges.csv"
    province_mapping = (
        load_province_mapping(province_file.read_bytes()) if province_file.exists() else []
    )

    responses: list[QuestionnaireResponse] = []
    response_num = 10001

    for school_name, campus in selected:
        num_responses = random.randint(3, 8)
        for _ in range(num_responses):
            answers: dict[int, UserAnswer] = {}
            for q_num, question in questions_map.items():
                answers[q_num] = _gen_answer(question, q_num, school_name, campus)

            metadata = _make_metadata(response_num)
            resp = QuestionnaireResponse(answers=answers, metadata=metadata)
            responses.append(resp)
            response_num += 1

    return responses, province_mapping
