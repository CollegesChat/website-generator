import os
import re
import sys
from pathlib import Path

from loguru import logger

logger.remove()
log_format = (
    "<green>{time:HH:mm:ss.SSS}</green> | "
    "<level>{level:<6}</level> | "
    "<cyan>{name}:{line}</cyan> - <level>{message}</level>"
)
logger.add(sys.stdout, format=log_format, colorize=True)

ARCHIVE_YEARS = 3
ROOT = Path("required")
SITE_DIR = Path(os.getenv("SITE_DIR", r"/mnt/data/Project/questionnaire-report-theme"))

NAME_PREPROCESS = re.compile(r"[\(\)（）【】#]")
FILENAME_PREPROCESS = re.compile(r"[/>|:&]")
MARKDOWN_ESCAPE_RE = re.compile(r'([\\`*_[\]{}()<>#+.!|~-])')

BASE_URL = "https://github.com/CollegesChat/university-information/raw/refs/heads/v2/"
DOC_URL = BASE_URL + "docs/choose-a-college/"
CSV_URL = "https://github.com/CollegesChat/china-university-list/releases/latest/download/output.csv"
DATA_URL = [
    "https://github.com/CollegesChat/university-information/raw/refs/heads/v2/datas/v1.csv",
    "https://github.com/CollegesChat/university-information/raw/refs/heads/v2/datas/v2.csv",
]
QUESTIONNAIRES_URL = [
    "https://github.com/CollegesChat/questionnaire/raw/refs/heads/main/v1.yaml",
    "https://github.com/CollegesChat/questionnaire/raw/refs/heads/main/v2.yaml",
]
REQUIRED_DOCS = [
    "出国受阻.md",
    "如何正义劝退？.md",
    "影响生活质量的一些方面.md",
]

(SITE_DIR / "content" / "docs" / "universities").mkdir(parents=True, exist_ok=True)
(SITE_DIR / "content" / "docs" / "archived" / "universities").mkdir(
    parents=True, exist_ok=True
)
(SITE_DIR / "content" / "docs" / "choose-a-college").mkdir(parents=True, exist_ok=True)
