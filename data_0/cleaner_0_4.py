# cleaner_0_5.py
# Назначение: безопасно и быстро "почистить" поле `name`: пробелы, %, единицы измерения, кавычки/тире.
# Отличия от 0_4: удаление внешних кавычек; пробел ПОСЛЕ знака % перед объёмом/весом.

import re
import argparse
import sys
from pathlib import Path
import pandas as pd

# --- Регулярки ---
PERCENT_FIX_RE      = re.compile(r'(\d)\s*[.,]\s*(\d)\s*%')      # 3 . 2 %  -> 3,2%
PERCENT_SPACE_RE    = re.compile(r'\s*%\s*')                     # вокруг % нет пробелов
PERCENT_AFTER_RE    = re.compile(r'%(?=[0-9А-Яа-яA-Za-z])')      # %<цифра/буква> -> % <…>
OUTER_QUOTES_RE     = re.compile(r'^[\s"“”«]+|[\s"“”»]+$')       # внешние кавычки/пробелы
DOUBLE_QUOTES_RE    = re.compile(r'""')                          # "" -> "
SPACES_RE           = re.compile(r'\s+')

def rb_fix(s: str) -> str:
    s = (s or "")
    # снять внешние кавычки и пробелы по краям
    s = OUTER_QUOTES_RE.sub('', s)
    s = s.strip()

    # заменить двойные кавычки внутри
    s = DOUBLE_QUOTES_RE.sub('"', s)

    # схлопнуть множественные пробелы
    s = SPACES_RE.sub(' ', s)

    # проценты: 3.2 % / 3 . 2 % -> 3,2%
    s = PERCENT_FIX_RE.sub(r'\1,\2%', s)
    # убрать пробелы вокруг % (получаем "3,2%")
    s = PERCENT_SPACE_RE.sub('%', s)
    # добавить пробел ПОСЛЕ % если дальше сразу идёт число/буква: "1,9%260 г" -> "1,9% 260 г"
    s = PERCENT_AFTER_RE.sub('% ', s)

    # единицы измерения (без регистра)
    s = re.sub(r'(?i)\b(гр|г\.|g)\b', 'г', s)
    s = re.sub(r'(?i)\b(кг\.|kg)\b', 'кг', s)
    s = re.sub(r'(?i)\b(мл\.|ml)\b', 'мл', s)
    s = re.sub(r'(?i)\b(литр|л\.|l)\b', 'л', s)

    # пробел между числом и единицей: 500г -> 500 г; 1л -> 1 л
    s = re.sub(r'(\d)(г|кг|мл|л)\b', r'\1 \2', s)

    # унификация тире и финальный обрез
    s = s.replace('–', '-').replace('—', '-').strip()
    return s

def build_cli():
    p = argparse.ArgumentParser(description="Cleaner: нормализует поле 'name' (пробелы, %, единицы, кавычки).")
    p.add_argument("-i", "--input",  default="data_0_3.csv", help="Входной CSV (по умолчанию data_1_0.csv)")
    p.add_argument("-o", "--output", default="data_0_4.csv",          help="Выходной CSV (по умолчанию <input>.cleaned.csv)")
    p.add_argument("-r", "--report", default="data_0_3_changes.csv",          help="CSV-отчёт изменений (по умолчанию <input>.cleaned.changes.csv)")
    p.add_argument("--inplace", action="store_true",        help="Перезаписать входной файл")
    return p

def main():
    args = build_cli().parse_args()
    in_path = Path(args.input)
    if not in_path.exists():
        print(f"[error] input not found: {in_path}", file=sys.stderr); sys.exit(1)

    out_path = Path(args.output) if args.output else (
        in_path if args.inplace else in_path.with_name(in_path.stem + ".cleaned.csv")
    )
    rep_path = Path(args.report) if args.report else in_path.with_name(in_path.stem + ".cleaned.changes.csv")

    df = pd.read_csv(in_path, dtype=str).fillna("")
    if "name" not in df.columns:
        print("[error] CSV не содержит столбца 'name'", file=sys.stderr); sys.exit(2)

    before = df["name"].tolist()
    df["name"] = df["name"].map(rb_fix)
    after  = df["name"].tolist()

    changed = [{"row_index": i, "old_name": o, "new_name": n} for i,(o,n) in enumerate(zip(before, after)) if o != n]

    df.to_csv(out_path, index=False)
    pd.DataFrame(changed).to_csv(rep_path, index=False)
    print(f"[ok] cleaned: {out_path}")
    print(f"[ok] changes: {rep_path} ({len(changed)} rows changed)")

if __name__ == "__main__":
    main()
