# normalize_catalog_oneclick.py
import os, json, time, re, math, random
import pandas as pd
from openai import OpenAI
import httpx
from dotenv import load_dotenv


# --- imports unchanged ---

# >>> move this to BEFORE reading env <<<
load_dotenv()

# ---- Config with safe defaults ----
INPUT_CSV  = "data_1_0.csv"
OUTPUT_CSV = "data_1_1.csv"
REPORT_CSV = "data_1_0_changes.csv"

MODEL      = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
BASE_URL   = os.getenv("OPENAI_BASE_URL")
RETRIES    = int(os.getenv("RETRIES", "5"))
PAUSE_BASE = float(os.getenv("PAUSE", "0.4"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "15"))
READ_TIMEOUT_SEC = float(os.getenv("READ_TIMEOUT_SEC", "45"))


# ---- OpenAI client with strict timeouts ----
HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=READ_TIMEOUT_SEC, write=30.0, pool=None)
HTTP_LIMITS  = httpx.Limits(max_keepalive_connections=5, max_connections=10)
client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY"),
    base_url=BASE_URL if BASE_URL else None,
    http_client=httpx.Client(timeout=HTTP_TIMEOUT, limits=HTTP_LIMITS)
)

SYSTEM_PROMPT = """
Ты — строгий редактор товарного каталога. На входе массив объектов:
- id (строка) — эхо-идентификатор (НЕ МЕНЯТЬ);
- barcode (строка) — НЕ МЕНЯТЬ;
- name (строка) — нормализовать ТОЛЬКО безопасно.

ГЛАВНЫЙ ПРИНЦИП
— Если есть малейшее сомнение — МИНИМУМ изменений; confidence≠high; productDesc=null.

Что можно править в name (безопасно)
1) Орфография и очевидные опечатки (в т.ч. брендов) без изменения смысла: «Чуда»→«Чудо», «Hohland»→«Hochland» и т.п.
2) Пунктуация/пробелы: убрать мусор (двойные пробелы, лишние кавычки/скобки, «- -»), унифицировать дефис «-».
3) Единицы: г/кг/мл/л; проценты «3,2%» (русская запятая). Если после «%» идёт число/единица — поставить пробел: «3,2% 500 г».
   — Нормализация очевидных форм вроде «0,110гр» → «110 г» допустима.
4) Служебные пометки упаковки можно убирать, если не теряется смысл: «пэт/жб/ж/б/п/б/шт/уп./д.п./с/б/ж/б/ст/б/PP/PET/Tetra/финпак/пюрпак».
5) Укорачивать можно ТОЛЬКО без потери важной инфы (тип/вкус/жирность/вес/объём/вид/серия).

ЖЁСТКИЕ ЗАПРЕТЫ
— Не менять тип товара (йогурт↔сок↔энергетик↔чай и т.п.).
— Не переводить/не улучшать бренды/серии (Lipton Ice, Monster, 7UP остаются как есть).
— Не исправлять объём/вес/проценты «на глаз» (напр. 0,07 г, 0,33 мл). Оставь как есть → confidence=medium/low; productDesc=null.
— Не придумывать вкус/серию/модель.
— Не объединять/разъединять коды. Если в name есть «x12», «12×450 мл» и т.п. — СОХРАНИ.
— Не менять намеренно регистр известных ТМ (7UP, NESTLÉ и т.п.), максимум — исправить явную опечатку.

BRAND
— brand возвращай ТОЛЬКО если явное устойчивое имя марки/ТМ присутствует в name (Чудо, Президент/Président, Danone, NEMOLOKO и т.п.).
— Исправляй ТОЛЬКО явные опечатки бренда (Haggies→Huggies, Hohland→Hochland). При сомнении — brand=null.
— Никогда не поднимай описательные слова в бренд (бан-лист, неполный): Classic, Original, Premium, Домашний/Домашняя, Крестьянский, Крепыш,
  Протеин/Протейн/Протейн+, FOOD, Лакомка (если не уверены, что это ТМ), Белые сыры, Рассыпчатый/Рассыпчатая и т.п. → brand=null.
— Если changes.brandFix=true, поле brand ОБЯЗАТЕЛЬНО должно быть НЕ null и содержать нормализованное имя марки, явно присутствующее в name (напр.: «Чудо», «Président/Президент», «Hochland», «Danone» и т.п.).

productDesc (1–2 очень короткие фразы)
— Пиши ТОЛЬКО при confidence=high.
— ТОЛЬКО факты из name: тип/вкус/жирность/масса/объём/форма (питьевой/плавленый/творожный и т.п.).
— Запрещены маркетинговые формулировки: «свежайшее/освежающий/полезный/идеально/отлично подходит/уникальный/насыщенный/классический».
— Примеры: «Питьевой йогурт с клубникой. Жирность 2%, 450 г.» / «Плавленый сыр Моцарелла, 150 г, 45%.»

АУДИТ
— changed: true/false — были ли правки в name.
— changes: ставь true ТОЛЬКО если была реальная правка в категории (spelling/units/punctuation/brandFix/other).
— confidence: high|medium|low. Любая неоднозначность → НЕ high.

ТРИГГЕРЫ ДЛЯ confidence≠high (и productDesc=null)
— Подозрительные единицы: <1 г или <5 мл для потребтоваров; «0,33 мл»; дробные сотые грамма/мл.
— В name несколько БАРКОДОВ или несколько разных объёмов/весов без явного смысла.
— Технические хвосты/артефакты: «Натиже», «ванна», «ассортименте» и пр., если их трактовка неочевидна.
— Непрозрачные аббревиатуры (МДЖ/мдж, ФМ и др.), если они не критичны для корректной нормализации.

ПРИМЕРЫ ОШИБОК (ориентиры)
— «Воздушная пщеница» → «Воздушная пшеница» (орфография).
— «Какос стружка» → «Кокос стружка» (орфография).
— «ПодУЗники Haggies» → «Подгузники Huggies» (опечатка бренда).
— «Удленитель ЕТО» → «Удлинитель ETO» (опечатка слова и бренда).
— «Lipton Lce Tea Черный» → «Lipton Ice Tea Черный» (опечатка бренда).
— «Анчоус тушка потрошеная» → «Анчоус тушка потрошёная» (правописание).
— «Соус Чили с чесночный Чим Чим» → «Соус Чили с чесноком Чим Чим» (грамматика).
— «Lavina Can 0.33 л Turbo» → оставить без изменения типа («напиток»), т.к. сомнительно; confidence=medium; productDesc=null.
— «Молоко Иртыш Кресьянское» → «Молоко Иртыш Крестьянское» (орфография).
— «Махеев оливковое масло высококалорийное 67%» → не менять тип, при сомнении ставить confidence=medium; productDesc=null.
— «Горчица с хреном 0,07 г» → оставить как есть (сомнительная единица), confidence=medium, productDesc=null.
— «Лимонад Натахтари Тархун 0,33 мл» → оставить как есть, confidence=medium, productDesc=null.
— «Funny Kithen Foodle» → «Funny Kitchen Foodle» (опечатка).

ФОРМАТ ВОЗВРАТА — МАССИВ строгих JSON-объектов:
{
  "id": string,
  "barcode": string,
  "name": string,
  "brand": string|null,
  "productDesc": string|null,
  "changed": boolean,
  "changes": {
    "spelling": boolean,
    "units": boolean,
    "punctuation": boolean,
    "brandFix": boolean,
    "other": boolean
  },
  "confidence": "high"|"medium"|"low"
}
"""


# safer unit token extraction
UNIT_TOKEN_RE = re.compile(r'(\d+(?:[.,]\d+)?\s?(?:г|кг|мл|л|%))', re.IGNORECASE)
def unit_tokens(s: str):
    return set(t.strip().lower() for t in UNIT_TOKEN_RE.findall(s or ""))

def looks_suspicious(orig_name: str, new_name: str) -> bool:
    o, n = unit_tokens(orig_name), unit_tokens(new_name)
    if o and not o.issubset(n):  # потеря чисел/единиц
        return True
    if len(orig_name) >= 12 and len(new_name or "") < max(8, int(len(orig_name) * 0.6)):
        return True
    return False

def brand_from_model_if_in_name(model_brand, original_name):
    if isinstance(model_brand, str) and model_brand.strip():
        if model_brand.lower() in (original_name or "").lower():
            return model_brand.strip()
    return None

def clamp_desc(desc, max_len=300):
    if isinstance(desc, str):
        return " ".join(desc.split())[:max_len]
    return None

def call_model_batch(items):
    # items: list of dicts {id, barcode, name}
    messages = [
        {"role":"system","content": SYSTEM_PROMPT},
        {"role":"user",  "content": json.dumps(items, ensure_ascii=False)}
    ]
    resp = client.chat.completions.create(
        model=MODEL,
        temperature=0,
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "NormalizedRowsEnvelope",
                "schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "rows": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "id": {"type":"string"},
                                    "barcode": {"type":"string"},
                                    "name": {"type":"string"},
                                    "brand": {"type":["string","null"]},
                                    "productDesc": {"type":["string","null"]},
                                    "changed": {"type":"boolean"},
                                    "changes": {
                                        "type":"object",
                                        "additionalProperties": False,
                                        "properties": {
                                            "spelling":{"type":"boolean"},
                                            "units":{"type":"boolean"},
                                            "punctuation":{"type":"boolean"},
                                            "brandFix":{"type":"boolean"},
                                            "other":{"type":"boolean"}
                                        },
                                        "required":["spelling","units","punctuation","brandFix","other"]
                                    },
                                    "confidence": {"enum":["high","medium","low"]}
                                },
                                "required": ["id","barcode","name","brand","productDesc","changed","changes","confidence"]
                            }
                        }
                    },
                    "required": ["rows"]
                }
            }
        },
        messages=messages,
        timeout=READ_TIMEOUT_SEC
    )
    data = json.loads(resp.choices[0].message.content)
    return data.get("rows", [])

def iter_batches(indices, size):
    for i in range(0, len(indices), size):
        yield indices[i:i+size]

def main():
    # Read CSV as strings to avoid NaN surprises
    df = pd.read_csv(INPUT_CSV, dtype=str).fillna("")
    if "brand" not in df.columns: df["brand"] = ""
    if "productDesc" not in df.columns: df["productDesc"] = ""

    report_rows = []
    idx_all = df.index.tolist()

    try:
        for bi, batch_idx in enumerate(iter_batches(idx_all, BATCH_SIZE), start=1):
            t0 = time.time()
            # Build batch payload
            batch_payload = []
            for i in batch_idx:
                row = df.loc[i]
                # skip rows without mandatory fields
                if "barcode" not in row or "name" not in row:
                    continue
                batch_payload.append({
                    "id": str(i),
                    "barcode": str(row["barcode"]),
                    "name": str(row["name"])
                })

            if not batch_payload:
                continue

            # Call with retries
            last_err = None
            for attempt in range(RETRIES + 1):
                try:
                    results = call_model_batch(batch_payload)
                    last_err = None
                    break
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    last_err = e
                    if attempt < RETRIES:
                        sleep_s = PAUSE_BASE * (attempt + 1) + random.uniform(0, 0.3)
                        print(f"[warn] batch {bi} failed (attempt {attempt+1}): {e} — retry in {sleep_s:.1f}s")
                        time.sleep(sleep_s)

            if last_err:
                # On failure keep originals, log to report
                for item in batch_payload:
                    i = int(item["id"])
                    report_rows.append({
                        "barcode": item["barcode"],
                        "old_name": item["name"],
                        "new_name": item["name"],
                        "brand": df.at[i, "brand"],
                        "productDesc": df.at[i, "productDesc"],
                        "confidence": "low",
                        "changes": json.dumps({"other": True}, ensure_ascii=False),
                        "error": str(last_err)
                    })
                continue

            # Merge results
            by_id = {r["id"]: r for r in results if isinstance(r, dict) and "id" in r}
            for item in batch_payload:
                i = int(item["id"])
                orig_name = item["name"]
                res = by_id.get(str(i))
                if not res:
                    # no change
                    report_rows.append({
                        "barcode": item["barcode"],
                        "old_name": orig_name,
                        "new_name": orig_name,
                        "brand": df.at[i, "brand"],
                        "productDesc": df.at[i, "productDesc"],
                        "confidence": "low",
                        "changes": json.dumps({"other": True}, ensure_ascii=False),
                    })
                    continue

                # 1) barcode must match
                if str(res.get("barcode","")) != item["barcode"]:
                    # ignore model output for safety
                    new_name = orig_name
                    new_brand = df.at[i, "brand"]
                    new_desc = df.at[i, "productDesc"]
                    conf = "low"
                    chg = {"other": True}
                else:
                    # 2) safe name choose with confidence & unit-loss guard
                    proposed = res.get("name", orig_name)
                    conf = res.get("confidence", "low")
                    if conf != "high" or looks_suspicious(orig_name, proposed):
                        new_name = orig_name
                    else:
                        new_name = proposed

                    # 3) brand only if literally present in ORIGINAL name
                    new_brand = brand_from_model_if_in_name(res.get("brand"), orig_name) or ""

                    # 4) productDesc only when high confidence
                    new_desc = clamp_desc(res.get("productDesc")) if conf == "high" else ""

                    chg = res.get("changes", {})

                # Apply
                df.at[i, "name"] = new_name
                df.at[i, "brand"] = new_brand
                df.at[i, "productDesc"] = new_desc

                # Report
                report_rows.append({
                    "barcode": item["barcode"],
                    "old_name": orig_name,
                    "new_name": new_name,
                    "brand": new_brand,
                    "productDesc": new_desc,
                    "confidence": conf,
                    "changes": json.dumps(chg, ensure_ascii=False)
                })

            dt = time.time() - t0
            print(f"[ok] batch {bi}: {len(batch_payload)} rows in {dt:.1f}s")

    except KeyboardInterrupt:
        # partial save
        print("\n[info] interrupted — writing partial files...")
        df.to_csv(OUTPUT_CSV.replace(".csv", "_partial.csv"), index=False)
        pd.DataFrame(report_rows).to_csv(REPORT_CSV.replace(".csv", "_partial.csv"), index=False)
        print(f"[info] Partial: {OUTPUT_CSV.replace('.csv','_partial.csv')}")
        print(f"[info] Partial: {REPORT_CSV.replace('.csv','_partial.csv')}")
        return

    # Final save
    df.to_csv(OUTPUT_CSV, index=False)
    pd.DataFrame(report_rows).to_csv(REPORT_CSV, index=False)
    print(f"Updated: {OUTPUT_CSV}\nReport:  {REPORT_CSV}")

if __name__ == "__main__":
    main()
