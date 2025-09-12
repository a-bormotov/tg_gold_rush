import os
import sys
import time
from pathlib import Path

import psycopg2
import pandas as pd
from psycopg2 import OperationalError, InterfaceError


# Подключения: выставляются в workflow / окружении
DB1_URL = os.getenv("DB1_URL")  # БД ресурсов/пользователей/транзакций (query_1.sql)
DB2_URL = os.getenv("DB2_URL")  # БД событий (query_2.sql)

SQL1_FILE = Path("query_1.sql")
SQL2_FILE = Path("query_2.sql")

OUT_GOLD  = Path("user_gold.csv")   # ДО вычитания
OUT_TOTAL = Path("user_total.csv")  # ПОСЛЕ вычитания + score + rank
SNAPSHOT_FILE = Path("user_snapshot.csv")  # формат: userId,gold (сколько вычесть)


def fetch_df(conn_url: str, sql: str, params=None, retries: int = 3, delay: int = 3) -> pd.DataFrame:
    """Выполнить SQL и вернуть DataFrame с ретраями и TCP keepalive."""
    if not conn_url:
        raise RuntimeError("Не задан URL подключения к БД.")

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                conn_url,
                # держим TCP живым, чтобы туннель не "засыпал"
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
            )
            try:
                return pd.read_sql(sql, conn, params=params)
            finally:
                conn.close()
        except (OperationalError, InterfaceError) as e:
            last_err = e
            print(f"[fetch_df] attempt {attempt}/{retries} failed: {e}", file=sys.stderr)
            time.sleep(delay)
    # если все попытки провалились — пробрасываем последнюю ошибку
    raise last_err


def norm_lower(df: pd.DataFrame) -> pd.DataFrame:
    """Привести имена колонок к нижнему регистру (без лишних пробелов)."""
    return df.rename(columns={c: c.lower().strip() for c in df.columns})


def ensure_userid(df: pd.DataFrame) -> pd.DataFrame:
    """
    Гарантировать наличие колонки 'userid' (lowercase).
    Переименуем возможные варианты: "userId", "User ID", 'user id', 'user_id'.
    """
    if "userid" in df.columns:
        return df
    for c in list(df.columns):
        base = c.strip('"').lower()
        if base in ("userid", "user id", "user_id"):
            return df.rename(columns={c: "userid"})
    raise RuntimeError(
        f"Не найдена колонка userId в результате запроса. Имеющиеся колонки: {df.columns.tolist()}"
    )


def load_snapshot(filepath: Path) -> pd.DataFrame:
    """
    Загрузить снимок списаний из CSV (userId,gold).
    Возвращает DataFrame с колонками: userid, subtract_gold (float).
    Если файла нет — вернёт пустой DF.
    """
    if not filepath.exists():
        print(f"[snapshot] Файл {filepath} не найден — вычитание не будет применено.")
        return pd.DataFrame(columns=["userid", "subtract_gold"])

    try:
        df = pd.read_csv(
            filepath,
            dtype={"userId": "string"},
            usecols=["userId", "gold"]
        )
    except ValueError:
        df = pd.read_csv(filepath, dtype="string")
        df = norm_lower(df)
        uid_col = None
        gold_col = None
        for c in df.columns:
            b = c.strip('"').lower()
            if b in ("userid", "user id", "user_id"):
                uid_col = c
            if b == "gold":
                gold_col = c
        if uid_col is None or gold_col is None:
            print(f"[snapshot] Не удалось распознать колонки userId/gold в {filepath}. Снимок не будет применён.")
            return pd.DataFrame(columns=["userid", "subtract_gold"])
        df = df[[uid_col, gold_col]].rename(columns={uid_col: "userId", gold_col: "gold"})

    df = norm_lower(df)
    df = ensure_userid(df)
    df["subtract_gold"] = pd.to_numeric(df["gold"], errors="coerce").fillna(0)
    return df[["userid", "subtract_gold"]]


def main():
    # --- ШАГ 1: query_2.sql (БД2) — все, кто открывал гачу в указанном интервале ---
    if not SQL2_FILE.exists():
        raise FileNotFoundError("Нет файла query_2.sql рядом со скриптом.")

    sql2 = SQL2_FILE.read_text(encoding="utf-8")
    # ВНИМАНИЕ: query_2.sql должен содержать фиксированные timestamps и НЕ содержать = ANY(%s)
    df2 = fetch_df(DB2_URL, sql2)  # без params

    if df2.empty:
        # Никто не открывал гачу: выгружаем пустые CSV с заголовками
        pd.DataFrame(columns=["username", "gold", "userid"]).to_csv(OUT_GOLD, index=False, encoding="utf-8")
        pd.DataFrame(columns=["rank", "username", "score", "gold", "rares", "epics", "legendaries", "userid"]).to_csv(
            OUT_TOTAL, index=False, encoding="utf-8"
        )
        print(f"[query_2] Пустой результат. Созданы пустые {OUT_GOLD} и {OUT_TOTAL}.")
        return

    df2 = norm_lower(df2)
    df2 = ensure_userid(df2)
    for k in ("rares", "epics", "legendaries"):
        if k not in df2.columns:
            df2[k] = 0

    user_ids = pd.unique(df2["userid"]).tolist()
    if not user_ids:
        pd.DataFrame(columns=["username", "gold", "userid"]).to_csv(OUT_GOLD, index=False, encoding="utf-8")
        pd.DataFrame(columns=["rank", "username", "score", "gold", "rares", "epics", "legendaries", "userid"]).to_csv(
            OUT_TOTAL, index=False, encoding="utf-8"
        )
        print("[query_2] Нет userId. Созданы пустые CSV.")
        return

    # --- ШАГ 2: query_1.sql (БД1) — из этих userId оставить только с транзакциями и взять gold/username ---
    if not SQL1_FILE.exists():
        raise FileNotFoundError("Нет файла query_1.sql рядом со скриптом.")

    sql1 = SQL1_FILE.read_text(encoding="utf-8").strip()

    try:
        df1 = fetch_df(DB1_URL, sql1, params=(user_ids,))
    except Exception as e:
        print(
            f"[query_1] Не удалось выполнить с фильтром ANY(%s): {e}\n"
            f"Попробую вытянуть всё и отфильтровать в памяти (медленнее).",
            file=sys.stderr,
        )
        
        # ⚠️ ВАЖНО: убираем параметризированное условие из SQL перед повторным запуском
        # Превратим `AND ur."userId" = ANY(%s)` в `AND TRUE` (или просто удалим)
        sql1_no_param = re.sub(
            r'AND\s+ur\."userId"\s*=\s*ANY\(\s*%s\s*\)',
            'AND TRUE',
            sql1,
            flags=re.IGNORECASE
        )

        # Теперь тянем всё и фильтруем в памяти
        df1_all = fetch_df(DB1_URL, sql1)
        df1_all = norm_lower(df1_all)
        df1_all = ensure_userid(df1_all)
        df1 = df1_all[df1_all["userid"].isin(user_ids)].copy()

    if df1.empty:
        pd.DataFrame(columns=["username", "gold", "userid"]).to_csv(OUT_GOLD, index=False, encoding="utf-8")
        pd.DataFrame(columns=["rank", "username", "score", "gold", "rares", "epics", "legendaries", "userid"]).to_csv(
            OUT_TOTAL, index=False, encoding="utf-8"
        )
        print("[query_1] После фильтра по транзакциям пользователей нет. Созданы пустые CSV.")
        return

    # Нормализуем и проверим нужные колонки
    df1 = norm_lower(df1)
    if "userid" not in df1.columns:
        for c in list(df1.columns):
            if c.strip('"').lower() in ("userid", "user id", "user_id"):
                df1 = df1.rename(columns={c: "userid"})
                break
    for need in ("username", "gold", "userid"):
        if need not in df1.columns:
            raise RuntimeError(
                f"[query_1] Отсутствует колонка '{need}'. Есть: {df1.columns.tolist()}"
            )

    # --- ШАГ 3: сохранить user_gold.csv (ДО вычитания) ---
    df1[["username", "gold", "userid"]].to_csv(OUT_GOLD, index=False, encoding="utf-8")
    print(f"Сохранено {len(df1)} строк в {OUT_GOLD}")

    # --- ШАГ 4: применить снимок (user_snapshot.csv) — вычесть gold по каждому пользователю ---
    snap = load_snapshot(SNAPSHOT_FILE)  # userid, subtract_gold
    # gold обязательно в число
    df1["gold"] = pd.to_numeric(df1["gold"], errors="coerce").fillna(0)

    if not snap.empty:
        df1 = df1.merge(snap, on="userid", how="left")
        df1["subtract_gold"] = pd.to_numeric(df1.get("subtract_gold"), errors="coerce").fillna(0)
        df1["gold"] = df1["gold"] - df1["subtract_gold"]
        df1 = df1.drop(columns=["subtract_gold"])
    else:
        print("[snapshot] Пустой/отсутствующий снимок — вычитание пропущено.")

    # --- ШАГ 5: объединить с редкостями ---
    df2_small = df2[["userid", "rares", "epics", "legendaries"]].copy()
    total = df1.merge(df2_small, on="userid", how="left")
    total[["rares", "epics", "legendaries"]] = total[["rares", "epics", "legendaries"]].fillna(0).astype(int)

    # --- ШАГ 6: расчёт score и rank ---
    # score = gold * (1 + 0.001*rares + 0.006*epics + 0.03*legendaries)
    total["gold"] = pd.to_numeric(total["gold"], errors="coerce").fillna(0)
    bonus_pct = (
        0.001 * total["rares"] +
        0.006 * total["epics"] +
        0.03  * total["legendaries"]
    )
    total["score"] = total["gold"] * (1.0 + bonus_pct)

    # сортировка по score (DESC) и порядковый rank с 1
    total = total.sort_values(["score", "gold"], ascending=[False, False], kind="mergesort").reset_index(drop=True)
    total.insert(0, "rank", total.index + 1)

    # --- порядок колонок и сохранение ---
    total = total[["rank", "username", "score", "gold", "rares", "epics", "legendaries", "userid"]]
    total.to_csv(OUT_TOTAL, index=False, encoding="utf-8")
    print(f"Сохранено {len(total)} строк в {OUT_TOTAL}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
