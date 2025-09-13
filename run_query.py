import os
import sys
import time
from pathlib import Path

import psycopg2
import pandas as pd
from psycopg2 import OperationalError, InterfaceError

# Подключения
DB1_URL = os.getenv("DB1_URL")  # БД ресурсов/пользователей/транзакций (query_1.sql)
DB2_URL = os.getenv("DB2_URL")  # БД событий (query_2.sql)

SQL1_FILE = Path("query_1.sql")
SQL2_FILE = Path("query_2.sql")

OUT_GOLD  = Path("user_gold.csv")   # теперь из query_2 + username
OUT_TOTAL = Path("user_total.csv")  # итог с редкостями и score

def fetch_df(conn_url: str, sql: str, params=None, retries: int = 3, delay: int = 3) -> pd.DataFrame:
    """Выполнить SQL и вернуть DataFrame с ретраями и TCP keepalive."""
    if not conn_url:
        raise RuntimeError("Не задан URL подключения к БД.")
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(
                conn_url,
                keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=5,
            )
            try:
                return pd.read_sql(sql, conn, params=params)
            finally:
                conn.close()
        except (OperationalError, InterfaceError) as e:
            last_err = e
            print(f"[fetch_df] attempt {attempt}/{retries} failed: {e}", file=sys.stderr)
            time.sleep(delay)
    raise last_err

def norm_lower(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={c: c.lower().strip() for c in df.columns})

def ensure_userid(df: pd.DataFrame) -> pd.DataFrame:
    if "userid" in df.columns:
        return df
    for c in list(df.columns):
        if c.strip('"').lower() in ("userid", "user id", "user_id"):
            return df.rename(columns={c: "userid"})
    raise RuntimeError(f"Не найдена колонка userId. Есть: {df.columns.tolist()}")

def main():
    # --- ШАГ 1: query_2.sql (БД2) — редкости + золото из событий ---
    if not SQL2_FILE.exists():
        raise FileNotFoundError("Нет файла query_2.sql рядом со скриптом.")
    sql2 = SQL2_FILE.read_text(encoding="utf-8")
    df2 = fetch_df(DB2_URL, sql2)  # без params

    if df2.empty:
        # Пустые CSV, если никто не открывал гачу
        pd.DataFrame(columns=["username","gold","userid"]).to_csv(OUT_GOLD, index=False, encoding="utf-8")
        pd.DataFrame(columns=["rank","username","score","gold","rares","epics","legendaries","userid"]).to_csv(
            OUT_TOTAL, index=False, encoding="utf-8"
        )
        print(f"[query_2] Пусто. Созданы {OUT_GOLD} и {OUT_TOTAL}.")
        return

    df2 = norm_lower(df2)
    df2 = ensure_userid(df2)
    for k in ("rares","epics","legendaries","gold"):
        if k not in df2.columns: df2[k] = 0
    # типы
    df2[["rares","epics","legendaries"]] = df2[["rares","epics","legendaries"]].fillna(0).astype(int)
    df2["gold"] = pd.to_numeric(df2["gold"], errors="coerce").fillna(0)

    user_ids = [str(u) for u in pd.unique(df2["userid"]).tolist()]
    if not user_ids:
        pd.DataFrame(columns=["username","gold","userid"]).to_csv(OUT_GOLD, index=False, encoding="utf-8")
        pd.DataFrame(columns=["rank","username","score","gold","rares","epics","legendaries","userid"]).to_csv(
            OUT_TOTAL, index=False, encoding="utf-8"
        )
        print("[query_2] Нет userId.")
        return

    # --- ШАГ 2: query_1.sql (БД1) — отфильтровать допустимых + получить username ---
    if not SQL1_FILE.exists():
        raise FileNotFoundError("Нет файла query_1.sql рядом со скриптом.")
    sql1 = SQL1_FILE.read_text(encoding="utf-8").strip()
    params = (user_ids,)  # ANY(%s)
    df1 = fetch_df(DB1_URL, sql1, params=params, retries=3, delay=3)

    if df1.empty:
        # никто не прошёл фильтры
        pd.DataFrame(columns=["username","gold","userid"]).to_csv(OUT_GOLD, index=False, encoding="utf-8")
        pd.DataFrame(columns=["rank","username","score","gold","rares","epics","legendaries","userid"]).to_csv(
            OUT_TOTAL, index=False, encoding="utf-8"
        )
        print("[query_1] Никто не прошёл фильтры.")
        return

    df1 = norm_lower(df1)
    if "userid" not in df1.columns:
        for c in list(df1.columns):
            if c.strip('"').lower() in ("userid","user id","user_id"):
                df1 = df1.rename(columns={c:"userid"})
                break
    if "username" not in df1.columns:
        raise RuntimeError("[query_1] Нет колонки 'username'.")

    # Приведём ключ к строке
    df1["userid"] = df1["userid"].astype("string")
    df2["userid"] = df2["userid"].astype("string")

    # --- МЕРДЖ: username + (gold/rares/epics/legendaries) ---
    total = df1.merge(df2[["userid","gold","rares","epics","legendaries"]], on="userid", how="left")
    total[["gold","rares","epics","legendaries"]] = total[["gold","rares","epics","legendaries"]].fillna(0)
    total["gold"] = pd.to_numeric(total["gold"], errors="coerce").fillna(0)

    # user_gold.csv (для совместимости/проверки)
    total[["username","gold","userid"]].to_csv(OUT_GOLD, index=False, encoding="utf-8")
    print(f"Сохранено {len(total)} строк в {OUT_GOLD}")

    # --- score и rank ---
    bonus_pct = 0.001*pd.to_numeric(total["rares"]) + 0.006*pd.to_numeric(total["epics"]) + 0.03*pd.to_numeric(total["legendaries"])
    total["score"] = total["gold"] * (1.0 + bonus_pct)

    total = total.sort_values(["score","gold"], ascending=[False,False], kind="mergesort").reset_index(drop=True)
    total.insert(0, "rank", total.index + 1)

    total = total[["rank","username","score","gold","rares","epics","legendaries","userid"]]
    total.to_csv(OUT_TOTAL, index=False, encoding="utf-8")
    print(f"Сохранено {len(total)} строк в {OUT_TOTAL}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
