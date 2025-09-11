import os
import sys
from pathlib import Path
import psycopg2
import pandas as pd

DB1_URL = os.getenv("DB1_URL")  # БД с золотом/именами
DB2_URL = os.getenv("DB2_URL")  # БД с событиями редкостей

SQL1_FILE = Path("query_1.sql")
SQL2_FILE = Path("query_2.sql")

OUT_GOLD = Path("user_gold.csv")
OUT_TOTAL = Path("user_total.csv")


def fetch_df(conn_url: str, sql: str, params=None) -> pd.DataFrame:
    if not conn_url:
        raise RuntimeError("Не задан URL подключения к БД.")
    with psycopg2.connect(conn_url) as conn:
        return pd.read_sql(sql, conn, params=params)


def norm_lower(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={c: c.lower().strip() for c in df.columns})


def main():
    # --- 1) Второй запрос: редкости, БЕЗ фильтра по игрокам (БД2) ---
    if not SQL2_FILE.exists():
        raise FileNotFoundError("Нет файла query_2.sql")
    sql2 = SQL2_FILE.read_text(encoding="utf-8")

    df2 = fetch_df(DB2_URL, sql2)  # без params
    if df2.empty:
        # Если редкостей нет вообще — выгружаем пустые файлы с заголовками
        pd.DataFrame(columns=["username", "gold", "userid"]).to_csv(OUT_GOLD, index=False, encoding="utf-8")
        pd.DataFrame(columns=["username", "gold", "rares", "epics", "legendaries", "userid"]).to_csv(
            OUT_TOTAL, index=False, encoding="utf-8"
        )
        print(f"query_2 вернул пусто — созданы пустые {OUT_GOLD} и {OUT_TOTAL}")
        return

    # нормализуем столбцы редкостей
    df2 = norm_lower(df2)
    # привести имя ID, если пришло как userId/"User ID"
    if "userid" not in df2.columns:
        for c in list(df2.columns):
            if c.strip('"').lower() in ("userid", "user id"):
                df2 = df2.rename(columns={c: "userid"})
                break
    for k in ("rares", "epics", "legendaries"):
        if k not in df2.columns:
            df2[k] = 0

    user_ids = pd.unique(df2["userid"]).tolist()
    if not user_ids:
        pd.DataFrame(columns=["username", "gold", "userid"]).to_csv(OUT_GOLD, index=False, encoding="utf-8")
        # total = только нули по редкостям и пустой gold
        pd.DataFrame(columns=["username", "gold", "rares", "epics", "legendaries", "userid"]).to_csv(
            OUT_TOTAL, index=False, encoding="utf-8"
        )
        print(f"В query_2 нет пользовательских ID — созданы пустые {OUT_GOLD} и {OUT_TOTAL}")
        return

    # --- 2) Первый запрос: gold/username ТОЛЬКО для этих userId (БД1) ---
    if not SQL1_FILE.exists():
        raise FileNotFoundError("Нет файла query_1.sql")
    sql1_raw = SQL1_FILE.read_text(encoding="utf-8").strip()

    # Оборачиваем query_1.sql подзапросом и фильтруем по "userId" на стороне БД1
    # ВАЖНО: внутри query_1.sql должна быть колонка "userId"
    sql1_filtered = f"""
    WITH src AS (
    {sql1_raw}
    )
    SELECT * FROM src
    WHERE "userId" = ANY(%s);
    """

    try:
        df1 = fetch_df(DB1_URL, sql1_filtered, params=(user_ids,))
    except Exception as e:
        # fallback: если внутри query_1.sql колонка названа иначе — тянем всё и фильтруем в памяти
        print(f"Предупреждение: не удалось отфильтровать по \"userId\" на стороне БД ({e}). "
              f"Попробую отфильтровать в памяти.", file=sys.stderr)
        df1_all = fetch_df(DB1_URL, sql1_raw)
        # пытаемся найти колонку с userId
        candidates = [c for c in df1_all.columns if c.strip('"').lower() in ("userid", "user id", "user_id", "user id")]
        if candidates:
            id_col = candidates[0]
            df1 = df1_all[df1_all[id_col].isin(user_ids)].copy()
            df1 = df1.rename(columns={id_col: "userId"})
        else:
            raise RuntimeError("Не удалось найти колонку userId в результате query_1.sql")

    if df1.empty:
        # если в БД1 по этим юзерам ничего — gold=0, username пустой
        out = pd.DataFrame({
            "username": [],
            "gold": [],
            "userid": []
        })
    else:
        # нормализуем имена
        df1_norm = norm_lower(df1)
        # привести имена столбцов к username, gold, userid
        rename_map = {}
        for c in list(df1_norm.columns):
            base = c.strip('"').lower()
            if base == "user id":
                rename_map[c] = "userid"
        df1_norm = df1_norm.rename(columns=rename_map)
        for need in ("username", "gold", "userid"):
            if need not in df1_norm.columns:
                raise RuntimeError(f"В query_1 результате отсутствует колонка '{need}'. Нашли: {df1_norm.columns.tolist()}")
        out_gold = df1_norm[["username", "gold", "userid"]].copy()
        # сохраним user_gold.csv
        out_gold.to_csv(OUT_GOLD, index=False, encoding="utf-8")
        print(f"Сохранено {len(out_gold)} строк в {OUT_GOLD}")
        out = out_gold

    # --- 3) Мёрдж с редкостями (df2) и сохранение user_total.csv ---
    df2_small = df2[["userid", "rares", "epics", "legendaries"]].copy()
    total = out.merge(df2_small, on="userid", how="left")
    total[["rares", "epics", "legendaries"]] = total[["rares", "epics", "legendaries"]].fillna(0).astype(int)
    total = total[["username", "gold", "rares", "epics", "legendaries", "userid"]]
    total.to_csv(OUT_TOTAL, index=False, encoding="utf-8")
    print(f"Сохранено {len(total)} строк в {OUT_TOTAL}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
