import os
import sys
from pathlib import Path
import psycopg2
import pandas as pd

DB1_URL = os.getenv("DB1_URL")  # первая БД (gold/username/userId)
DB2_URL = os.getenv("DB2_URL")  # вторая БД (events/rarity)
SQL1_FILE = Path("query_1.sql")
SQL2_FILE = Path("query_2.sql")
OUT1 = Path("user_gold.csv")
OUT2 = Path("user_total.csv")


def fetch_df(conn_url: str, sql: str, params=None) -> pd.DataFrame:
    if not conn_url:
        raise RuntimeError("Не задан URL подключения к БД.")
    with psycopg2.connect(conn_url) as conn:
        return pd.read_sql(sql, conn, params=params)


def norm_lower(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={c: c.lower().strip() for c in df.columns})


def main():
    # --- 1) Первый запрос: gold/username/userId (БД1) ---
    if not SQL1_FILE.exists():
        raise FileNotFoundError("Нет файла query_1.sql")
    sql1 = SQL1_FILE.read_text(encoding="utf-8")

    df1 = fetch_df(DB1_URL, sql1)
    if df1.empty:
        # пустая выгрузка: сохранить оба CSV c заголовками
        pd.DataFrame(columns=["username", "gold", "userid"]).to_csv(OUT1, index=False, encoding="utf-8")
        pd.DataFrame(columns=["username", "gold", "rares", "epics", "legendaries", "userid"]).to_csv(
            OUT2, index=False, encoding="utf-8"
        )
        print(f"Пустой результат первого запроса. Созданы пустые {OUT1} и {OUT2}.")
        return

    # нормализуем имена столбцов
    df1 = norm_lower(df1)
    # привести варианты названий к нужным
    rename_candidates = {}
    for col in list(df1.columns):
        base = col.strip('"').lower()
        if base == 'user id' or base == '"user id"':
            rename_candidates[col] = 'userid'
    if rename_candidates:
        df1 = df1.rename(columns=rename_candidates)

    # убедимся, что нужные колонки есть
    required1 = ["username", "gold", "userid"]
    for col in required1:
        if col not in df1.columns:
            raise RuntimeError(f"В результате query_1.sql отсутствует колонка '{col}'. Найдено: {df1.columns.tolist()}")

    # сохранить user_gold.csv
    df1.to_csv(OUT1, index=False, encoding="utf-8")
    print(f"Сохранено {len(df1)} строк в {OUT1}")

    user_ids = df1["userid"].tolist()
    if not user_ids:
        # пользователей нет — итог пустой
        pd.DataFrame(columns=["username", "gold", "rares", "epics", "legendaries", "userid"]).to_csv(
            OUT2, index=False, encoding="utf-8"
        )
        print(f"Список пользователей пуст. Создан пустой {OUT2}")
        return

    # --- 2) Второй запрос: редкости для этих userId (БД2) ---
    if not SQL2_FILE.exists():
        raise FileNotFoundError("Нет файла query_2.sql")
    sql2 = SQL2_FILE.read_text(encoding="utf-8")

    df2 = fetch_df(DB2_URL, sql2, params=(user_ids,))
    if df2.empty:
        # нет редкостей — просто нули
        out = df1.copy()
        out["rares"] = 0
        out["epics"] = 0
        out["legendaries"] = 0
    else:
        df2 = norm_lower(df2)
        # привести имя колонки userid, если пришло как "userId"
        if "userid" not in df2.columns:
            for c in df2.columns:
                if c.strip('"').lower() == 'userid':
                    df2 = df2.rename(columns={c: "userid"})
                    break
        for k in ["rares", "epics", "legendaries"]:
            if k not in df2.columns:
                df2[k] = 0

        # мёрдж по userid (left — чтобы сохранить ровно пользователей из df1)
        out = df1.merge(df2[["userid", "rares", "epics", "legendaries"]], on="userid", how="left")
        out[["rares", "epics", "legendaries"]] = out[["rares", "epics", "legendaries"]].fillna(0).astype(int)

    # --- 3) Порядок колонок и сохранение итогового CSV ---
    out = out[["username", "gold", "rares", "epics", "legendaries", "userid"]]
    out.to_csv(OUT2, index=False, encoding="utf-8")
    print(f"Сохранено {len(out)} строк в {OUT2}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
