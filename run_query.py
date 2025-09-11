import os
import sys
from pathlib import Path

import psycopg2
import pandas as pd


# Подключения: выставляются в workflow / окружении
DB1_URL = os.getenv("DB1_URL")  # БД ресурсов/пользователей/транзакций (query_1.sql)
DB2_URL = os.getenv("DB2_URL")  # БД событий (query_2.sql)

SQL1_FILE = Path("query_1.sql")
SQL2_FILE = Path("query_2.sql")

OUT_GOLD  = Path("user_gold.csv")
OUT_TOTAL = Path("user_total.csv")


def fetch_df(conn_url: str, sql: str, params=None) -> pd.DataFrame:
    """Выполнить SQL и вернуть DataFrame."""
    if not conn_url:
        raise RuntimeError("Не задан URL подключения к БД.")
    with psycopg2.connect(conn_url) as conn:
        return pd.read_sql(sql, conn, params=params)


def norm_lower(df: pd.DataFrame) -> pd.DataFrame:
    """Привести имена колонок к нижнему регистру (без лишних пробелов)."""
    return df.rename(columns={c: c.lower().strip() for c in df.columns})


def ensure_userid(df: pd.DataFrame, prefer: str = "userid") -> pd.DataFrame:
    """
    Гарантировать наличие колонки 'userid' (lowercase).
    Переименуем возможные варианты: "userId", "User ID", 'user id', и т.п.
    """
    if "userid" in df.columns:
        return df
    for c in list(df.columns):
        base = c.strip('"').lower()
        if base in ("userid", "user id", "user_id"):
            return df.rename(columns={c: "userid"})
    # Если не нашли — бросаем осмысленную ошибку
    raise RuntimeError(
        f"Не найдена колонка userId в результате запроса. Имеющиеся колонки: {df.columns.tolist()}"
    )


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
        pd.DataFrame(columns=["username", "gold", "rares", "epics", "legendaries", "userid"]).to_csv(
            OUT_TOTAL, index=False, encoding="utf-8"
        )
        print(f"[query_2] Пустой результат. Созданы пустые {OUT_GOLD} и {OUT_TOTAL}.")
        return

    # Нормализуем колонки и гарантируем userid
    df2 = norm_lower(df2)
    df2 = ensure_userid(df2)
    # Гарантируем наличие счётчиков редкостей
    for k in ("rares", "epics", "legendaries"):
        if k not in df2.columns:
            df2[k] = 0

    user_ids = pd.unique(df2["userid"]).tolist()
    if not user_ids:
        pd.DataFrame(columns=["username", "gold", "userid"]).to_csv(OUT_GOLD, index=False, encoding="utf-8")
        pd.DataFrame(columns=["username", "gold", "rares", "epics", "legendaries", "userid"]).to_csv(
            OUT_TOTAL, index=False, encoding="utf-8"
        )
        print("[query_2] Нет userId. Созданы пустые CSV.")
        return

    # --- ШАГ 2: query_1.sql (БД1) — из этих userId оставить только с транзакциями и взять gold/username ---
    if not SQL1_FILE.exists():
        raise FileNotFoundError("Нет файла query_1.sql рядом со скриптом.")

    sql1 = SQL1_FILE.read_text(encoding="utf-8").strip()

    # ВАЖНО: query_1.sql должен уметь фильтроваться по ANY(%s) по колонке "userId"
    # Пример внутри query_1.sql:
    #   ... WHERE ur."resourceType"='gold'
    #       AND ur."userId" = ANY(%s)
    #       AND (EXISTS(...) OR EXISTS(...) OR EXISTS(...))
    try:
        df1 = fetch_df(DB1_URL, sql1, params=(user_ids,))
    except Exception as e:
        # На случай, если внутри query_1.sql другой алиас для userId — fallback:
        print(
            f"[query_1] Предупреждение: не удалось выполнить с параметрами ANY(%s): {e}\n"
            f"Попробую вытянуть всё и отфильтровать в памяти (медленнее).",
            file=sys.stderr,
        )
        df1_all = fetch_df(DB1_URL, sql1)  # без фильтра — не всегда возможно, зависит от SQL
        df1_all = norm_lower(df1_all)
        # Поищем колонку с userId и отфильтруем
        try:
            df1_all = ensure_userid(df1_all)
        except Exception as e2:
            raise RuntimeError(
                f"Не удалось применить фильтр по userId ни в БД, ни в памяти: {e2}"
            )
        df1 = df1_all[df1_all["userid"].isin(user_ids)].copy()

    if df1.empty:
        # Никто из списка не прошёл фильтр по транзакциям
        pd.DataFrame(columns=["username", "gold", "userid"]).to_csv(OUT_GOLD, index=False, encoding="utf-8")
        pd.DataFrame(columns=["username", "gold", "rares", "epics", "legendaries", "userid"]).to_csv(
            OUT_TOTAL, index=False, encoding="utf-8"
        )
        print("[query_1] После фильтра по транзакциям пользователей нет. Созданы пустые CSV.")
        return

    # Нормализуем и проверим нужные колонки
    df1 = norm_lower(df1)
    # Приведём userId → userid, если нужно
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

    # --- Сохранить user_gold.csv (чистый результат первого запроса) ---
    df1[["username", "gold", "userid"]].to_csv(OUT_GOLD, index=False, encoding="utf-8")
    print(f"Сохранено {len(df1)} строк в {OUT_GOLD}")

    # --- ШАГ 3: объединить с редкостями и сохранить user_total.csv ---
    df2_small = df2[["userid", "rares", "epics", "legendaries"]].copy()
    total = df1.merge(df2_small, on="userid", how="left")
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
