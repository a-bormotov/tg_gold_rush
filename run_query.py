import os
import psycopg2
import pandas as pd
from pathlib import Path

DB_URL = os.getenv("DB_URL")

OUTPUT_FILE = Path("user_gold.csv")

def main():
    if not DB_URL:
        raise RuntimeError("Не задана переменная окружения DB_URL")

    with open("query_1.sql", "r", encoding="utf-8") as f:
        sql = f.read()

    conn = psycopg2.connect(DB_URL)
    try:
        df = pd.read_sql(sql, conn)
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"Сохранено {len(df)} строк в {OUTPUT_FILE}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
