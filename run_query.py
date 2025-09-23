#!/usr/bin/env python3
import os
import sys
import csv
from contextlib import contextmanager
from tempfile import NamedTemporaryFile

import psycopg2

# ---------- утилиты ----------
def die(msg, code=1):
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(code)

def read_sql(path: str) -> str:
    if not os.path.exists(path):
        die(f"SQL file not found: {path}")
    return open(path, "r", encoding="utf-8").read().strip().rstrip(";")

def save_csv(columns, rows, out_path):
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(columns)
        for r in rows:
            w.writerow(list(r))
    print(f"[OK] Saved CSV → {out_path} ({len(rows)} rows)")

# ---------- SSH-туннель (опционально) ----------
@contextmanager
def ssh_tunnel_if_needed(
    use_ssh: bool,
    db_host: str,
    db_port: int,
    ssh_host: str | None,
    ssh_port: int | None,
    ssh_user: str | None,
    ssh_private_key: str | None,
):
    if not use_ssh:
        yield db_host, db_port, None
        return

    try:
        from sshtunnel import SSHTunnelForwarder
    except Exception as e:
        die(f"sshtunnel is required when SSH is enabled: {e}")

    if not ssh_host or not ssh_user or not ssh_private_key:
        die("SSH is enabled, but SSH host/user/key are missing (SSH1_HOST/SSH1_USER/SSH1_PRIVATE_KEY).")

    # пишем ключ во временный файл
    with NamedTemporaryFile("w", delete=False) as key_file:
        key_file.write(ssh_private_key)
        key_path = key_file.name
    os.chmod(key_path, 0o600)

    tunnel = None
    try:
        tunnel = SSHTunnelForwarder(
            (ssh_host, int(ssh_port or 22)),
            ssh_username=ssh_user,
            ssh_pkey=key_path,
            remote_bind_address=(db_host, int(db_port)),
            local_bind_address=("127.0.0.1", 0),
        )
        tunnel.start()
        print(f"[INFO] SSH tunnel: 127.0.0.1:{tunnel.local_bind_port} → {db_host}:{db_port}")
        yield "127.0.0.1", tunnel.local_bind_port, tunnel
    finally:
        try:
            if tunnel and tunnel.is_active:
                tunnel.stop()
                print("[INFO] SSH tunnel closed.")
        finally:
            try:
                os.remove(key_path)
            except Exception:
                pass

# ---------- выполнение SELECT ----------
def run_select(db_kwargs: dict, host: str, port: int, sql: str):
    kw = dict(db_kwargs)
    kw["host"] = host
    kw["port"] = int(port)

    with psycopg2.connect(**kw) as conn:
        with conn.cursor() as cur:
            # делаем сессию предсказуемой
            cur.execute("SET TIME ZONE 'UTC'")
            sp = os.getenv("DB1_SEARCH_PATH")
            if sp:
                cur.execute(f"SET search_path = {sp}")

            # диагностика
            cur.execute("SHOW TIME ZONE"); tz = cur.fetchone()[0]
            cur.execute("SHOW search_path"); sps = cur.fetchone()[0]
            cur.execute("SELECT current_database(), current_user")
            dbname, dbuser = cur.fetchone()
            print(f"[DB1] db={dbname} user={dbuser} tz={tz} search_path={sps}")

            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            return cols, rows

# ---------- main ----------
def main():
    # файлы
    SQL_FILE = os.getenv("SQL_FILE", "data.sql")
    OUTPUT_CSV = os.getenv("OUTPUT_CSV", "raw_data.csv")

    # подключение к DB1
    DB1_URL = os.getenv("DB1_URL")  # если задан, используется DSN одной строкой
    DB1_HOST = os.getenv("DB1_HOST")
    DB1_PORT = int(os.getenv("DB1_PORT", "5432"))
    DB1_NAME = os.getenv("DB1_NAME")
    DB1_USER = os.getenv("DB1_USER")
    DB1_PASSWORD = os.getenv("DB1_PASSWORD")
    DB1_SSLMODE = os.getenv("DB1_SSLMODE")  # optional
    DB1_STMT_MS = (os.getenv("DB1_STATEMENT_TIMEOUT_MS") or "0").strip()  # optional

    # SSH (опционально)
    USE_SSH_GLOBAL = (os.getenv("USE_SSH", "false").strip().lower() in ("1","true","yes","on"))
    USE_SSH_DB1 = (os.getenv("USE_SSH_DB1", str(USE_SSH_GLOBAL)).strip().lower() in ("1","true","yes","on"))
    SSH1_HOST = os.getenv("SSH1_HOST")
    SSH1_PORT = os.getenv("SSH1_PORT", "22")
    SSH1_USER = os.getenv("SSH1_USER")
    SSH1_PRIVATE_KEY = os.getenv("SSH1_PRIVATE_KEY")

    sql = read_sql(SQL_FILE)

    # формируем kwargs для psycopg2
    if DB1_URL:
        db_kwargs = {
            "dsn": DB1_URL,
            "connect_timeout": 30,
            "options": f"-c statement_timeout={DB1_STMT_MS or '0'}",
        }
    else:
        for v, n in [(DB1_HOST, "DB1_HOST"), (DB1_NAME, "DB1_NAME"), (DB1_USER, "DB1_USER"), (DB1_PASSWORD, "DB1_PASSWORD")]:
            if not v:
                die(f"Missing required env var: {n}")
        db_kwargs = {
            "dbname": DB1_NAME,
            "user": DB1_USER,
            "password": DB1_PASSWORD,
            "connect_timeout": 30,
            "options": f"-c statement_timeout={DB1_STMT_MS or '0'}",
        }
        if DB1_SSLMODE:
            db_kwargs["sslmode"] = DB1_SSLMODE

    # выполняем запрос (через SSH при необходимости)
    with ssh_tunnel_if_needed(USE_SSH_DB1, DB1_HOST or "localhost", DB1_PORT, SSH1_HOST, SSH1_PORT, SSH1_USER, SSH1_PRIVATE_KEY) as (h, p, _t):
        cols, rows = run_select(db_kwargs, h, p, sql)

    # сохраняем CSV
    save_csv(cols, rows, OUTPUT_CSV)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        die(str(e))
