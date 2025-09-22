#!/usr/bin/env python3
import os
import sys
import tempfile
from contextlib import contextmanager

import psycopg2

USE_SSH = os.getenv("USE_SSH", "false").strip().lower() in ("1", "true", "yes", "y", "on")

DB_HOST = os.getenv("DB1_HOST")
DB_NAME = os.getenv("DB1_NAME")
DB_PASS = os.getenv("DB1_PASSWORD")
DB_PORT = int(os.getenv("DB1_PORT", "5432"))
DB_USER = os.getenv("DB1_USER")

SSH_HOST = os.getenv("SSH1_HOST")
SSH_PORT = int(os.getenv("SSH1_PORT", "22"))
SSH_USER = os.getenv("SSH1_USER")
SSH_PRIVATE_KEY = os.getenv("SSH1_PRIVATE_KEY")

SQL_FILE = os.getenv("SQL_FILE", "data.sql")
OUTPUT_CSV = os.getenv("OUTPUT_CSV", "raw_data.csv")

def die(msg, code=1):
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(code)

def read_sql(path: str) -> str:
    if not os.path.exists(path):
        die(f"SQL file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        # COPY (...) TO STDOUT не любит завершающую ';'
        return f.read().strip().rstrip(";")

@contextmanager
def maybe_ssh_tunnel():
    """Если USE_SSH=true — поднимаем SSH-туннель, иначе возвращаем исходные DB_HOST/DB_PORT."""
    if not USE_SSH:
        yield DB_HOST, DB_PORT, None
        return

    try:
        from sshtunnel import SSHTunnelForwarder
    except Exception as e:
        die(f"sshtunnel is required when USE_SSH=true: {e}")

    if not SSH_PRIVATE_KEY:
        die("USE_SSH=true, но SSH1_PRIVATE_KEY не задан.")

    # сохраняем ключ во временный файл (600)
    with tempfile.NamedTemporaryFile("w", delete=False) as key_file:
        key_file.write(SSH_PRIVATE_KEY)
        key_path = key_file.name
    os.chmod(key_path, 0o600)

    tunnel = None
    try:
        tunnel = SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_pkey=key_path,
            remote_bind_address=(DB_HOST, DB_PORT),
            local_bind_address=("127.0.0.1", 0),
        )
        tunnel.start()
        local_host, local_port = tunnel.local_bind_host, tunnel.local_bind_port
        print(f"[INFO] SSH tunnel: {local_host}:{local_port} → {DB_HOST}:{DB_PORT}")
        yield local_host, local_port, tunnel
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

def run_query_to_csv(sql: str, out_csv: str):
    connect_kwargs = {
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": DB_PASS,
        "connect_timeout": 30,
    }

    # SSL режим при необходимости
    if os.getenv("DB1_SSLMODE"):
        connect_kwargs["sslmode"] = os.getenv("DB1_SSLMODE")

    # Таймаут запроса через startup packet (работает даже с пулами)
    # 0 = без лимита
    stmt_timeout_ms = os.getenv("DB1_STATEMENT_TIMEOUT_MS", "0").strip() or "0"
    connect_kwargs["options"] = f"-c statement_timeout={stmt_timeout_ms}"

    with maybe_ssh_tunnel() as (host, port, _tunnel):
        connect_kwargs["host"] = host
        connect_kwargs["port"] = port

        with psycopg2.connect(**connect_kwargs) as conn:
            with conn.cursor() as cur:
                copy_sql = f"COPY ({sql}) TO STDOUT WITH CSV HEADER"
                with open(out_csv, "w", encoding="utf-8", newline="") as f:
                    cur.copy_expert(copy_sql, f)

    print(f"[OK] Saved CSV → {out_csv}")

def main():
    # проверим необходимые переменные
    for v, name in [(DB_HOST, "DB1_HOST"), (DB_NAME, "DB1_NAME"), (DB_PASS, "DB1_PASSWORD"), (DB_USER, "DB1_USER")]:
        if not v:
            die(f"Missing required env var: {name}")
    if USE_SSH and (not SSH_HOST or not SSH_USER):
        die("USE_SSH=true, но не заданы SSH1_HOST/SSH1_USER.")

    sql = read_sql(SQL_FILE)
    run_query_to_csv(sql, OUTPUT_CSV)

if __name__ == "__main__":
    main()
