#!/usr/bin/env python3
import os
import sys
import csv
import tempfile
from contextlib import contextmanager
from decimal import Decimal

import psycopg2

# ------------------------
# helpers
# ------------------------

def die(msg, code=1):
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(code)

def bool_env(name, default=False):
    val = os.getenv(name, str(default)).strip().lower()
    return val in ("1", "true", "yes", "y", "on")

def read_sql(path: str) -> str:
    if not os.path.exists(path):
        die(f"SQL file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        # COPY (...) и параметризованные execute не любят завершающую ';'
        return f.read().strip().rstrip(";")

# ------------------------
# SSH tunnels (generic)
# ------------------------

@contextmanager
def ssh_tunnel_if_needed(use_ssh: bool, db_host: str, db_port: int,
                         ssh_host: str | None, ssh_port: int | None,
                         ssh_user: str | None, ssh_private_key: str | None):
    """Если use_ssh=True — поднимаем туннель и возвращаем локальные host/port,
    иначе — возвращаем исходные db_host/db_port."""
    if not use_ssh:
        yield db_host, db_port, None
        return

    try:
        from sshtunnel import SSHTunnelForwarder
    except Exception as e:
        die(f"sshtunnel is required when SSH is enabled: {e}")

    if not ssh_host or not ssh_user or not ssh_private_key:
        die("SSH is enabled, but SSH host/user/key are missing (check SSH*_HOST/SSH*_USER/SSH*_PRIVATE_KEY).")

    # пишем ключ во временный файл (600)
    with tempfile.NamedTemporaryFile("w", delete=False) as key_file:
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
        local_host, local_port = tunnel.local_bind_host, tunnel.local_bind_port
        print(f"[INFO] SSH tunnel: {local_host}:{local_port} → {db_host}:{db_port}")
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

# ------------------------
# DB connect + queries
# ------------------------

def build_connect_kwargs(db_name, db_user, db_pass, sslmode, stmt_timeout_ms):
    kwargs = {
        "dbname": db_name,
        "user": db_user,
        "password": db_pass,
        "connect_timeout": 30,
    }
    if sslmode:
        kwargs["sslmode"] = sslmode
    # задаём statement_timeout через startup options (работает даже с пулами)
    stmt_timeout_ms = (stmt_timeout_ms or "0").strip() or "0"
    kwargs["options"] = f"-c statement_timeout={stmt_timeout_ms}"
    return kwargs

def run_select_rows(connect_kwargs, host, port, sql, params=None):
    """Выполнить SELECT и вернуть (cols, rows)."""
    connect_kwargs = dict(connect_kwargs)
    connect_kwargs["host"] = host
    connect_kwargs["port"] = int(port)

    with psycopg2.connect(**connect_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
    return cols, rows

def save_csv(cols, rows, path):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow(list(r))
    print(f"[OK] Saved CSV → {path} ({len(rows)} rows)")

# ------------------------
# main flow
# ------------------------

def main():
    # ---------- env: DB1 ----------
    DB1_HOST = os.getenv("DB1_HOST")
    DB1_NAME = os.getenv("DB1_NAME")
    DB1_PASSWORD = os.getenv("DB1_PASSWORD")
    DB1_PORT = int(os.getenv("DB1_PORT", "5432"))
    DB1_USER = os.getenv("DB1_USER")
    DB1_SSLMODE = os.getenv("DB1_SSLMODE")
    DB1_STMT_MS = os.getenv("DB1_STATEMENT_TIMEOUT_MS", "0")

    # SSH for DB1 (uses generic USE_SSH if per-DB not provided)
    USE_SSH_GLOBAL = bool_env("USE_SSH", False)
    USE_SSH_DB1 = bool_env("USE_SSH_DB1", USE_SSH_GLOBAL)

    SSH1_HOST = os.getenv("SSH1_HOST")
    SSH1_PORT = os.getenv("SSH1_PORT", "22")
    SSH1_PRIVATE_KEY = os.getenv("SSH1_PRIVATE_KEY")
    SSH1_USER = os.getenv("SSH1_USER")

    # ---------- env: DB2 ----------
    DB2_HOST = os.getenv("DB2_HOST")
    DB2_NAME = os.getenv("DB2_NAME")
    DB2_PASSWORD = os.getenv("DB2_PASSWORD")
    DB2_PORT = int(os.getenv("DB2_PORT", "5432"))
    DB2_USER = os.getenv("DB2_USER")
    DB2_SSLMODE = os.getenv("DB2_SSLMODE")
    DB2_STMT_MS = os.getenv("DB2_STATEMENT_TIMEOUT_MS", DB1_STMT_MS)

    # SSH for DB2
    USE_SSH_DB2 = bool_env("USE_SSH_DB2", USE_SSH_GLOBAL)
    SSH2_HOST = os.getenv("SSH2_HOST")
    SSH2_PORT = os.getenv("SSH2_PORT", "22")
    SSH2_PRIVATE_KEY = os.getenv("SSH2_PRIVATE_KEY")
    SSH2_USER = os.getenv("SSH2_USER")

    # ---------- files ----------
    SQL_FILE = os.getenv("SQL_FILE", "data.sql")
    USER_SQL_FILE = os.getenv("USER_SQL_FILE", "user_data.sql")
    RAW_CSV = os.getenv("OUTPUT_CSV", "raw_data.csv")
    RESULT_CSV = os.getenv("RESULT_CSV", "result_data.csv")
    TOP_N = int(os.getenv("TOP_N", "3000"))

    # ---------- validation ----------
    for v, name in [(DB1_HOST, "DB1_HOST"), (DB1_NAME, "DB1_NAME"), (DB1_PASSWORD, "DB1_PASSWORD"), (DB1_USER, "DB1_USER")]:
        if not v:
            die(f"Missing required env var: {name}")
    for v, name in [(DB2_HOST, "DB2_HOST"), (DB2_NAME, "DB2_NAME"), (DB2_PASSWORD, "DB2_PASSWORD"), (DB2_USER, "DB2_USER")]:
        if not v:
            die(f"Missing required env var: {name}")

    sql1 = read_sql(SQL_FILE)
    sql2 = read_sql(USER_SQL_FILE)

    # ---------- query #1: DB1 ----------
    db1_kwargs = build_connect_kwargs(DB1_NAME, DB1_USER, DB1_PASSWORD, DB1_SSLMODE, DB1_STMT_MS)
    with ssh_tunnel_if_needed(USE_SSH_DB1, DB1_HOST, DB1_PORT, SSH1_HOST, SSH1_PORT, SSH1_USER, SSH1_PRIVATE_KEY) as (h1, p1, _t1):
        cols1, rows1 = run_select_rows(db1_kwargs, h1, p1, sql1)

    # сохраняем raw_data.csv (весь первый результат)
    save_csv(cols1, rows1, RAW_CSV)

    # найдём индексы нужных колонок
    try:
        idx_user = cols1.index("userId")
        idx_score = cols1.index("score")
        idx_purple = cols1.index("purple")
        idx_legend = cols1.index("legendaries")
    except ValueError as e:
        die(f"Expected columns not found in first query result: {e}")

    def to_num(x):
        if isinstance(x, Decimal):
            return x
        try:
            return Decimal(str(x))
        except Exception:
            return Decimal(0)

    # сортировка и TOP_N
    rows_sorted = sorted(
        rows1,
        key=lambda r: (to_num(r[idx_score]) * Decimal(-1),
                       to_num(r[idx_purple]) * Decimal(-1),
                       to_num(r[idx_legend]) * Decimal(-1),
                       r[idx_user]),
    )
    top_rows = rows_sorted[:TOP_N]
    top_user_ids = [r[idx_user] for r in top_rows]

    # приведём userId к int, если возможно (для ::bigint[])
    ids_for_db2 = []
    for uid in top_user_ids:
        try:
            ids_for_db2.append(int(uid))
        except Exception:
            # если вдруг нечисловой — пропустим из запроса во 2-ю БД
            pass

    # ---------- query #2: DB2 (usernames из файла user_data.sql) ----------
    if ids_for_db2:
        db2_kwargs = build_connect_kwargs(DB2_NAME, DB2_USER, DB2_PASSWORD, DB2_SSLMODE, DB2_STMT_MS)
        with ssh_tunnel_if_needed(USE_SSH_DB2, DB2_HOST, DB2_PORT, SSH2_HOST, SSH2_PORT, SSH2_USER, SSH2_PRIVATE_KEY) as (h2, p2, _t2):
            cols2, rows2 = run_select_rows(db2_kwargs, h2, p2, sql2, (ids_for_db2,))
        # userId -> username
        uname_by_id = {row[0]: row[1] for row in rows2}
    else:
        uname_by_id = {}

    # ---------- assemble result_data.csv ----------
    # итоговый порядок: username, score, purple, legendaries, userId
    result_cols = ["username", "score", "purple", "legendaries", "userId"]
    result_rows = []

    for r in top_rows:
        uid = r[idx_user]
        score = r[idx_score]
        purple = r[idx_purple]
        leg = r[idx_legend]

        # имя из DB2, если есть; иначе фоллбек к текстовому userId
        uname = None
        try:
            uname = uname_by_id.get(int(uid))
        except Exception:
            uname = None
        if not uname:
            uname = str(uid)

        result_rows.append([uname, score, purple, leg, uid])

    save_csv(result_cols, result_rows, RESULT_CSV)

if __name__ == "__main__":
    main()
