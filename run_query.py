#!/usr/bin/env python3
import os
import sys
import csv
from decimal import Decimal
from contextlib import contextmanager
from tempfile import NamedTemporaryFile

import psycopg2
from psycopg2.extras import execute_values

def die(msg, code=1):
    print(f"[ERROR] {msg}", file=sys.stderr); sys.exit(code)

def bool_env(name, default=False):
    return os.getenv(name, str(default)).strip().lower() in ("1","true","yes","y","on")

def read_sql(path: str) -> str:
    if not os.path.exists(path): die(f"SQL file not found: {path}")
    return open(path, "r", encoding="utf-8").read().strip().rstrip(";")

@contextmanager
def ssh_tunnel_if_needed(use_ssh: bool, db_host: str, db_port: int,
                         ssh_host: str|None, ssh_port: int|None,
                         ssh_user: str|None, ssh_private_key: str|None):
    if not use_ssh:
        yield db_host, db_port, None; return
    try:
        from sshtunnel import SSHTunnelForwarder
    except Exception as e:
        die(f"sshtunnel is required when SSH is enabled: {e}")
    if not ssh_host or not ssh_user or not ssh_private_key:
        die("SSH is enabled, but SSH host/user/key are missing (check SSH*_HOST/SSH*_USER/SSH*_PRIVATE_KEY).")

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
        lh, lp = tunnel.local_bind_host, tunnel.local_bind_port
        print(f"[INFO] SSH tunnel: {lh}:{lp} → {db_host}:{db_port}")
        yield lh, lp, tunnel
    finally:
        try:
            if tunnel and tunnel.is_active:
                tunnel.stop(); print("[INFO] SSH tunnel closed.")
        finally:
            try: os.remove(key_path)
            except Exception: pass

def build_connect_kwargs(db_name=None, db_user=None, db_pass=None, sslmode=None, stmt_timeout_ms="0", dsn_url=None):
    if dsn_url:
        return {"dsn": dsn_url, "connect_timeout": 30, "options": f"-c statement_timeout={(stmt_timeout_ms or '0').strip() or '0'}"}
    kw = {"dbname": db_name, "user": db_user, "password": db_pass, "connect_timeout": 30,
          "options": f"-c statement_timeout={(stmt_timeout_ms or '0').strip() or '0'}"}
    if sslmode: kw["sslmode"] = sslmode
    return kw

def run_select_rows(connect_kwargs, host, port, sql, params=None):
    kw = dict(connect_kwargs)
    if host is not None: kw["host"] = host
    if port is not None: kw["port"] = int(port)
    with psycopg2.connect(**kw) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
    return cols, rows

def save_csv(cols, rows, path):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f); w.writerow(cols); [w.writerow(list(r)) for r in rows]
    print(f"[OK] Saved CSV → {path} ({len(rows)} rows)")

def main():
    # ---- DB1
    DB1_HOST=os.getenv("DB1_HOST"); DB1_NAME=os.getenv("DB1_NAME"); DB1_PASSWORD=os.getenv("DB1_PASSWORD")
    DB1_PORT=int(os.getenv("DB1_PORT","5432")); DB1_USER=os.getenv("DB1_USER")
    DB1_SSLMODE=os.getenv("DB1_SSLMODE"); DB1_STMT_MS=os.getenv("DB1_STATEMENT_TIMEOUT_MS","0")

    # ---- DB2
    DB2_HOST=os.getenv("DB2_HOST"); DB2_NAME=os.getenv("DB2_NAME"); DB2_PASSWORD=os.getenv("DB2_PASSWORD")
    DB2_PORT=int(os.getenv("DB2_PORT","5432")); DB2_USER=os.getenv("DB2_USER")
    DB2_SSLMODE=os.getenv("DB2_SSLMODE"); DB2_STMT_MS=os.getenv("DB2_STATEMENT_TIMEOUT_MS", DB1_STMT_MS)

    # ---- SSH flags + creds
    USE_SSH_GLOBAL = bool_env("USE_SSH", False)
    USE_SSH_DB1 = bool_env("USE_SSH_DB1", USE_SSH_GLOBAL)
    USE_SSH_DB2 = bool_env("USE_SSH_DB2", USE_SSH_GLOBAL)

    SSH1_HOST=os.getenv("SSH1_HOST"); SSH1_PORT=os.getenv("SSH1_PORT","22")
    SSH1_USER=os.getenv("SSH1_USER"); SSH1_PRIVATE_KEY=os.getenv("SSH1_PRIVATE_KEY")

    SSH2_HOST=os.getenv("SSH2_HOST") or SSH1_HOST
    SSH2_PORT=os.getenv("SSH2_PORT","22") or SSH1_PORT
    SSH2_USER=os.getenv("SSH2_USER") or SSH1_USER
    SSH2_PRIVATE_KEY=os.getenv("SSH2_PRIVATE_KEY") or SSH1_PRIVATE_KEY

    # ---- files
    SQL_FILE=os.getenv("SQL_FILE","data.sql")
    USER_SQL_FILE=os.getenv("USER_SQL_FILE","user_data.sql")
    RAW_CSV=os.getenv("OUTPUT_CSV","raw_data.csv")
    RESULT_CSV=os.getenv("RESULT_CSV","result_data.csv")
    TOP_N=int(os.getenv("TOP_N","3000"))

    # ---- validation
    for v,n in [(DB1_HOST,"DB1_HOST"),(DB1_NAME,"DB1_NAME"),(DB1_PASSWORD,"DB1_PASSWORD"),(DB1_USER,"DB1_USER")]:
        if not v: die(f"Missing required env var: {n}")
    for v,n in [(DB2_HOST,"DB2_HOST"),(DB2_NAME,"DB2_NAME"),(DB2_PASSWORD,"DB2_PASSWORD"),(DB2_USER,"DB2_USER")]:
        if not v: die(f"Missing required env var: {n}")

    sql1 = read_sql(SQL_FILE)
    sql2 = read_sql(USER_SQL_FILE)

    # ---- DB1: основной запрос
    db1_kwargs = build_connect_kwargs(DB1_NAME, DB1_USER, DB1_PASSWORD, DB1_SSLMODE, DB1_STMT_MS)
    with ssh_tunnel_if_needed(USE_SSH_DB1, DB1_HOST, DB1_PORT, SSH1_HOST, SSH1_PORT, SSH1_USER, SSH1_PRIVATE_KEY) as (h1,p1,_):
        cols1, rows1 = run_select_rows(db1_kwargs, h1, p1, sql1)
    save_csv(cols1, rows1, RAW_CSV)

    # столбцы и сортировка TOP_N
    try:
        i_user = cols1.index("userId"); i_score = cols1.index("score"); i_purple = cols1.index("purple"); i_leg = cols1.index("legendaries")
    except ValueError as e:
        die(f"Expected columns not found in first query result: {e}")

    def to_num(x):
        try: return Decimal(str(x))
        except Exception: return Decimal(0)

    rows_sorted = sorted(
        rows1,
        key=lambda r: (to_num(r[i_score])*-1, to_num(r[i_purple])*-1, to_num(r[i_leg])*-1, str(r[i_user]))
    )
    top_rows = rows_sorted[:TOP_N]
    top_user_ids = [str(r[i_user]) for r in top_rows]  # строки

    # ---- DB2: usernames без temp table (read-only friendly)
    db2_kwargs = build_connect_kwargs(DB2_NAME, DB2_USER, DB2_PASSWORD, DB2_SSLMODE, DB2_STMT_MS)
    with ssh_tunnel_if_needed(USE_SSH_DB2, DB2_HOST, DB2_PORT, SSH2_HOST, SSH2_PORT, SSH2_USER, SSH2_PRIVATE_KEY) as (h2,p2,_):
        kw = dict(db2_kwargs); kw["host"]=h2; kw["port"]=int(p2)
        with psycopg2.connect(**kw) as conn:
            with conn.cursor() as cur:
                # строим VALUES (id, ord), (id, ord), ...
                data = [(uid, idx+1) for idx, uid in enumerate(top_user_ids)]
                # user_data.sql содержит "WITH ids(id, ord) AS (VALUES %s) SELECT ..."
                execute_values(cur, sql2, data, template="(%s,%s)")
                cols2 = [d[0] for d in cur.description]
                rows2 = cur.fetchall()

    uname_by_id = {row[0]: row[1] for row in rows2}  # userId(text) -> username

    # ---- финальный CSV
    result_cols = ["username","score","purple","legendaries","userId"]
    result_rows = []
    for r in top_rows:
        uid = str(r[i_user])
        uname = uname_by_id.get(uid) or uid
        result_rows.append([uname, r[i_score], r[i_purple], r[i_leg], uid])
    save_csv(result_cols, result_rows, RESULT_CSV)

if __name__ == "__main__":
    main()
