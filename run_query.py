#!/usr/bin/env python3
import os
import sys
import csv
from decimal import Decimal
from contextlib import contextmanager
from tempfile import NamedTemporaryFile

import psycopg2
from psycopg2.extras import execute_values

# ------------------ utils ------------------

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

def to_num(x):
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal(0)

def read_blacklist_ids(path: str) -> set[str]:
    ids = set()
    if not os.path.exists(path):
        print(f"[INFO] Blacklist not found at {path} — skipping.")
        return ids
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames:
            key = None
            for fn in reader.fieldnames:
                if fn and fn.strip('"').strip().lower() in ("userid", "user_id", "user id"):
                    key = fn
                    break
            if key is None:
                f.seek(0)
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if row:
                        v = str(row[0]).strip()
                        if v:
                            ids.add(v)
            else:
                for row in reader:
                    v = str(row.get(key, "")).strip()
                    if v:
                        ids.add(v)
        else:
            f.seek(0)
            reader = csv.reader(f)
            for row in reader:
                if row:
                    v = str(row[0]).strip()
                    if v:
                        ids.add(v)
    print(f"[INFO] Loaded {len(ids)} blacklisted ids from {path}.")
    return ids

# ------------------ SSH tunnel ------------------

@contextmanager
def ssh_tunnel_if_needed(use_ssh: bool, db_host: str, db_port: int,
                         ssh_host: str | None, ssh_port: int | None,
                         ssh_user: str | None, ssh_private_key: str | None):
    if not use_ssh:
        yield db_host, db_port, None
        return
    try:
        from sshtunnel import SSHTunnelForwarder
    except Exception as e:
        die(f"sshtunnel is required when SSH is enabled: {e}")
    if not ssh_host or not ssh_user or not ssh_private_key:
        die("SSH is enabled, but SSH host/user/key are missing (SSH*_HOST/SSH*_USER/SSH*_PRIVATE_KEY).")

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

# ------------------ DB helpers ------------------

def build_connect_kwargs(db_name=None, db_user=None, db_pass=None, sslmode=None, stmt_timeout_ms="0", dsn_url=None):
    if dsn_url:
        return {"dsn": dsn_url, "connect_timeout": 30,
                "options": f"-c statement_timeout={(stmt_timeout_ms or '0').strip() or '0'}"}
    kw = {"dbname": db_name, "user": db_user, "password": db_pass, "connect_timeout": 30,
          "options": f"-c statement_timeout={(stmt_timeout_ms or '0').strip() or '0'}"}
    if sslmode:
        kw["sslmode"] = sslmode
    return kw

def run_select_rows(connect_kwargs, host, port, sql, search_path_env: str | None, tag: str):
    kw = dict(connect_kwargs)
    kw["host"] = host
    kw["port"] = int(port)
    with psycopg2.connect(**kw) as conn:
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
            if search_path_env:
                cur.execute(f"SET search_path = {search_path_env}")
            cur.execute("SHOW TIME ZONE"); tz = cur.fetchone()[0]
            cur.execute("SHOW search_path"); sp_now = cur.fetchone()[0]
            cur.execute("SELECT current_database(), current_user")
            dbname, dbuser = cur.fetchone()
            print(f"[{tag}] db={dbname} user={dbuser} tz={tz} search_path={sp_now}")

            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
    return cols, rows

# ------------------ main ------------------

def main():
    # файлы/лимиты
    SQL_FILE = os.getenv("SQL_FILE", "data.sql")                # DB1
    USER_SQL_FILE = os.getenv("USER_SQL_FILE", "user_data.sql") # DB2 (WITH ids(id, ord) AS (VALUES %s) ...)
    RAW_CSV = os.getenv("OUTPUT_CSV", "raw_data.csv")
    RESULT_CSV = os.getenv("RESULT_CSV", "result_data.csv")
    BLACKLIST_FILE = os.getenv("BLACKLIST_CSV", "black_list.csv")
    TOP_N = int(os.getenv("TOP_N", "3000"))

    # ---------- DB1 (events / data.sql) ----------
    DB1_URL = os.getenv("DB1_URL")
    DB1_HOST = os.getenv("DB1_HOST"); DB1_PORT = int(os.getenv("DB1_PORT", "5432"))
    DB1_NAME = os.getenv("DB1_NAME"); DB1_USER = os.getenv("DB1_USER"); DB1_PASSWORD = os.getenv("DB1_PASSWORD")
    DB1_SSLMODE = os.getenv("DB1_SSLMODE"); DB1_STMT_MS = os.getenv("DB1_STATEMENT_TIMEOUT_MS", "0")
    DB1_SEARCH_PATH = os.getenv("DB1_SEARCH_PATH")

    USE_SSH_GLOBAL = os.getenv("USE_SSH", "false").strip().lower() in ("1","true","yes","on")
    USE_SSH_DB1 = os.getenv("USE_SSH_DB1", str(USE_SSH_GLOBAL)).strip().lower() in ("1","true","yes","on")

    SSH1_HOST = os.getenv("SSH1_HOST"); SSH1_PORT = os.getenv("SSH1_PORT", "22")
    SSH1_USER = os.getenv("SSH1_USER"); SSH1_PRIVATE_KEY = os.getenv("SSH1_PRIVATE_KEY")

    sql1 = read_sql(SQL_FILE)

    if DB1_URL:
        db1_kwargs = {"dsn": DB1_URL, "connect_timeout": 30, "options": f"-c statement_timeout={DB1_STMT_MS or '0'}"}
        host_for_db1 = "localhost"; port_for_db1 = 5432
    else:
        for v, n in [(DB1_HOST, "DB1_HOST"), (DB1_NAME, "DB1_NAME"), (DB1_USER, "DB1_USER"), (DB1_PASSWORD, "DB1_PASSWORD")]:
            if not v: die(f"Missing env: {n}")
        db1_kwargs = build_connect_kwargs(DB1_NAME, DB1_USER, DB1_PASSWORD, DB1_SSLMODE, DB1_STMT_MS)
        host_for_db1 = DB1_HOST; port_for_db1 = DB1_PORT

    with ssh_tunnel_if_needed(USE_SSH_DB1, host_for_db1, port_for_db1, SSH1_HOST, SSH1_PORT, SSH1_USER, SSH1_PRIVATE_KEY) as (h1, p1, _):
        cols1, rows1 = run_select_rows(db1_kwargs, h1, p1, sql1, DB1_SEARCH_PATH, "DB1")

    # Сохраняем сырые результаты
    save_csv(cols1, rows1, RAW_CSV)
    print(f"[DEBUG] raw rows (DB1): {len(rows1)}")

    if not rows1:
        # пустой результат — пишем пустой result_data.csv и выходим
        save_csv(["username", "score", "purple", "legendaries", "userId"], [], RESULT_CSV)
        return

    # индексы колонок raw
    try:
        i_user = cols1.index("userId")
        i_score = cols1.index("score")
        i_purple = cols1.index("purple")
        i_leg = cols1.index("legendaries")
    except ValueError as e:
        die(f"Expected columns in raw_data: userId, score, purple, legendaries. Details: {e}")

    user_ids = [str(r[i_user]) for r in rows1]
    if not user_ids:
        save_csv(["username", "score", "purple", "legendaries", "userId"], [], RESULT_CSV)
        return

    # ---------- DB2 (users / user_data.sql) ----------
    DB2_URL = os.getenv("DB2_URL")
    DB2_HOST = os.getenv("DB2_HOST"); DB2_PORT = int(os.getenv("DB2_PORT", "5432"))
    DB2_NAME = os.getenv("DB2_NAME"); DB2_USER = os.getenv("DB2_USER"); DB2_PASSWORD = os.getenv("DB2_PASSWORD")
    DB2_SSLMODE = os.getenv("DB2_SSLMODE"); DB2_STMT_MS = os.getenv("DB2_STATEMENT_TIMEOUT_MS", DB1_STMT_MS)
    DB2_SEARCH_PATH = os.getenv("DB2_SEARCH_PATH")

    USE_SSH_DB2 = os.getenv("USE_SSH_DB2", str(USE_SSH_GLOBAL)).strip().lower() in ("1","true","yes","on")
    SSH2_HOST = os.getenv("SSH2_HOST") or SSH1_HOST
    SSH2_PORT = os.getenv("SSH2_PORT", "22") or SSH1_PORT
    SSH2_USER = os.getenv("SSH2_USER") or SSH1_USER
    SSH2_PRIVATE_KEY = os.getenv("SSH2_PRIVATE_KEY") or SSH1_PRIVATE_KEY

    sql2 = read_sql(USER_SQL_FILE)

    if DB2_URL:
        db2_kwargs = {"dsn": DB2_URL, "connect_timeout": 30, "options": f"-c statement_timeout={DB2_STMT_MS or '0'}"}
        host_for_db2 = "localhost"; port_for_db2 = 5432
    else:
        for v, n in [(DB2_HOST, "DB2_HOST"), (DB2_NAME, "DB2_NAME"), (DB2_USER, "DB2_USER"), (DB2_PASSWORD, "DB2_PASSWORD")]:
            if not v: die(f"Missing env: {n}")
        db2_kwargs = build_connect_kwargs(DB2_NAME, DB2_USER, DB2_PASSWORD, DB2_SSLMODE, DB2_STMT_MS)
        host_for_db2 = DB2_HOST; port_for_db2 = DB2_PORT

    def fetch_usernames_with_values(pairs):
        """Исполняет user_data.sql с VALUES %s корректно: либо одним заходом, либо чанками и аккумулирует результат."""
        with ssh_tunnel_if_needed(USE_SSH_DB2, host_for_db2, port_for_db2, SSH2_HOST, SSH2_PORT, SSH2_USER, SSH2_PRIVATE_KEY) as (h2, p2, _):
            kw = dict(db2_kwargs); kw["host"] = h2; kw["port"] = int(p2)
            with psycopg2.connect(**kw) as conn:
                with conn.cursor() as cur:
                    cur.execute("SET TIME ZONE 'UTC'")
                    if DB2_SEARCH_PATH:
                        cur.execute(f"SET search_path = {DB2_SEARCH_PATH}")

                    total_pairs = len(pairs)
                    # предел по числу параметров в одном запросе (Postgres ~65535)
                    # на пару приходится 2 параметра (%s,%s)
                    MAX_PARAMS = 60000
                    max_pairs_per_stmt = max(1, min(total_pairs, MAX_PARAMS // 2))

                    rows_acc = []
                    cols_out = None
                    start = 0
                    while start < total_pairs:
                        end = min(total_pairs, start + max_pairs_per_stmt)
                        chunk = pairs[start:end]
                        # ВАЖНО: page_size=len(chunk) — заставляем выполнить ОДИН раз на chunk
                        execute_values(cur, sql2, chunk, template="(%s,%s)", page_size=len(chunk))
                        cols = [d[0] for d in cur.description]
                        if cols_out is None:
                            cols_out = cols
                        rows_acc.extend(cur.fetchall())
                        start = end

                    # сортируем по ord, чтобы восстановить общий порядок
                    try:
                        j_ord = cols_out.index("ord")
                        rows_acc.sort(key=lambda r: int(r[j_ord]))
                    except Exception:
                        pass

                    return cols_out, rows_acc

    # формируем (id, ord)
    pairs = [(uid, idx + 1) for idx, uid in enumerate(user_ids)]
    cols2, rows2 = fetch_usernames_with_values(pairs)

    # карта userId -> username
    try:
        j_user = cols2.index("userId")
        j_uname = cols2.index("username")
    except ValueError as e:
        die(f"user_data.sql must return columns: userId, username (and ord). Details: {e}")

    uname_by_id = {str(r[j_user]): (r[j_uname] or str(r[j_user])) for r in rows2}
    allowed_ids = set(uname_by_id.keys())
    print(f"[INFO] Allowed after DB2 filters: {len(allowed_ids)} / input={len(user_ids)}")

    # ---------- blacklist ----------
    black_ids = read_blacklist_ids(BLACKLIST_FILE)
    if black_ids:
        print(f"[INFO] Blacklist size: {len(black_ids)}")

    # ---------- build result ----------
    filtered = []
    removed_black = 0
    removed_not_allowed = 0
    for r in rows1:
        uid = str(r[i_user])
        if uid not in allowed_ids:
            removed_not_allowed += 1
            continue
        if uid in black_ids:
            removed_black += 1
            continue
        filtered.append(r)

    print(f"[INFO] Filtered counts: kept={len(filtered)}, removed_by_createdAt/line={removed_not_allowed}, removed_by_blacklist={removed_black}")

    # сортируем и ограничиваем TOP_N
    filtered.sort(key=lambda r: (to_num(r[i_score]) * -1,
                                 to_num(r[i_purple]) * -1,
                                 to_num(r[i_leg]) * -1,
                                 str(r[i_user])))
    top_rows = filtered[:TOP_N]

    # формируем result_data.csv
    result_cols = ["username", "score", "purple", "legendaries", "userId"]
    result_rows = []
    for r in top_rows:
        uid = str(r[i_user])
        uname = uname_by_id.get(uid, uid)
        result_rows.append([uname, r[i_score], r[i_purple], r[i_leg], uid])

    save_csv(result_cols, result_rows, RESULT_CSV)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        die(str(e))
