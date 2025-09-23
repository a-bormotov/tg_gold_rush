#!/usr/bin/env python3
import os
import sys
import csv
from decimal import Decimal
from contextlib import contextmanager
from tempfile import NamedTemporaryFile

import psycopg2
from psycopg2.extras import execute_values

# ------------- utils -------------
def die(msg, code=1):
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(code)

def bool_env(name, default=False):
    return os.getenv(name, str(default)).strip().lower() in ("1","true","yes","y","on")

def read_sql(path: str) -> str:
    if not os.path.exists(path):
        die(f"SQL file not found: {path}")
    return open(path, "r", encoding="utf-8").read().strip().rstrip(";")

def save_csv(cols, rows, path):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow(list(r))
    print(f"[OK] Saved CSV → {path} ({len(rows)} rows)")

def to_num(x):
    try:
        return Decimal(str(x))
    except Exception:
        return Decimal(0)

def read_blacklist_ids(path: str) -> set[str]:
    """Читает black_list.csv и возвращает set userId как строки. Если файла нет — пустой set."""
    ids = set()
    if not os.path.exists(path):
        print(f"[INFO] Blacklist not found at {path} — skipping.")
        return ids
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = [fn.strip() for fn in (reader.fieldnames or [])]
        key = None
        for fn in fieldnames:
            if fn.strip('"').strip().lower() in ("userid", "user_id", "user id"):
                key = fn
                break
        if key is None:
            f.seek(0)
            reader2 = csv.reader(f)
            header = next(reader2, None)
            if header and len(header) > 0 and any(h for h in header):
                f.seek(0)
                reader = csv.DictReader(f)
                first = fieldnames[0] if fieldnames else None
                for row in reader:
                    if first in row:
                        v = str(row[first]).strip()
                        if v:
                            ids.add(v)
            else:
                for row in reader2:
                    if not row:
                        continue
                    v = str(row[0]).strip()
                    if v:
                        ids.add(v)
            print(f"[INFO] Loaded {len(ids)} blacklisted ids (no header).")
            return ids

        for row in reader:
            v = str(row.get(key, "")).strip()
            if v:
                ids.add(v)
    print(f"[INFO] Loaded {len(ids)} blacklisted ids from {path}.")
    return ids

# -------- SSH tunnel helper --------
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
                tunnel.stop()
                print("[INFO] SSH tunnel closed.")
        finally:
            try:
                os.remove(key_path)
            except Exception:
                pass

# ---------- DB helpers ----------
def build_connect_kwargs(db_name=None, db_user=None, db_pass=None, sslmode=None, stmt_timeout_ms="0", dsn_url=None):
    if dsn_url:
        return {"dsn": dsn_url, "connect_timeout": 30,
                "options": f"-c statement_timeout={(stmt_timeout_ms or '0').strip() or '0'}"}
    kw = {"dbname": db_name, "user": db_user, "password": db_pass, "connect_timeout": 30,
          "options": f"-c statement_timeout={(stmt_timeout_ms or '0').strip() or '0'}"}
    if sslmode:
        kw["sslmode"] = sslmode
    return kw

def run_select_rows(connect_kwargs, host, port, sql, params=None):
    kw = dict(connect_kwargs)
    if host is not None:
        kw["host"] = host
    if port is not None:
        kw["port"] = int(port)
    with psycopg2.connect(**kw) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
    return cols, rows

# --------------- main ---------------
def main():
    # DB1 (events/score)
    DB1_HOST = os.getenv("DB1_HOST"); DB1_NAME = os.getenv("DB1_NAME"); DB1_PASSWORD = os.getenv("DB1_PASSWORD")
    DB1_PORT = int(os.getenv("DB1_PORT", "5432")); DB1_USER = os.getenv("DB1_USER")
    DB1_SSLMODE = os.getenv("DB1_SSLMODE"); DB1_STMT_MS = os.getenv("DB1_STATEMENT_TIMEOUT_MS", "0")

    # DB2 (users)
    DB2_HOST = os.getenv("DB2_HOST"); DB2_NAME = os.getenv("DB2_NAME"); DB2_PASSWORD = os.getenv("DB2_PASSWORD")
    DB2_PORT = int(os.getenv("DB2_PORT", "5432")); DB2_USER = os.getenv("DB2_USER")
    DB2_SSLMODE = os.getenv("DB2_SSLMODE"); DB2_STMT_MS = os.getenv("DB2_STATEMENT_TIMEOUT_MS", DB1_STMT_MS)

    # SSH flags + creds
    USE_SSH_GLOBAL = bool_env("USE_SSH", False)
    USE_SSH_DB1 = bool_env("USE_SSH_DB1", USE_SSH_GLOBAL)
    USE_SSH_DB2 = bool_env("USE_SSH_DB2", USE_SSH_GLOBAL)

    SSH1_HOST = os.getenv("SSH1_HOST"); SSH1_PORT = os.getenv("SSH1_PORT", "22")
    SSH1_USER = os.getenv("SSH1_USER"); SSH1_PRIVATE_KEY = os.getenv("SSH1_PRIVATE_KEY")

    SSH2_HOST = os.getenv("SSH2_HOST") or SSH1_HOST
    SSH2_PORT = os.getenv("SSH2_PORT", "22") or SSH1_PORT
    SSH2_USER = os.getenv("SSH2_USER") or SSH1_USER
    SSH2_PRIVATE_KEY = os.getenv("SSH2_PRIVATE_KEY") or SSH1_PRIVATE_KEY

    # files & limits
    SQL_FILE = os.getenv("SQL_FILE", "data.sql")                  # DB1
    USER_SQL_FILE = os.getenv("USER_SQL_FILE", "user_data.sql")   # DB2 (фильтр по createdAt + исключение 'line')
    RAW_CSV = os.getenv("OUTPUT_CSV", "raw_data.csv")
    RESULT_CSV = os.getenv("RESULT_CSV", "result_data.csv")
    BLACKLIST_FILE = os.getenv("BLACKLIST_CSV", "black_list.csv")
    TOP_N = int(os.getenv("TOP_N", "3000"))

    # validate
    for v, n in [(DB1_HOST, "DB1_HOST"), (DB1_NAME, "DB1_NAME"), (DB1_PASSWORD, "DB1_PASSWORD"), (DB1_USER, "DB1_USER")]:
        if not v:
            die(f"Missing required env var: {n}")
    for v, n in [(DB2_HOST, "DB2_HOST"), (DB2_NAME, "DB2_NAME"), (DB2_PASSWORD, "DB2_PASSWORD"), (DB2_USER, "DB2_USER")]:
        if not v:
            die(f"Missing required env var: {n}")

    sql1 = read_sql(SQL_FILE)
    sql2 = read_sql(USER_SQL_FILE)

    # 1) DB1: все игроки за окно
    db1_kwargs = build_connect_kwargs(DB1_NAME, DB1_USER, DB1_PASSWORD, DB1_SSLMODE, DB1_STMT_MS)
    with ssh_tunnel_if_needed(USE_SSH_DB1, DB1_HOST, DB1_PORT, SSH1_HOST, SSH1_PORT, SSH1_USER, SSH1_PRIVATE_KEY) as (h1, p1, _):
        cols1, rows1 = run_select_rows(db1_kwargs, h1, p1, sql1)
    save_csv(cols1, rows1, RAW_CSV)

    # NEW: если DB1 вернул 0 строк — пишем пустой результат и уходим
    if not rows1:
        print("[INFO] DB1 returned 0 rows. Writing empty result_data.csv and exiting.")
        save_csv(["username", "score", "purple", "legendaries", "userId"], [], RESULT_CSV)
        return

    # Индексы
    try:
        i_user = cols1.index("userId")
        i_score = cols1.index("score")
        i_purple = cols1.index("purple")
        i_leg = cols1.index("legendaries")
    except ValueError as e:
        die(f"Expected columns not found in first query result: {e}")

    # Все userId (строками)
    all_user_ids = [str(r[i_user]) for r in rows1]

    # NEW: если id нет — пустой результат и выход (на всякий случай)
    if not all_user_ids:
        print("[INFO] No user ids to lookup in DB2. Writing empty result_data.csv.")
        save_csv(["username", "score", "purple", "legendaries", "userId"], [], RESULT_CSV)
        return

    # 2) DB2: имена и фильтр (по SQL)
    db2_kwargs = build_connect_kwargs(DB2_NAME, DB2_USER, DB2_PASSWORD, DB2_SSLMODE, DB2_STMT_MS)
    with ssh_tunnel_if_needed(USE_SSH_DB2, DB2_HOST, DB2_PORT, SSH2_HOST, SSH2_PORT, SSH2_USER, SSH2_PRIVATE_KEY) as (h2, p2, _):
        kw = dict(db2_kwargs); kw["host"] = h2; kw["port"] = int(p2)
        with psycopg2.connect(**kw) as conn:
            with conn.cursor() as cur:
                pairs = [(uid, idx + 1) for idx, uid in enumerate(all_user_ids)]
                # если pairs пуст — не исполняем SELECT с VALUES %s
                if pairs:
                    execute_values(cur, sql2, pairs, template="(%s,%s)")
                    cols2 = [d[0] for d in cur.description]
                    rows2 = cur.fetchall()
                else:
                    cols2, rows2 = ["userId", "username", "ord"], []

    # Разрешённые (после фильтра в SQL) и имена
    if rows2:
        j_user = cols2.index("userId")
        j_uname = cols2.index("username")
        uname_by_id = {str(r[j_user]): (r[j_uname] or str(r[j_user])) for r in rows2}
    else:
        uname_by_id = {}

    allowed_ids = set(uname_by_id.keys())
    print(f"[INFO] Allowed after createdAt filter: {len(allowed_ids)} users.")

    # 3) Чёрный список (из корня)
    blacklist_ids = read_blacklist_ids(BLACKLIST_FILE)
    if blacklist_ids:
        print(f"[INFO] Blacklist will exclude {len(blacklist_ids)} ids.")

    # 4) Оставляем только разрешённых и не в чёрном списке
    rows_filtered = []
    removed_black = 0
    for r in rows1:
        uid = str(r[i_user])
        if allowed_ids and uid not in allowed_ids:
            continue  # отфильтрован по createdAt/правилу 'line' в SQL
        if uid in blacklist_ids:
            removed_black += 1
            continue
        rows_filtered.append(r)
    if blacklist_ids:
        print(f"[INFO] Excluded by blacklist: {removed_black}")

    # 5) Сортировка и TOP_N
    rows_sorted = sorted(
        rows_filtered,
        key=lambda r: (to_num(r[i_score]) * -1,
                       to_num(r[i_purple]) * -1,
                       to_num(r[i_leg]) * -1,
                       str(r[i_user]))
    )
    top_rows = rows_sorted[:TOP_N]

    # 6) Итоговый CSV
    result_cols = ["username", "score", "purple", "legendaries", "userId"]
    result_rows = []
    for r in top_rows:
        uid = str(r[i_user])
        uname = uname_by_id.get(uid, uid)
        result_rows.append([uname, r[i_score], r[i_purple], r[i_leg], uid])
    save_csv(result_cols, result_rows, RESULT_CSV)

if __name__ == "__main__":
    main()
