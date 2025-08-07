"""
db.py
"""

import os
from collections import defaultdict
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from utils import upload_json_to_blob

# from llm import search_in_existing_index

load_dotenv()

PG_DSN = os.getenv("PG_DSN")
STATEMENT_TIMEOUT_MS = os.getenv("STATEMENT_TIMEOUT_MS")


def get_engine():
    """get database engine"""
    dsn = PG_DSN
    engine = create_engine(dsn, pool_pre_ping=True)
    return engine


def fetchall(engine, q: str, params: dict | None = None) -> list[dict]:
    """query fetching"""
    with engine.connect() as conn:
        res = conn.execute(text(q), params or {})
        return [dict(r._mapping) for r in res]


def get_tables(engine) -> list[dict]:
    """get tables"""
    q = """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type='BASE TABLE'
      AND table_schema NOT IN ('pg_catalog','information_schema')
    ORDER BY table_schema, table_name;
    """
    return fetchall(engine, q)


def get_columns(engine) -> list[dict]:
    """get columns"""
    q = """
    SELECT table_schema, table_name, column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_schema NOT IN ('pg_catalog','information_schema')
    ORDER BY table_schema, table_name, ordinal_position;
    """
    return fetchall(engine, q)


def get_pk(engine) -> list[dict]:
    """get priority key"""
    q = """
    SELECT tc.table_schema, tc.table_name, kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      USING (constraint_name, table_schema, table_name)
    WHERE tc.constraint_type='PRIMARY KEY';
    """
    return fetchall(engine, q)


def get_fk(engine) -> list[dict]:
    """get foreign key"""
    q = """
    SELECT tc.table_schema, tc.table_name, kcu.column_name,
           ccu.table_schema AS foreign_schema, ccu.table_name AS foreign_table, ccu.column_name AS foreign_column
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      USING (constraint_name, table_schema, table_name)
    JOIN information_schema.constraint_column_usage ccu
      USING (constraint_name)
    WHERE tc.constraint_type='FOREIGN KEY';
    """
    return fetchall(engine, q)


def build_schema_cache():
    """Build a cache of schema information from the database.
    Returns a list of dicts with schema, table, columns, primary_key, foreign_keys.
    """

    engine = get_engine()

    tables = get_tables(engine)
    columns = get_columns(engine)
    pk = get_pk(engine)
    fk = get_fk(engine)

    # Assemble by (schema, table)
    schema_map = defaultdict(
        lambda: {
            "schema": "",
            "table": "",
            "columns": [],
            "primary_key": [],
            "foreign_keys": [],
        }
    )

    for t in tables:
        key = f"{t['table_schema']}.{t['table_name']}"
        schema_map[key]["schema"] = t["table_schema"]
        schema_map[key]["table"] = t["table_name"]

    for c in columns:
        key = f"{c['table_schema']}.{c['table_name']}"
        schema_map[key]["columns"].append(
            {
                "name": c["column_name"],
                "type": c["data_type"],
                "nullable": c["is_nullable"] == "YES",
                "default": c["column_default"],
            }
        )

    for r in pk:
        key = f"{r['table_schema']}.{r['table_name']}"
        schema_map[key]["primary_key"].append(r["column_name"])

    for r in fk:
        key = f"{r['table_schema']}.{r['table_name']}"
        schema_map[key]["foreign_keys"].append(
            {
                "from": r["column_name"],
                "to": f"{r['foreign_schema']}.{r['foreign_table']}.{r['foreign_column']}",
            }
        )
    # list_scema = [schema_map[sm] for sm in schema_map]
    for sm in schema_map:
        upload_json_to_blob(schema_map[sm], sm)
        # with open(f"json/{sm}.json", "w") as f:
        #     json.dump(schema_map[sm], f, indent=4)  # indent for pretty-printing

    # Convert to list
    return list(schema_map.values())


def run_readonly_select_simple(
    sql: str, max_rows: int = 200
) -> tuple[list[str], list[tuple]]:
    """가장 간단하고 안전한 방법"""
    engine = get_engine()
    timeout_ms = int(STATEMENT_TIMEOUT_MS)

    with engine.connect() as conn:
        # 세션 설정을 한 번에
        setup_sql = f"""
        SET statement_timeout = {timeout_ms};
        SET transaction_read_only = true;
        """
        conn.execute(text(setup_sql))

        # 쿼리 실행
        result = conn.execute(text(sql))
        rows = result.fetchall()

        # 행 수 제한
        rows = rows[:max_rows]
        cols = list(result.keys())

        # print("cols", cols)
        # print("rows", len(rows), "rows")

        # SQLAlchemy Row 객체를 tuple로 변환
        rows_as_tuples = [tuple(row) for row in rows]

        return cols, rows_as_tuples
