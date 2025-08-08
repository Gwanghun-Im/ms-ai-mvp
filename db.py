"""
데이터베이스 접근 계층

- 스키마 메타데이터 조회(테이블/컬럼/PK/FK)
- 스키마 캐시(JSON) 생성 및 Blob 업로드
- 읽기 전용 SELECT 실행 헬퍼
"""

import os
from collections import defaultdict
from typing import Any, Iterable
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from utils import upload_json_to_blob, delete_all_blobs_in_container

# from llm import search_in_existing_index

load_dotenv()

PG_DSN = os.getenv("PG_DSN")
STATEMENT_TIMEOUT_MS = os.getenv("STATEMENT_TIMEOUT_MS", "30000")  # 기본 30초


def get_engine():
    """SQLAlchemy Engine 생성

    Returns:
        Engine: 연결 가능한 SQLAlchemy 엔진

    Raises:
        RuntimeError: 연결 문자열이 설정되지 않은 경우
    """
    dsn = PG_DSN
    if not dsn:
        raise RuntimeError("환경 변수 PG_DSN이 설정되지 않았습니다.")
    engine = create_engine(dsn, pool_pre_ping=True)
    return engine


def fetchall(engine, q: str, params: dict | None = None) -> list[dict[str, Any]]:
    """SELECT 결과를 dict 리스트로 반환

    Args:
        engine: SQLAlchemy Engine
        q: 실행할 SQL (SELECT)
        params: 바인딩 파라미터

    Returns:
        각 Row를 dict로 변환한 리스트
    """
    with engine.connect() as conn:
        res = conn.execute(text(q), params or {})
        return [dict(r._mapping) for r in res]


def get_tables(engine) -> list[dict[str, Any]]:
    """테이블 목록 조회"""
    q = """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type='BASE TABLE'
      AND table_schema NOT IN ('pg_catalog','information_schema')
    ORDER BY table_schema, table_name;
    """
    return fetchall(engine, q)


def get_columns(engine) -> list[dict[str, Any]]:
    """컬럼 메타데이터 조회"""
    q = """
    SELECT table_schema, table_name, column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_schema NOT IN ('pg_catalog','information_schema')
    ORDER BY table_schema, table_name, ordinal_position;
    """
    return fetchall(engine, q)


def get_pk(engine) -> list[dict[str, Any]]:
    """프라이머리 키 컬럼 조회"""
    q = """
    SELECT tc.table_schema, tc.table_name, kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      USING (constraint_name, table_schema, table_name)
    WHERE tc.constraint_type='PRIMARY KEY';
    """
    return fetchall(engine, q)


def get_fk(engine) -> list[dict[str, Any]]:
    """포린 키 관계 조회"""
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


def build_schema_cache() -> list[dict[str, Any]]:
    """DB에서 스키마 정보를 수집하여 JSON 캐시를 생성/업로드합니다.

    Returns:
        스키마 단위(dict)의 리스트. 각 항목은 `schema`, `table`, `columns`, `primary_key`, `foreign_keys`를 포함합니다.
    """

    engine = get_engine()

    tables = get_tables(engine)
    columns = get_columns(engine)
    pk = get_pk(engine)
    fk = get_fk(engine)

    # (schema.table) 단위로 메타데이터 통합
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

    delete_all_blobs_in_container()
    # list_scema = [schema_map[sm] for sm in schema_map]
    for sm in schema_map:
        # 각 테이블 정보를 개별 JSON으로 Blob 업로드 (컨테이너: data, 이름: <schema.table>.json)
        upload_json_to_blob(schema_map[sm], sm)
        # with open(f"json/{sm}.json", "w") as f:
        #     json.dump(schema_map[sm], f, indent=4)  # indent for pretty-printing

    # Convert to list
    return list(schema_map.values())


def run_readonly_select_simple(
    sql: str, max_rows: int = 200
) -> tuple[list[str], list[tuple]]:
    """읽기 전용 트랜잭션에서 SELECT를 실행합니다.

    Args:
        sql: 실행할 SELECT 문
        max_rows: 결과 행 최대 개수(LIMIT 보호)

    Returns:
        (컬럼명 리스트, 튜플 행 리스트)
    """
    engine = get_engine()
    timeout_ms = int(STATEMENT_TIMEOUT_MS)

    with engine.connect() as conn:
        # 세션 설정(타임아웃 및 읽기 전용)
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
