import os
import time
import sqlglot
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from db import run_readonly_select_simple, build_schema_cache
from llm import nl2sql, get_schema_snippet, rerun_existing_index, translator
from utils import is_korean

st.set_page_config(page_title="NL→SQL Chatbot (PostgreSQL)", layout="wide")
st.title("NL2SQL (PostgreSQL)")

load_dotenv()

DEFAULT_MAX_ROWS = os.getenv("DEFAULT_MAX_ROWS")

with st.sidebar:
    st.header("Settings")
    # top_k = st.number_input("스키마 Top-K 테이블", 1, 10, 3, 1)
    default_max_rows = int(DEFAULT_MAX_ROWS)
    max_rows = st.number_input("LIMIT(최대 행 수)", 10, 5000, default_max_rows, 10)
    st.caption("DB는 읽기 전용 트랜잭션으로 실행되며, statement_timeout이 적용됩니다.")
    update_schema = st.button("▶️ Update Schema")
    # st.caption("Schema정보는 1시간 단위로 임베딩 합니다.")

# 세션 상태 초기화
if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "assistant",
            "content": "무엇을 조회할지 자연어로 말씀해 주세요. 예) '직원정보를 조회해줘'",
        }
    ]


# SQL 실행 함수
def execute_sql_query(
    sql_query: str, max_rows: int, message_index: int = None, show_ui: bool = True
):
    """SQL 쿼리를 실행하고 결과를 처리하는 공통 함수"""
    try:
        cols, rows = run_readonly_select_simple(sql_query, max_rows=max_rows)
        result_dict = []

        if not rows:
            if show_ui:
                st.warning("결과가 없습니다.")
        else:
            df = pd.DataFrame(rows, columns=cols)
            if show_ui:
                st.success(f"행 {len(df)}개를 반환했습니다.")
                st.dataframe(df, use_container_width=True, hide_index=True)

                # CSV 다운로드 버튼
                csv_key = (
                    f"download_{message_index}_{int(time.time())}"
                    if message_index is not None
                    else f"download_{int(time.time())}"
                )
                csv_data = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "📥 CSV 다운로드", csv_data, "result.csv", "text/csv", key=csv_key
                )
            result_dict = df.to_dict("records")  # 전체 결과 반환

        return result_dict, len(rows), None

    except Exception as e:
        if show_ui:
            st.error(f"실행 오류: {e}")
        return None, 0, str(e)


# 메시지 히스토리 표시
for i, m in enumerate(st.session_state.messages):
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

        # SQL이 있는 경우 표시
        if m.get("sql"):
            st.markdown("**생성된 SQL**")
            st.code(m["sql"], language="sql")

        # reasoning이 있는 경우 표시
        if m.get("reasoning"):
            st.caption(m["reasoning"])

        # 결과가 있는 경우 표시
        if m.get("result"):
            st.markdown("**쿼리 결과**")
            result_data = m["result"]
            if isinstance(result_data, list) and result_data:
                df = pd.DataFrame(result_data)
                st.dataframe(df, use_container_width=True, hide_index=True)

                # CSV 다운로드 버튼 (히스토리용)
                csv_key = f"history_download_{i}_{int(time.time())}"
                csv_data = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "📥 CSV 다운로드", csv_data, "result.csv", "text/csv", key=csv_key
                )
            else:
                st.info("결과가 없습니다.")

        # 에러가 있는 경우 표시
        if m.get("error"):
            st.error(m["error"])


# update_schema 버튼 처리 부분
if update_schema:
    try:
        start_time = time.time()
        with st.spinner("스키마를 업데이트하는 중... 잠시만 기다려 주세요."):
            build_schema_cache()
        with st.spinner("인덱스 rerun하는 중... 잠시만 기다려 주세요."):
            rerun_existing_index()

        end_time = time.time()
        duration = round(end_time - start_time, 2)
        st.success(
            f"✅ 스키마가 성공적으로 업데이트되었습니다! (소요시간: {duration}초)"
        )

    except Exception as e:
        st.error(f"❌ 스키마 업데이트 실패: {e}")
        st.warning("네트워크 연결이나 데이터베이스 상태를 확인하고 다시 시도해 주세요.")

        # 디버깅을 위해 상세 오류 정보 표시 (개발 환경에서만 사용)
        with st.expander("상세 오류 정보 (개발용)"):
            st.code(str(e))


# 사용자 입력 처리
prompt = st.chat_input("질문을 입력하세요")

if prompt:
    # 사용자 메시지 추가
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    question = translator(prompt) if is_korean(prompt) else prompt
    # 스키마 확인
    SCHEMA_SNIPPET = None
    with st.spinner("스키마 확인 중..."):
        try:
            SCHEMA_SNIPPET = get_schema_snippet(question, top_k=5)
        except Exception as e:
            st.session_state.messages.append(
                {"role": "assistant", "content": f"스키마 확인 실패: {e}"}
            )
            st.rerun()

    # 스키마 확인이 성공한 경우에만 계속 진행
    if SCHEMA_SNIPPET is not None:
        # LLM으로 SQL 생성
        sql = None
        reasoning = None
        with st.spinner("SQL 생성 중..."):
            try:
                llm_out = nl2sql(question, SCHEMA_SNIPPET, max_rows=max_rows)
                sql = llm_out.get("sql", "").strip()
                reasoning = llm_out.get("reasoning_short", "")
            except Exception as e:
                st.session_state.messages.append(
                    {"role": "assistant", "content": f"SQL 생성 실패: {e}"}
                )
                st.rerun()

        # SQL이 생성된 경우에만 계속 진행
        if sql is not None:
            # SQL 검증 및 보안 체크
            sql_valid = False
            try:
                if not sql:
                    raise ValueError("SQL이 생성되지 않았습니다.")

                parsed = sqlglot.parse_one(sql, read="postgres")

                # 쿼리의 최상위 명령어가 SELECT인지 확인
                if parsed.key.upper() != "SELECT":
                    raise ValueError("SELECT 쿼리만 허용됩니다.")

                sql_valid = True

            except Exception as e:
                st.session_state.messages.append(
                    {"role": "assistant", "content": f"SQL 검증 실패: {e}"}
                )
                st.rerun()

            # SQL이 유효한 경우에만 실행
            if sql_valid:
                # 쿼리 실행
                with st.spinner("쿼리 실행 중..."):
                    result_dict, row_count, error_msg = execute_sql_query(
                        sql, max_rows, show_ui=False
                    )

                if error_msg:
                    # 세션에 에러 저장
                    st.session_state.messages.append(
                        {
                            "role": "assistant",
                            "content": f"쿼리 실행에 실패했습니다: {error_msg}",
                            "sql": sql,
                            "reasoning": reasoning,
                            "error": error_msg,
                        }
                    )
                else:
                    # 세션에 성공 결과 저장
                    st.session_state.messages.append(
                        {
                            "role": "assistant",
                            "content": f"쿼리를 성공적으로 실행했습니다. 행 {row_count}개를 반환했습니다.",
                            "sql": sql,
                            "reasoning": reasoning,
                            "result": (
                                result_dict if result_dict else []
                            ),  # 처음 10개 행만 저장
                            "row_count": row_count,
                        }
                    )

                # 페이지 새로고침하여 새로운 메시지 표시
                st.rerun()
