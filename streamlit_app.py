"""
Streamlit 애플리케이션 (NL → SQL 챗봇)

- 사이드바 설정: LIMIT, 스키마 업데이트
- 채팅 UI: 메시지 히스토리, 생성된 SQL/이유, 결과 표시 및 CSV 다운로드
- 안전장치: SQL 파싱으로 SELECT만 허용, 읽기 전용 트랜잭션/타임아웃 적용
"""

import os
import time
import json
import sqlglot
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from db import run_readonly_select_simple, build_schema_cache
from llm import nl2sql, get_schema_snippet, rerun_existing_index, translator
from utils import is_korean

st.set_page_config(page_title="NL→SQL Chatbot (PostgreSQL)", layout="wide")

# =====================
# 글로벌 스타일(CSS)
# =====================
st.markdown(
    """
    <style>
    /* 페이지 상단 히어로 영역 */
    .hero {
        padding: 18px 22px; border-radius: 14px; margin-bottom: 8px;
        background: linear-gradient(135deg, #0ea5e9 0%, #22c55e 100%);
        color: white;
        box-shadow: 0 8px 20px rgba(0,0,0,0.08);
    }
    .hero h1 {font-size: 1.6rem; margin: 0 0 6px 0;}
    .hero p {opacity: 0.95; margin: 0;}

    /* 샘플 프롬프트 칩 버튼 */
    .chip-row {display: flex; gap: 8px; flex-wrap: wrap; margin: 8px 0 2px 0;}
    .chip button[kind="secondary"] {border-radius: 999px !important; padding: 4px 12px !important;}

    /* 코드블록 및 탭 */
    pre code {font-size: 12px !important;}
    .stTabs [data-baseweb="tab-list"] {gap: 4px;}
    .stTabs [data-baseweb="tab"] {border-radius: 8px; padding: 6px 10px;}

    /* 다운로드 버튼 */
    .stDownloadButton button {border-radius: 8px;}

    /* 메시지 카드 스타일(라이트/다크 모두 잘 보이도록 반투명 배경 적용) */
    :root {
        --bubble-bg: rgba(255, 255, 255, 0.75);
        --bubble-border: rgba(0, 0, 0, 0.06);
    }
    @media (prefers-color-scheme: dark) {
        :root {
            --bubble-bg: rgba(255, 255, 255, 0.08);
            --bubble-border: rgba(255, 255, 255, 0.12);
        }
    }
    [data-testid="stChatMessage"]{
        background: var(--bubble-bg);
        border: 1px solid var(--bubble-border);
        border-radius: 12px;
        padding: 10px 12px;
        margin-bottom: 12px;
        box-shadow: 0 6px 18px rgba(0,0,0,0.05);
    }
    /* 탭 내부 여백 개선 */
    [data-testid="stChatMessage"] .stTabs {
        margin-top: 6px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# 히어로 섹션
with st.container():
    st.markdown(
        """
        <div class="hero">
            <h1>🧠 NL → SQL (PostgreSQL)</h1>
            <p>자연어로 질문하면 안전한 SELECT 쿼리를 생성하고 결과를 시각적으로 확인합니다.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

load_dotenv()

# 기본 LIMIT 값(환경변수 없으면 200)
DEFAULT_MAX_ROWS = os.getenv("DEFAULT_MAX_ROWS", "200")

with st.sidebar:
    st.header("⚙️ Settings")
    default_max_rows = int(DEFAULT_MAX_ROWS)
    max_rows = st.number_input("LIMIT(최대 행 수)", 10, 5000, default_max_rows, 10)
    schema_top_k = st.slider("스키마 Top-K", min_value=1, max_value=10, value=5, step=1)
    st.caption("DB는 읽기 전용 트랜잭션으로 실행되며, statement_timeout이 적용됩니다.")

    c1, c2 = st.columns(2)
    with c1:
        update_schema = st.button("🔄 Update Schema", use_container_width=True)
    with c2:
        if st.button("🧹 Clear Chat", use_container_width=True):
            st.session_state.messages = [
                {
                    "role": "assistant",
                    "content": "무엇을 조회할지 자연어로 말씀해 주세요. 예) '직원정보를 조회해줘'",
                }
            ]
            st.rerun()

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
    sql_query: str,
    max_rows: int,
    message_index: int | None = None,
    show_ui: bool = True,
):
    """SQL 쿼리를 실행하고 결과를 처리하는 공통 함수

    Returns:
        (결과 dict 리스트, 반환 행 수, 오류 메시지 또는 None)
    """
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


# 스키마 조각에서 schema.table 이름을 안전하게 추출
def _extract_schema_table_names(schema_snippet: list | None) -> list[str]:
    names: set[str] = set()
    if not schema_snippet:
        return []
    for item in schema_snippet:
        obj = None
        if isinstance(item, str):
            try:
                obj = json.loads(item)
            except Exception:
                obj = None
        elif isinstance(item, dict):
            obj = item

        if isinstance(obj, dict):
            schema = obj.get("schema") or obj.get("table_schema") or ""
            table = obj.get("table") or obj.get("table_name") or ""
            if schema or table:
                names.add(f"{schema}.{table}".strip("."))
    return sorted(names)


# 메시지 히스토리 표시 (아바타/탭/메트릭)
avatars = {"assistant": "🤖", "user": "🧑"}
for i, m in enumerate(st.session_state.messages):
    role = m["role"]
    with st.chat_message(role, avatar=avatars.get(role, "💬")):
        st.markdown(m["content"])

        has_sql = bool(m.get("sql"))
        has_result = isinstance(m.get("result"), list) and m.get("result")
        has_reason = bool(m.get("reasoning"))

        if has_sql or has_result or has_reason:
            tabs = st.tabs(["결과", "SQL", "설명"])

            # 결과 탭
            with tabs[0]:
                if has_result:
                    df = pd.DataFrame(m["result"])
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    csv_key = f"history_download_{i}_{int(time.time())}"
                    csv_data = df.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        "📥 CSV 다운로드",
                        csv_data,
                        "result.csv",
                        "text/csv",
                        key=csv_key,
                    )
                else:
                    st.info("결과가 없습니다.")

                # 메트릭(있다면)
                mc1, mc2, mc3 = st.columns(3)
                if m.get("row_count") is not None:
                    mc1.metric("반환 행 수", f"{m.get('row_count'):,}")
                if m.get("exec_ms") is not None:
                    mc2.metric("실행 시간", f"{m.get('exec_ms')} ms")
                if m.get("limit") is not None:
                    mc3.metric("LIMIT", f"{m.get('limit')}")

            # SQL 탭
            with tabs[1]:
                if has_sql:
                    st.code(m["sql"], language="sql")
                else:
                    st.info("생성된 SQL이 없습니다.")

            # 설명 탭
            with tabs[2]:
                if has_reason:
                    st.write(m["reasoning"])
                if m.get("error"):
                    st.error(m["error"])
                if m.get("schema_tables"):
                    st.caption("스키마 참조:")
                    st.write(", ".join(m["schema_tables"]))
        else:
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


# 샘플 프롬프트 칩
sample_prompts = [
    "카테고리 목록 조회",
    "가장 많이 대여한 DVD",
    "카테고리별 대여한 DVD",
    "현재 반납되지 않은 DVD",
    "액션 영화를 조회해줘",
]

st.markdown("추천 질문")
chip_cols = st.columns(len(sample_prompts))
selected_prompt = None
for idx, col in enumerate(chip_cols):
    with col:
        if st.button(sample_prompts[idx], key=f"chip_{idx}"):
            selected_prompt = sample_prompts[idx]

# 사용자 입력 처리 (채팅 입력 + 샘플 칩)
user_prompt = st.chat_input("질문을 입력하세요")
prompt = user_prompt or selected_prompt

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
            SCHEMA_SNIPPET = get_schema_snippet(question, top_k=schema_top_k)
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
                    _start = time.time()
                    result_dict, row_count, error_msg = execute_sql_query(
                        sql, max_rows, show_ui=False
                    )
                    exec_ms = int((time.time() - _start) * 1000)

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
                            "content": "✅ 쿼리를 성공적으로 실행했습니다.",
                            "sql": sql,
                            "reasoning": reasoning,
                            "result": result_dict if result_dict else [],
                            "row_count": row_count,
                            "exec_ms": exec_ms,
                            "limit": max_rows,
                            "schema_tables": _extract_schema_table_names(
                                SCHEMA_SNIPPET
                            ),
                        }
                    )

                # 페이지 새로고침하여 새로운 메시지 표시
                st.rerun()
