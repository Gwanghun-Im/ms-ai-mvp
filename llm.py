"""
LLM 연동 및 검색 유틸리티

- Azure OpenAI를 이용한 NL→SQL 생성(`nl2sql`)
- Azure Translator를 이용한 한→영 번역(`translator`)
- Azure Cognitive Search를 이용한 스키마/예제 검색
"""

import json
import os
import uuid
import requests
from typing import Any, Dict, List
from dotenv import load_dotenv
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexerClient
from azure.search.documents.models import VectorizableTextQuery
from azure.core.credentials import AzureKeyCredential

from langchain_openai import AzureChatOpenAI
from langchain.retrievers import AzureCognitiveSearchRetriever
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from pydantic import BaseModel, Field


load_dotenv()

AZURE_LOCATION = os.getenv("AZURE_LOCATION")
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")
AZURE_OPENAI_EMBEDDING_DEPLOYMENT = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT")
AZURE_SEARCH_ENDPOINT = os.getenv("AZURE_SEARCH_ENDPOINT")
AZURE_SEARCH_API_KEY = os.getenv("AZURE_SEARCH_API_KEY")
AZURE_TRANSLATOR_ENDPOINT = os.getenv("AZURE_TRANSLATOR_ENDPOINT")
AZURE_TRANSLATOR_KEY = os.getenv("AZURE_TRANSLATOR_KEY")


class SQLResponse(BaseModel):
    """LLM이 반환해야 하는 JSON 스키마 정의"""

    sql: str = Field(description="생성된 SQL 쿼리")
    reasoning_short: str = Field(
        description="선택한 테이블/컬럼/조인의 이유를 1~2문장으로 요약"
    )


def translator(text: str) -> str:
    """한국어 문장을 영어로 번역합니다(Azure Translator).

    Args:
        text: 번역할 원문(한국어)

    Returns:
        번역된 영문 문자열
    """
    path = "/translate"
    constructed_url = AZURE_TRANSLATOR_ENDPOINT + path

    params = {"api-version": "3.0", "from": "ko", "to": "en"}

    headers = {
        "Ocp-Apim-Subscription-Key": AZURE_TRANSLATOR_KEY,
        # location required if you're using a multi-service or regional (not global) resource.
        "Ocp-Apim-Subscription-Region": AZURE_LOCATION,
        "Content-type": "application/json",
        "X-ClientTraceId": str(uuid.uuid4()),
    }

    # You can pass more than one object in body.
    body = [{"text": text}]

    request = requests.post(
        constructed_url, params=params, headers=headers, json=body, timeout=10
    )
    request.raise_for_status()
    response = request.json()

    return response[0]["translations"][0]["text"]


def nl2sql(
    question: str, schema_snippet: List[Dict[str, Any]], max_rows: int = 200
) -> Dict[str, Any]:
    """자연어 질문을 기반으로 PostgreSQL SELECT SQL을 생성합니다.

    Args:
        question: 사용자의 자연어 질문(영문 권장)
        schema_snippet: 관련 스키마 조각 리스트
        max_rows: LIMIT 기본값

    Returns:
        `{"sql": str, "reasoning_short": str}` 형태의 dict
    """

    client = AzureChatOpenAI(
        azure_deployment=AZURE_OPENAI_DEPLOYMENT,
        api_version=AZURE_OPENAI_API_VERSION,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        temperature=0.0,
        api_key=AZURE_OPENAI_API_KEY,
        model_kwargs={"response_format": {"type": "json_object"}},
    )

    # 유사한 쿼리 예제 가져오기 (검색 실패 시 무시)
    examples = ""
    try:
        similar_queries = search_similar_queries_simple(question, top_k=2)
        if similar_queries:
            examples = "\n\n참고할 수 있는 유사한 예제들:\n"
            for i, example in enumerate(similar_queries, 1):
                examples += f"""
예제 {i}:
질문: {example['question']}
SQL: {example['sql_query']}
"""
    except Exception as e:
        print(f"예제 검색 실패: {e}")
        examples = ""  # 예제 없이 진행

    # 프롬프트 템플릿 생성 (중괄호 이스케이프 포함)
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                f"""당신은 숙련된 데이터 분석가이자 SQL 전문가다.
오직 PostgreSQL 호환 SELECT만 작성한다.
DDL/DML/권한 변경/트랜잭션/임시테이블 금지.
항상 LIMIT를 포함하고, 기본은 {max_rows}.
아래 스키마/컬럼 정보 외의 테이블/컬럼은 사용하지 마라.
조인이 필요하면 외래키 관계를 우선 사용하고, 명확하지 않으면 가장 합리적인 키를 선택하라.
출력은 반드시 JSON 형식이어야 한다.
출력할 SQL문은 필요하다면 newline이나 tab을 사용해 가시성이 좋게 만들어야 한다.

{examples}

형식:
{{{{
"sql": "SELECT ... LIMIT {max_rows};",
"reasoning_short": "선택한 테이블/컬럼/조인의 이유를 1~2문장으로 요약"
}}}}""",
            ),
            ("user", "{input}"),
        ]
    )

    # JSON 출력 파서 설정
    parser = JsonOutputParser(pydantic_object=SQLResponse)

    # 체인 생성
    chain = prompt | client | parser

    # 입력 데이터 준비
    user_input = {"schema": schema_snippet, "question": question}

    # 체인 실행
    result = chain.invoke({"input": json.dumps(user_input, ensure_ascii=False)})
    return result


def search_similar_queries_simple(question: str, top_k: int = 3) -> List[Dict[str, str]]:
    """질문과 유사한 SQL 예제를 벡터 검색으로 조회합니다."""
    print("search start::::", question)
    search_client = SearchClient(
        endpoint=AZURE_SEARCH_ENDPOINT,
        index_name="rag-query",
        credential=AzureKeyCredential(AZURE_SEARCH_API_KEY),
    )

    try:
        # 텍스트+벡터 검색
        vector_query = VectorizableTextQuery(
            text=question, k_nearest_neighbors=top_k, fields="text_vector"
        )
        results = search_client.search(
            search_text=question,
            vector_queries=[vector_query],
            top=top_k,
        )
        similar_queries = []
        for result in results:
            similar_queries.append(
                {
                    "question": result["chunk"],
                    "sql_query": result["sql_query"],
                }
            )
        return similar_queries

    except Exception as e:
        print(f"쿼리 예제 검색 오류: {e}")
        return []


def rerun_existing_index() -> None:
    """기존 Azure Cognitive Search 인덱서를 재실행합니다."""
    indexer_client = SearchIndexerClient(
        endpoint=AZURE_SEARCH_ENDPOINT,
        credential=AzureKeyCredential(AZURE_SEARCH_API_KEY),
    )
    indexer_client.run_indexer("rag-table-indexer")


def get_schema_snippet(question: str, top_k: int = 3) -> list[dict | str]:
    """질문과 가장 연관성 높은 스키마 조각을 검색합니다.

    Returns:
        스키마 조각 리스트. 인덱스 설계에 따라 dict 또는 JSON 문자열일 수 있습니다.
    """
    # eng_question = translator(question) if is_korean(question) else question
    search_client = SearchClient(
        endpoint=AZURE_SEARCH_ENDPOINT,
        index_name="rag-table",
        credential=AzureKeyCredential(AZURE_SEARCH_API_KEY),
    )

    try:
        vector_query = VectorizableTextQuery(
            text=question, k_nearest_neighbors=10, fields="text_vector"
        )
        # 텍스트+벡터 통합 검색
        results = search_client.search(
            search_text=question,
            vector_queries=[vector_query],
            top=10,
            include_total_count=True,
        )

        # 결과는 인덱스 매핑에 따라 dict 혹은 문자열(JSON)일 수 있음
        out: list[dict | str] = []
        for r in results:
            chunk = r["chunk"]
            # 일부 인덱스는 문자열 JSON을 저장할 수 있으므로 우선 원형으로 반환
            # 상위 호출부에서 필요 시 json.loads로 파싱
            out.append(chunk)
        return out[:top_k]

    except Exception as e:
        print(f"검색 오류: {e}")
