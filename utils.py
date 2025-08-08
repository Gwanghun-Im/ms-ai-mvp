"""
유틸리티 함수 모음

- 한글 포함 여부 검사
- Azure Blob Storage 업로드 헬퍼

환경 변수
- `AZURE_STORAGE_CONNECTED_STRING` 또는 `AZURE_STORAGE_CONNECTION_STRING`
"""

import re
import os
import json
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()

# 구(Connected) 키와 표준(Connection) 키를 모두 지원
AZURE_STORAGE_CONNECTION_STRING = os.getenv(
    "AZURE_STORAGE_CONNECTED_STRING"
) or os.getenv("AZURE_STORAGE_CONNECTION_STRING")


def is_korean(text: str) -> bool:
    """문자열에 한글이 포함되어 있는지 여부를 반환합니다.

    Args:
        text: 검사할 문자열

    Returns:
        한글 문자가 1개 이상 포함되어 있으면 True, 아니면 False
    """
    return bool(re.search(r"[ㄱ-ㅎ가-힣]", text or ""))


def delete_all_blobs_in_container(container_name: str = "data") -> int:
    """지정한 컨테이너의 모든 Blob 파일을 삭제합니다.

    Args:
        container_name: 대상 컨테이너 이름(기본값: "data")

    Returns:
        삭제된 Blob 개수

    Raises:
        RuntimeError: 연결 문자열이 설정되지 않은 경우
    """

    if not AZURE_STORAGE_CONNECTION_STRING:
        raise RuntimeError(
            "Azure Storage 연결 문자열이 설정되지 않았습니다. 환경 변수를 확인하세요: "
            "AZURE_STORAGE_CONNECTED_STRING 또는 AZURE_STORAGE_CONNECTION_STRING"
        )

    service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    container_client = service.get_container_client(container_name)

    if not container_client.exists():
        return 0

    count = 0
    # 모든 Blob 순회 삭제
    for blob in container_client.list_blobs():
        container_client.delete_blob(blob.name)
        count += 1
    return count


def upload_json_to_blob(
    data: dict | list,
    title: str,
    *,
    container_name: str = "data",
    purge_before_upload: bool = False,
) -> None:
    """Azure Blob Storage에 JSON 파일을 업로드합니다.

    Args:
        data: 업로드할 파이썬 객체(dict/list)
        title: Blob 파일명(확장자 제외)
        container_name: 업로드 대상 컨테이너 이름(기본: "data")
        purge_before_upload: 업로드 전에 컨테이너의 모든 파일을 삭제할지 여부

    Raises:
        RuntimeError: 연결 문자열이 설정되지 않은 경우
    """

    if not AZURE_STORAGE_CONNECTION_STRING:
        raise RuntimeError(
            "Azure Storage 연결 문자열이 설정되지 않았습니다. 환경 변수를 확인하세요: "
            "AZURE_STORAGE_CONNECTED_STRING 또는 AZURE_STORAGE_CONNECTION_STRING"
        )

    # BlobServiceClient 생성
    blob_service_client = BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )

    # 업로드할 컨테이너와 파일 이름 설정
    container_name = "data"
    blob_name = f"{title}.json"

    # Python 객체를 JSON 문자열로 변환(한글 가독성을 위해 들여쓰기 적용)
    json_data = json.dumps(data, indent=4, ensure_ascii=False)

    # Blob 클라이언트 생성
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )

    # JSON 데이터를 Blob에 업로드(존재하면 덮어쓰기)
    blob_client.upload_blob(json_data, blob_type="BlockBlob", overwrite=True)
