"""
db.py
"""

import re
import os
import json
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()

AZURE_STORAGE_CONNECTED_STRING = os.getenv("AZURE_STORAGE_CONNECTED_STRING")


def is_korean(text: str) -> bool:
    """text에 한글의 포함여부를 검사"""
    return bool(re.search(r"[ㄱ-ㅎ가-힣]", text or ""))


def upload_json_to_blob(data, title):
    """Azuer blob storage에 json 파일 업로드"""

    # BlobServiceClient 생성
    blob_service_client = BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTED_STRING
    )

    # 업로드할 컨테이너와 파일 이름 설정
    container_name = "data"
    blob_name = f"{title}.json"

    # Python dict를 JSON으로 변환
    json_data = json.dumps(data, indent=4)

    # Blob 클라이언트 생성
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )

    # JSON 데이터를 Blob에 업로드
    blob_client.upload_blob(json_data, blob_type="BlockBlob", overwrite=True)
