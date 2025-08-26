import os
import io
import re
import json
import base64
from urllib.parse import urljoin
from datetime import datetime

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

# =========================
# 설정값 (ENV 폴백 포함)
# =========================
CSV_FILE_NAME = 'jobkorea_FirstVVIP.csv'
BASE_URL = "https://www.jobkorea.co.kr"

GOOGLE_DRIVE_FOLDER_ID = (
    os.environ.get('GDRIVE_FOLDER_ID')
    or os.environ.get('GOOGLE_DRIVE_FOLDER_ID')   # 대체 키 허용
    or os.environ.get('INPUT_GDRIVE_FOLDER_ID')   # workflow_dispatch inputs 호환
)

GDRIVE_CREDENTIALS_DATA = (
    os.environ.get('GDRIVE_CREDENTIALS_DATA')
    or os.environ.get('INPUT_GDRIVE_CREDENTIALS_DATA')   # inputs 호환
)

GDRIVE_CREDENTIALS_PATH = os.environ.get('GDRIVE_CREDENTIALS_PATH')
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')  # 표준 변수도 폴백


# =========================
# 유틸
# =========================
def _norm(t: str) -> str:
    """HTML 엔티티 해제 + 공백 정리"""
    import html as ihtml
    t = ihtml.unescape(t or "")
    return re.sub(r"\s+", " ", t.strip())


def _to_abs(url: str) -> str:
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    return urljoin(BASE_URL, url)


def _build_session() -> requests.Session:
    """재시도/헤더 설정된 세션"""
    s = requests.Session()
    retries = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })
    return s


# =========================
# 자격증명 로더
# =========================
def _load_service_account_info():
    """
    우선순위
    1) GDRIVE_CREDENTIALS_DATA (JSON → 실패 시 base64 → 실패 시 literal_eval)
    2) 파일 경로(GDRIVE_CREDENTIALS_PATH, GOOGLE_APPLICATION_CREDENTIALS, ./credentials.json)
    """
    # 1) ENV
    if GDRIVE_CREDENTIALS_DATA:
        # JSON 시도
        try:
            return json.loads(GDRIVE_CREDENTIALS_DATA)
        except json.JSONDecodeError:
            pass
        # base64 → JSON 시도
        try:
            decoded = base64.b64decode(GDRIVE_CREDENTIALS_DATA).decode('utf-8')
            return json.loads(decoded)
        except Exception:
            pass
        # literal_eval 마지막 시도
        try:
            import ast
            return ast.literal_eval(GDRIVE_CREDENTIALS_DATA)
        except Exception as e:
            raise RuntimeError("GDRIVE_CREDENTIALS_DATA 파싱 실패(JSON 또는 base64(JSON) 필요).") from e

    # 2) 파일 경로 폴백
    candidate_paths = []
    if GDRIVE_CREDENTIALS_PATH:
        candidate_paths.append(GDRIVE_CREDENTIALS_PATH)
    if GOOGLE_APPLICATION_CREDENTIALS:
        candidate_paths.append(GOOGLE_APPLICATION_CREDENTIALS)
    candidate_paths.append('credentials.json')

    for p in candidate_paths:
        if p and os.path.exists(p):
            size = os.path.getsize(p)
            if size == 0:
                raise FileNotFoundError(f"자격증명 파일이 비어있습니다: {p}")
            with open(p, 'r', encoding='utf-8') as f:
                s = f.read().strip()
            # 파일 내용이 base64일 수도 있음
            try:
                if s and not s.lstrip().startswith('{'):
                    decoded = base64.b64decode(s).decode('utf-8')
                    return json.loads(decoded)
                return json.loads(s)
            except Exception as e:
                raise RuntimeError(
                    f"자격증명 파일이 올바른 JSON이 아닙니다: {p} "
                    f"(base64를 파일에 넣었으면 디코드하거나 ENV로 넘겨주세요)"
                ) from e

    raise FileNotFoundError(
        "서비스 계정 자격증명을 찾을 수 없습니다. "
        "GDRIVE_CREDENTIALS_DATA(ENV, JSON/BASE64) 또는 "
        "GDRIVE_CREDENTIALS_PATH/GOOGLE_APPLICATION_CREDENTIALS/credentials.json을 제공하세요."
    )


# =========================
# Google Drive 서비스
# =========================
def get_gdrive_service():
    """Google Drive API 서비스 객체 생성"""
    if not GOOGLE_DRIVE_FOLDER_ID:
        raise ValueError("GDRIVE_FOLDER_ID 환경변수가 설정되어 있지 않습니다. 업로드 대상 폴더 ID를 지정하세요.")

    scopes = ['https://www.googleapis.com/auth/drive']
    info = _load_service_account_info()
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    drive = build('drive', 'v3', credentials=creds)
    return drive


def _assert_folder_access(drive, folder_id):
    """폴더가 보이는지/폴더가 맞는지/어느 드라이브인지 확인하고 driveId 반환"""
    try:
        meta = drive.files().get(
            fileId=folder_id,
            fields="id, name, mimeType, driveId",
            supportsAllDrives=True
        ).execute()
    except HttpError as e:
        if e.resp.status == 404:
            raise ValueError(
                "지정한 GDRIVE_FOLDER_ID 폴더를 찾을 수 없습니다. "
                "➜ 폴더 ID가 맞는지 확인하고, 서비스계정 이메일을 해당 '공유 드라이브'의 구성원(콘텐츠 관리자 이상)으로 추가하세요."
            )
        raise

    if meta.get("mimeType") != "application/vnd.google-apps.folder":
        raise ValueError(f"GDRIVE_FOLDER_ID가 폴더가 아닙니다: {meta.get('mimeType')}")
    print(f"업로드 대상 폴더 확인: {meta.get('name')} (driveId={meta.get('driveId')})")
    return meta.get("driveId")


# =========================
# HTML 파싱 (Requests + BeautifulSoup)
# =========================
def _parse_first_vvip_html(html: str) -> pd.DataFrame:
    """First VVIP 섹션 HTML 문자열을 받아 DataFrame으로 변환"""
    soup = BeautifulSoup(html, "lxml")
    sec = soup.select_one("#Prdt_BnnrFirstVVIP")
    if not sec:
        return pd.DataFrame()

    items = sec.select("ul.list_firstvvip li")
    data = []

    for li in items:
        a = li.select_one("a.card-wrap")
        job_url = _to_abs(a.get("href")) if a else "No URL"

        desc_el = li.select_one("div.description")
        job_title = _norm(desc_el.get_text(" ")) if desc_el else "No Title"

        sum_el = li.select_one("div.addition div.summary")
        job_summary = _norm(sum_el.get_text(" ")) if sum_el else "No Summary"

        dday_el = li.select_one("div.extra .dday")
        dday = _norm(dday_el.get_text(" ")) if dday_el else "No D-Day"

        logo_img = li.select_one("span.logo img")
        logo_url = _to_abs(logo_img.get("src") or logo_img.get("data-src") or "") if logo_img else "No Logo URL"

        # 회사명: 스크랩 버튼 onclick → 실패시 로고 alt
        company_name = ""
        scrap_btn = li.select_one(".btnScrap")
        if scrap_btn and scrap_btn.has_attr("onclick"):
            onclick = scrap_btn["onclick"]
            m = re.search(r"'_(.+?)_'", onclick)
            if m:
                company_name = _norm(m.group(1))
        if not company_name and logo_img:
            company_name = _norm(logo_img.get("alt"))
        if not company_name:
            company_name = "No Company Name"

        data.append({
            "Company Name": company_name,
            "Logo URL": logo_url,
            "Job Title": job_title,
            "Job Summary": job_summary,
            "D-Day": dday,
            "Job URL": job_url,
            "Scraped Date": datetime.now().strftime("%Y-%m-%d")
        })

    return pd.DataFrame(data)


# =========================
# 크롤러 (Requests만 사용)
# =========================
def scrape_job_postings() -> pd.DataFrame:
    """잡코리아 'First VVIP' 섹션 크롤링"""
    session = _build_session()
    resp = session.get(BASE_URL + "/", timeout=30)
    resp.raise_for_status()

    df = _parse_first_vvip_html(resp.text)

    if df.empty:
        # 디버깅 편의를 위해 일부 저장(선택)
        with open("jobkorea_home_debug.html", "w", encoding="utf-8") as f:
            f.write(resp.text)
        print("경고: First VVIP 섹션을 찾지 못했습니다. 'jobkorea_home_debug.html'로 원본 저장.")
    else:
        print(f"총 {len(df)}개의 채용 공고를 수집했습니다.")

    return df


# =========================
# 메인
# =========================
def main():
    drive = get_gdrive_service()

    # ✅ 폴더 접근 가능 여부 확인 및 driveId 확보(공유 드라이브 대응)
    drive_id = _assert_folder_access(drive, GOOGLE_DRIVE_FOLDER_ID)

    # 1) 기존 CSV 검색 (Shared Drive 옵션 추가)
    query = f"name='{CSV_FILE_NAME}' and '{GOOGLE_DRIVE_FOLDER_ID}' in parents and trashed=false"
    resp = drive.files().list(
        q=query,
        spaces='drive',
        fields='files(id, name)',
        includeItemsFromAllDrives=True,   # ✅ 공유 드라이브 검색 포함
        supportsAllDrives=True,           # ✅ 공유 드라이브 지원
        corpora='drive',                  # ✅ 현재 드라이브 한정
        driveId=drive_id                  # ✅ 대상 드라이브 지정
    ).execute()
    files = resp.get('files', [])

    existing_df = pd.DataFrame()
    file_id = None

    if files:
        file_id = files[0]['id']
        print(f"기존 파일 '{CSV_FILE_NAME}' (ID: {file_id}) 발견 → 다운로드")
        request = drive.files().get_media(fileId=file_id)  # get_media에는 supportsAllDrives 파라미터가 없음
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        existing_df = pd.read_csv(fh)
        print(f"기존 데이터 {len(existing_df)}개 로드 완료.")
    else:
        print(f"기존 파일 '{CSV_FILE_NAME}' 없음 → 신규 생성 예정.")

    # 2) 신규 크롤링
    new_df = scrape_job_postings()

    # 3) 병합 & 중복 제거
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    combined_df.drop_duplicates(subset=['Job URL'], keep='last', inplace=True)
    print(f"병합/중복 제거 후 총 {len(combined_df)}개 레코드.")

    # 4) 업로드(업데이트/신규)
    csv_bytes = combined_df.to_csv(index=False).encode('utf-8-sig')  # Excel 호환
    media_body = MediaIoBaseUpload(io.BytesIO(csv_bytes), mimetype='text/csv', resumable=False)

    if file_id:
        drive.files().update(
            fileId=file_id,
            media_body=media_body,
            supportsAllDrives=True        # ✅ 공유 드라이브 지원
        ).execute()
        print(f"파일 ID {file_id} 업데이트 완료.")
    else:
        file_metadata = {'name': CSV_FILE_NAME, 'parents': [GOOGLE_DRIVE_FOLDER_ID]}
        created = drive.files().create(
            body=file_metadata,
            media_body=media_body,
            fields='id',
            supportsAllDrives=True        # ✅ 공유 드라이브 지원
        ).execute()
        print(f"폴더 {GOOGLE_DRIVE_FOLDER_ID}에 새 파일 업로드 완료 (ID: {created.get('id')}).")


if __name__ == '__main__':
    main()