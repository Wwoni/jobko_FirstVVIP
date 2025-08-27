# filename: jobko_FirstVVIP.py
import os
import io
import re
import json
import html
import base64
from urllib.parse import urljoin
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError

# =========================
# 설정값
# =========================
BASE_URL = "https://www.jobkorea.co.kr"
CSV_FILE_NAME = "jobkorea_FirstVVIP.csv"

GOOGLE_DRIVE_FOLDER_ID = (
    os.environ.get("GDRIVE_FOLDER_ID")
    or os.environ.get("GOOGLE_DRIVE_FOLDER_ID")
    or os.environ.get("INPUT_GDRIVE_FOLDER_ID")
)
GDRIVE_CREDENTIALS_DATA = (
    os.environ.get("GDRIVE_CREDENTIALS_DATA")
    or os.environ.get("INPUT_GDRIVE_CREDENTIALS_DATA")
)
GDRIVE_CREDENTIALS_PATH = os.environ.get("GDRIVE_CREDENTIALS_PATH")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

# =========================
# 유틸
# =========================
def _fix_url(u: str, base: str = BASE_URL) -> str:
    if not u:
        return u
    u = u.strip()
    if u.startswith("//"):
        return "https:" + u
    if u.startswith("/"):
        return urljoin(base, u)
    return u

def _text(node) -> str:
    return node.get_text(" ", strip=True) if node else ""

# =========================
# 자격증명 로더
# =========================
def _load_service_account_info():
    if GDRIVE_CREDENTIALS_DATA:
        try:
            return json.loads(GDRIVE_CREDENTIALS_DATA)
        except json.JSONDecodeError:
            pass
        try:
            decoded = base64.b64decode(GDRIVE_CREDENTIALS_DATA).decode("utf-8")
            return json.loads(decoded)
        except Exception:
            pass
        try:
            import ast
            return ast.literal_eval(GDRIVE_CREDENTIALS_DATA)
        except Exception as e:
            raise RuntimeError("GDRIVE_CREDENTIALS_DATA 파싱 실패(JSON 또는 base64(JSON) 필요).") from e

    candidate_paths = []
    if GDRIVE_CREDENTIALS_PATH:
        candidate_paths.append(GDRIVE_CREDENTIALS_PATH)
    if GOOGLE_APPLICATION_CREDENTIALS:
        candidate_paths.append(GOOGLE_APPLICATION_CREDENTIALS)
    candidate_paths.append("credentials.json")

    for p in candidate_paths:
        if p and os.path.exists(p):
            if os.path.getsize(p) == 0:
                raise FileNotFoundError(f"자격증명 파일이 비어있습니다: {p}")
            with open(p, "r", encoding="utf-8") as f:
                s = f.read().strip()
            try:
                if s and not s.lstrip().startswith("{"):
                    decoded = base64.b64decode(s).decode("utf-8")
                    return json.loads(decoded)
                return json.loads(s)
            except Exception as e:
                raise RuntimeError(
                    f"자격증명 파일이 올바른 JSON이 아닙니다: {p} "
                    f"(base64를 파일에 넣었으면 디코드하거나 ENV로 넘겨주세요)"
                ) from e

    raise FileNotFoundError(
        "서비스 계정 자격증명을 찾을 수 없습니다. "
        "GDRIVE_CREDENTIALS_DATA(ENV) 또는 "
        "GDRIVE_CREDENTIALS_PATH/GOOGLE_APPLICATION_CREDENTIALS/credentials.json을 제공하세요."
    )

# =========================
# Google Drive 서비스
# =========================
def get_gdrive_service():
    if not GOOGLE_DRIVE_FOLDER_ID:
        raise ValueError("GDRIVE_FOLDER_ID 환경변수가 비어 있습니다.")
    scopes = ["https://www.googleapis.com/auth/drive"]
    info = _load_service_account_info()
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return build("drive", "v3", credentials=creds)

def _assert_folder_access(drive, folder_id):
    try:
        meta = drive.files().get(
            fileId=folder_id, fields="id,name,mimeType,driveId", supportsAllDrives=True
        ).execute()
    except HttpError as e:
        if e.resp.status == 404:
            raise ValueError("GDRIVE_FOLDER_ID 폴더를 찾지 못했습니다. 권한/ID를 확인하세요.")
        raise
    if meta.get("mimeType") != "application/vnd.google-apps.folder":
        raise ValueError(f"GDRIVE_FOLDER_ID가 폴더가 아닙니다: {meta.get('mimeType')}")
    print(f"업로드 대상 폴더 확인: {meta.get('name')} (driveId={meta.get('driveId')})")
    return meta.get("driveId")

# =========================
# 크롤러 (requests + bs4)
# =========================
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

def _new_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retries = Retry(
        total=3,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s

def _extract_company_from_onclick(li) -> str | None:
    btn = li.select_one("button.btnScrap")
    if not btn:
        return None
    onclick = btn.get("onclick") or ""
    literals = re.findall(r"'([^']*)'", onclick)
    if not literals:
        return None
    payload = html.unescape(literals[-1])  # "_회사명_제목"
    payload = payload.replace("<BR>", " ").replace("&lt;BR&gt;", " ").lstrip("_").strip()
    if "_" in payload:
        company = payload.split("_", 1)[0].strip()
        return company or None
    return None

def _first_valid_href(anchors):
    for a in anchors:
        href = (a.get("href") or "").strip()
        if not href:
            continue
        low = href.lower()
        if low.startswith("javascript") or href in ("#", "/#"):
            continue
        if low.startswith("http") or href.startswith("/"):
            return href
    return ""

def _extract_job_url(li) -> str:
    """
    URL 추출 우선순위(확장판):
      1) a.card-wrap[href] (javascript 제외)
      2) a.card-wrap[data-linkurl]
      3) div.description a[href]
      4) div.company .name a[href]
      5) li 내부 첫 번째 유효한 a[href]
      6) a.card-wrap[data-gno] → /Recruit/GI_Read/{gno}
      7) li[data-match] JSON의 gno → /Recruit/GI_Read/{gno}
      8) li[dmp-collection] JSON의 recruitNo → /Recruit/GI_Read/{recruitNo}
    """
    a = li.select_one("a.card-wrap")
    if a:
        href = (a.get("href") or "").strip()
        if href and not href.lower().startswith("javascript"):
            return _fix_url(href)
        dlink = (a.get("data-linkurl") or "").strip()
        if dlink:
            return _fix_url(dlink)

    # 3~5: 다양한 레이아웃의 직접 링크
    for sel in ("div.description a", "div.company .name a"):
        cand = li.select_one(sel)
        if cand:
            href = _first_valid_href([cand])
            if href:
                return _fix_url(href)
    any_href = _first_valid_href(li.select("a[href]"))
    if any_href:
        return _fix_url(any_href)

    # 6~7: gno / data-match
    if a:
        gno = (a.get("data-gno") or "").strip()
        if gno:
            return _fix_url(f"/Recruit/GI_Read/{gno}")
    dm = li.get("data-match")
    if dm:
        try:
            gno = json.loads(html.unescape(dm)).get("gno")
            if gno:
                return _fix_url(f"/Recruit/GI_Read/{gno}")
        except Exception:
            pass

    # 8: dmp-collection의 recruitNo
    dmp = li.get("dmp-collection")
    if dmp:
        try:
            obj = json.loads(html.unescape(dmp))
            rec = obj.get("recruitNo")
            if rec:
                return _fix_url(f"/Recruit/GI_Read/{rec}")
        except Exception:
            pass

    return "No URL"

def _fetch_company_from_detail(session: requests.Session, job_url: str) -> str | None:
    if not job_url or job_url == "No URL":
        return None
    try:
        res = session.get(_fix_url(job_url), timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "lxml")
        cand = (
            soup.select_one(".coName a")
            or soup.select_one(".coTit a")
            or soup.select_one("a.coLink")
            or soup.select_one(".company .name")
            or soup.select_one('meta[property="og:site_name"]')
        )
        if cand:
            return (cand.get_text(strip=True)
                    if hasattr(cand, "get_text")
                    else cand.get("content", "").strip()) or None
    except Exception:
        pass
    return None

def scrape_job_postings() -> pd.DataFrame:
    session = _new_session()
    resp = session.get(BASE_URL + "/", timeout=20)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    section = soup.select_one("#Prdt_BnnrFirstVVIP")
    if not section:
        raise RuntimeError("First VVIP 섹션(#Prdt_BnnrFirstVVIP)을 찾지 못했습니다.")

    items = section.select("ul.list_firstvvip > li")
    if not items:
        print("경고: list_firstvvip 항목이 비었습니다.")

    rows = []
    for li in items:
        job_url = _extract_job_url(li)
        title = _text(li.select_one("div.description")) or "No Title"
        summary = _text(li.select_one("div.addition .summary")) or "No Summary"
        dday = _text(li.select_one("div.extra .dday")) or "No D-Day"
        logo_img = li.select_one("span.logo img")
        logo_url = _fix_url(logo_img.get("src")) if logo_img and logo_img.get("src") else "No Logo URL"

        company = _extract_company_from_onclick(li)
        if not company and job_url and job_url != "No URL":
            company = _fetch_company_from_detail(session, job_url)
        if not company:
            company = "No Company Name"

        rows.append(
            {
                "Company Name": company,
                "Logo URL": logo_url,
                "Job Title": title,
                "Job Summary": summary,
                "D-Day": dday,
                "Job URL": job_url,
                "Scraped Date": datetime.now().strftime("%Y-%m-%d"),
            }
        )

    print(f"총 {len(rows)}개의 채용 공고를 수집했습니다.")
    return pd.DataFrame(rows)

# =========================
# 메인
# =========================
def main():
    drive = get_gdrive_service()
    drive_id = _assert_folder_access(drive, GOOGLE_DRIVE_FOLDER_ID)

    query = f"name='{CSV_FILE_NAME}' and '{GOOGLE_DRIVE_FOLDER_ID}' in parents and trashed=false"
    resp = drive.files().list(
        q=query,
        spaces="drive",
        fields="files(id,name)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        corpora="drive",
        driveId=drive_id,
    ).execute()
    files = resp.get("files", [])

    existing_df, file_id = pd.DataFrame(), None
    if files:
        file_id = files[0]["id"]
        print(f"기존 파일 '{CSV_FILE_NAME}' (ID: {file_id}) 발견 → 다운로드")
        request = drive.files().get_media(fileId=file_id)
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

    new_df = scrape_job_postings()

    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    combined_df.drop_duplicates(subset=["Job URL", "Job Title"], keep="last", inplace=True)
    print(f"병합/중복 제거 후 총 {len(combined_df)}개 레코드.")

    csv_bytes = combined_df.to_csv(index=False).encode("utf-8-sig")
    media_body = MediaIoBaseUpload(io.BytesIO(csv_bytes), mimetype="text/csv", resumable=False)

    if file_id:
        drive.files().update(fileId=file_id, media_body=media_body, supportsAllDrives=True).execute()
        print(f"파일 ID {file_id} 업데이트 완료.")
    else:
        file_metadata = {"name": CSV_FILE_NAME, "parents": [GOOGLE_DRIVE_FOLDER_ID]}
        created = drive.files().create(
            body=file_metadata, media_body=media_body, fields="id", supportsAllDrives=True
        ).execute()
        print(f"폴더 {GOOGLE_DRIVE_FOLDER_ID}에 새 파일 업로드 완료 (ID: {created.get('id')}).")

if __name__ == "__main__":
    main()

