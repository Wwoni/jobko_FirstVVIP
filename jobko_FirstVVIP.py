import os
import io
import json
import base64
import pandas as pd
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# --- 설정 ---
GOOGLE_DRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID')  # 필수
GDRIVE_CREDENTIALS_DATA = os.environ.get('GDRIVE_CREDENTIALS_DATA')  # 선택(ENV JSON/BASE64)
GDRIVE_CREDENTIALS_PATH = os.environ.get('GDRIVE_CREDENTIALS_PATH')  # 선택(파일 경로)

CSV_FILE_NAME = 'jobkorea_postings.csv'


def _load_service_account_info():
    """
    서비스 계정 정보를 다음 우선순위로 로드
    1) 환경변수 GDRIVE_CREDENTIALS_DATA (JSON 문자열 → 실패 시 base64 → 실패 시 literal_eval)
    2) 환경변수 GDRIVE_CREDENTIALS_PATH 경로의 파일
    3) 로컬 'credentials.json' 파일
    """
    # 1) ENV – JSON 직파싱
    if GDRIVE_CREDENTIALS_DATA:
        # 먼저 JSON 시도
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
        # 마지막 안전 대안: literal_eval
        try:
            import ast
            return ast.literal_eval(GDRIVE_CREDENTIALS_DATA)
        except Exception as e:
            raise RuntimeError(
                "GDRIVE_CREDENTIALS_DATA를 파싱할 수 없습니다. JSON(또는 base64 인코딩된 JSON)을 사용하세요."
            ) from e

    # 2) 파일 경로
    candidate_paths = []
    if GDRIVE_CREDENTIALS_PATH:
        candidate_paths.append(GDRIVE_CREDENTIALS_PATH)
    candidate_paths.append('credentials.json')

    for p in candidate_paths:
        if os.path.exists(p):
            with open(p, 'r', encoding='utf-8') as f:
                return json.load(f)

    raise FileNotFoundError(
        "서비스 계정 자격증명을 찾을 수 없습니다. "
        "GDRIVE_CREDENTIALS_DATA 환경변수(JSON/BASE64)나 "
        "GDRIVE_CREDENTIALS_PATH/credentials.json 파일을 제공하세요."
    )


def get_gdrive_service():
    """Google Drive API 서비스 객체 생성"""
    if not GOOGLE_DRIVE_FOLDER_ID:
        raise ValueError("GDRIVE_FOLDER_ID 환경변수가 설정되어 있지 않습니다. 업로드 대상 폴더 ID를 지정하세요.")

    scopes = ['https://www.googleapis.com/auth/drive']
    info = _load_service_account_info()
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    drive = build('drive', 'v3', credentials=creds)
    return drive


def scrape_job_postings():
    """잡코리아 'First VVIP' 섹션 크롤링"""
    chrome_service = ChromeService(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_argument('--headless=new')  # 최신 헤드리스
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=chrome_service, options=options)

    driver.get('https://www.jobkorea.co.kr/')
    wait = WebDriverWait(driver, 20)

    # 팝업 닫기 (있을 때만)
    try:
        popup_close_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), '오늘 하루 안보기')]"))
        )
        popup_close_button.click()
        print("팝업을 닫았습니다.")
    except Exception:
        print("팝업이 없거나 닫지 못했지만 계속 진행합니다.")

    # First VVIP 섹션
    first_vvip_section = wait.until(EC.presence_of_element_located((By.ID, 'Prdt_BnnrFirstVVIP')))
    list_items = first_vvip_section.find_elements(By.CSS_SELECTOR, 'ul.list_firstvvip li')

    data = []
    for item in list_items:
        # 회사명
        try:
            company_name = item.find_element(By.CSS_SELECTOR, 'span.name a').text.strip()
        except:
            try:
                company_name = item.find_element(By.CSS_SELECTOR, 'span.logo img').get_attribute('alt').strip()
            except:
                company_name = 'No Company Name'

        # 로고 URL
        try:
            logo_url = item.find_element(By.CSS_SELECTOR, 'span.logo img').get_attribute('src').strip()
        except:
            logo_url = 'No Logo URL'

        # 채용 공고 제목
        try:
            job_title = item.find_element(By.CSS_SELECTOR, 'div.description a').text.strip()
        except:
            job_title = 'No Title'

        # 채용 공고 요약
        try:
            job_summary = item.find_element(By.CSS_SELECTOR, 'div.addition div.summary').text.strip()
        except:
            job_summary = 'No Summary'

        # 마감일
        try:
            dday = item.find_element(By.CSS_SELECTOR, 'div.extra .dday').text.strip()
        except:
            dday = 'No D-Day'

        # 채용 공고 URL
        job_url = 'No URL'
        try:
            job_url = item.find_element(By.CSS_SELECTOR, 'div.description a').get_attribute('href')
        except:
            try:
                job_url = item.find_element(By.CSS_SELECTOR, 'a.card-wrap').get_attribute('href')
            except:
                pass

        data.append({
            'Company Name': company_name,
            'Logo URL': logo_url,
            'Job Title': job_title,
            'Job Summary': job_summary,
            'D-Day': dday,
            'Job URL': job_url,
            'Scraped Date': datetime.now().strftime("%Y-%m-%d")
        })

    driver.quit()
    print(f"총 {len(data)}개의 채용 공고를 수집했습니다.")
    return pd.DataFrame(data)


def main():
    drive = get_gdrive_service()

    # 1) 기존 CSV 검색
    query = f"name='{CSV_FILE_NAME}' and '{GOOGLE_DRIVE_FOLDER_ID}' in parents and trashed=false"
    resp = drive.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    files = resp.get('files', [])

    existing_df = pd.DataFrame()
    file_id = None

    if files:
        file_id = files[0]['id']
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
        drive.files().update(fileId=file_id, media_body=media_body).execute()
        print(f"파일 ID {file_id} 업데이트 완료.")
    else:
        file_metadata = {'name': CSV_FILE_NAME, 'parents': [GOOGLE_DRIVE_FOLDER_ID]}
        created = drive.files().create(body=file_metadata, media_body=media_body, fields='id').execute()
        print(f"폴더 {GOOGLE_DRIVE_FOLDER_ID}에 새 파일 업로드 완료 (ID: {created.get('id')}).")


if __name__ == '__main__':
    main()
