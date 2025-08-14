import os
import pandas as pd
import io
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload

# --- 설정 ---
# GitHub Actions Secret 또는 로컬 환경 변수에서 값 가져오기
# 로컬 테스트 시: 'YOUR_FOLDER_ID' 와 같이 직접 문자열을 입력하세요.
GOOGLE_DRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID')
# GitHub Actions Secret에서 받아온 JSON 형식의 인증 정보를 파싱합니다.
# 로컬 테스트 시: credentials.json 파일을 직접 로드하도록 코드를 수정해야 합니다.
gcp_credentials_string = os.environ.get('GDRIVE_CREDENTIALS_DATA')

# 최종 저장될 CSV 파일 이름
CSV_FILE_NAME = 'jobkorea_postings.csv'


def get_gdrive_service():
    """Google Drive API 서비스 객체를 생성하고 반환합니다."""
    scopes = ['https://www.googleapis.com/auth/drive']
    creds_dict = eval(gcp_credentials_string) # 문자열을 딕셔너리로 변환
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    service = build('drive', 'v3', credentials=creds)
    return service

def scrape_job_postings():
    """잡코리아 'First VVIP' 섹션의 채용 공고를 크롤링합니다."""
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_argument('--headless') # 백그라운드 실행
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=service, options=options)

    driver.get('https://www.jobkorea.co.kr/')
    
    wait = WebDriverWait(driver, 20)
    
    try:
        # '오늘 하루 안보기' 팝업 닫기 (있을 경우)
        popup_close_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), '오늘 하루 안보기')]")))
        popup_close_button.click()
        print("팝업을 닫았습니다.")
    except Exception as e:
        print("팝업이 없거나 닫는 데 실패했습니다.", e)

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

        # 채용 공고 URL (가장 중요한 부분, 여러 방법 시도)
        job_url = 'No URL'
        try: # 1순위 방법 (가장 안정적)
            job_url = item.find_element(By.CSS_SELECTOR, 'div.description a').get_attribute('href')
        except:
            try: # 2순위 방법
                job_url = item.find_element(By.CSS_SELECTOR, 'a.card-wrap').get_attribute('href')
            except:
                pass # job_url은 'No URL'로 유지

        data.append({
            'Company Name': company_name,
            'Logo URL': logo_url,
            'Job Title': job_title,
            'Job Summary': job_summary,
            'D-Day': dday,
            'Job URL': job_url,
            'Scraped Date': datetime.now().strftime("%Y-%m-%d") # 수집 날짜 추가
        })

    driver.quit()
    print(f"총 {len(data)}개의 채용 공고를 수집했습니다.")
    return pd.DataFrame(data)


def main():
    service = get_gdrive_service()
    
    # 1. 구글 드라이브에서 기존 CSV 파일 검색
    query = f"name='{CSV_FILE_NAME}' and '{GOOGLE_DRIVE_FOLDER_ID}' in parents and trashed=false"
    response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
    files = response.get('files', [])

    existing_df = pd.DataFrame()
    file_id = None

    if files:
        file_id = files[0].get('id')
        print(f"기존 파일 '{CSV_FILE_NAME}' (ID: {file_id})을 찾았습니다. 데이터를 다운로드합니다.")
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        existing_df = pd.read_csv(fh)
        print(f"기존 데이터 {len(existing_df)}개를 로드했습니다.")
    else:
        print(f"기존 파일 '{CSV_FILE_NAME}'을 찾을 수 없습니다. 새로운 파일을 생성합니다.")

    # 2. 새로운 데이터 크롤링
    new_df = scrape_job_postings()

    # 3. 데이터 합치기 및 중복 제거
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    # Job URL이 같으면 중복으로 간주하고, 가장 최근에 수집된 데이터(last)를 남김
    combined_df.drop_duplicates(subset=['Job URL'], keep='last', inplace=True)
    
    print(f"데이터 병합 및 중복 제거 후 총 {len(combined_df)}개의 데이터를 준비했습니다.")

    # 4. 최종 데이터를 CSV로 변환하여 구글 드라이브에 업로드
    csv_buffer = io.StringIO()
    combined_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    csv_buffer.seek(0)
    
    media_body = MediaIoBaseUpload(io.BytesIO(csv_buffer.read().encode('utf-8')), mimetype='text/csv', resumable=True)
    
    if file_id: # 기존 파일이 있으면 업데이트
        service.files().update(fileId=file_id, media_body=media_body).execute()
        print(f"파일 ID {file_id}를 성공적으로 업데이트했습니다.")
    else: # 없으면 새로 생성
        file_metadata = {'name': CSV_FILE_NAME, 'parents': [GOOGLE_DRIVE_FOLDER_ID]}
        service.files().create(body=file_metadata, media_body=media_body, fields='id').execute()
        print(f"폴더 ID {GOOGLE_DRIVE_FOLDER_ID}에 새 파일을 성공적으로 업로드했습니다.")


if __name__ == '__main__':
    main()
