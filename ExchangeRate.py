from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
import time

data_list = []
# 웹 드라이버 설정 (예: Chrome)
driver = webdriver.Chrome()

# 1. 페이지 로드
url = "https://www.kebhana.com/cms/rate/index.do?contentUrl=/cms/rate/wpfxd651_07i.do#//HanaBank"
driver.get(url)

# 2. 페이지 로드 대기
wait = WebDriverWait(driver, 10)

# 3. 로딩 레이어가 사라질 때까지 대기
try:
    loading_layer = wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "#OPB_loadingLayerID_generatedByJSOPB_modalMaskID_generatedByJS")))
    wait.until(EC.invisibility_of_element(loading_layer))
except TimeoutException:
    print("Loading layer did not disappear, continuing...")

# 4. "기간환율변동" 선택 클릭
try:
    period_exchange_button = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "label[for='inqType_p'].radioForm")))
    period_exchange_button.click()
except (TimeoutException, ElementClickInterceptedException):
    driver.execute_script("document.querySelector(\"label[for='inqType_p']\").click();")

# 5. 날짜 수정 (조회 기간 시작일)
date_input = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#tmpInqStrDt_p")))
date_input.clear()
date_input.send_keys("2023-07-20")

# 6. 통화 선택 (EUR:유로(유럽연합))
currency_select = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#curCd")))
select = Select(currency_select)
select.select_by_value("USD")

# 7. "조회" 버튼 클릭
query_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#HANA_CONTENTS_DIV > div.btnBoxCenter > a")))
query_button.click()

# 8. 조회 후 5초 대기
time.sleep(3)

# 9. 테이블 헤더 크롤링
# header = driver.find_elements(By.CSS_SELECTOR, "#searchContentDiv > div.printdiv > table > thead > tr")
# for row in header:
#     columns = row.find_elements(By.TAG_NAME, "th")
#     header_data = [column.text.replace("\n", " ") for column in columns]  # <br> 태그 대신 공백으로 교체
#     print("Header:", header_data)

# 10. 테이블 데이터 크롤링
rows = driver.find_elements(By.CSS_SELECTOR, "#searchContentDiv > div.printdiv > table > tbody > tr")
# for row in rows:
#     columns = row.find_elements(By.TAG_NAME, "td")
#     data = [column.text for column in columns]
#     data.append(data)
#     print("Row:", data)
for row in rows:
    columns = row.find_elements(By.TAG_NAME, "td")
    data = [column.text for column in columns]
    # 첫 번째 값 제외, 나머지를 float로 변환
    for i in range(1, len(data)):
        try:
            data[i] = float(data[i].replace(',', ''))  # 쉼표 제거 후 float 변환
        except ValueError:
            print(f"변환 실패: {data[i]}")
    data_list.append(data)
    print("Row:", data)

# 웹 드라이버 종료
driver.quit()