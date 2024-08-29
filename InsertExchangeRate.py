from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
import time
import oracledb

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

# 6. 통화 선택 (USD: 미국 달러)
currency_select = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#curCd")))
select = Select(currency_select)
select.select_by_value("USD")

# 7. "조회" 버튼 클릭
query_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#HANA_CONTENTS_DIV > div.btnBoxCenter > a")))
query_button.click()

# 8. 조회 후 5초 대기
time.sleep(3)

# 9. 테이블 데이터 크롤링
rows = driver.find_elements(By.CSS_SELECTOR, "#searchContentDiv > div.printdiv > table > tbody > tr")
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

# Oracle 데이터베이스 연결 설정
dsn = oracledb.makedsn('localhost', 1521, service_name='xe')
conn = oracledb.connect(user='system', password='pass', dsn=dsn)

print("Successfully connected to Oracle Database")

# cursor 객체 생성
cursor = conn.cursor()

# 데이터를 삽입하기 위한 MERGE 구문
sql = """
MERGE INTO SECURITIES_EXCHANGE_RATE ser
USING (SELECT TO_DATE(:rate_date, 'YYYY-MM-DD') AS rate_date FROM dual) src
ON (ser.rate_date = src.rate_date)
WHEN MATCHED THEN
    UPDATE SET
        buy_rate = :buy_rate,
        sell_rate = :sell_rate,
        send_rate = :send_rate,
        receive_rate = :receive_rate,
        foreign_check_rate = :foreign_check_rate,
        trading_standard_rate = :trading_standard_rate,
        change_rate = :change_rate,
        exchange_fee = :exchange_fee,
        usd_conversion_rate = :usd_conversion_rate
WHEN NOT MATCHED THEN
    INSERT (
        rate_date, buy_rate, sell_rate, send_rate, receive_rate, 
        foreign_check_rate, trading_standard_rate, change_rate, exchange_fee, usd_conversion_rate
    )
    VALUES (
        TO_DATE(:rate_date, 'YYYY-MM-DD'), :buy_rate, :sell_rate, :send_rate, :receive_rate, 
        :foreign_check_rate, :trading_standard_rate, :change_rate, :exchange_fee, :usd_conversion_rate
    )
"""

# 데이터 삽입
for data in data_list:
    try:
        cursor.execute(sql, rate_date=data[0], buy_rate=data[1], sell_rate=data[2], send_rate=data[3],
                       receive_rate=data[4], foreign_check_rate=data[5], trading_standard_rate=data[6],
                       change_rate=data[7], exchange_fee=data[8], usd_conversion_rate=data[9])
        conn.commit()
        print(f"Data inserted successfully: {data}")
    except Exception as e:
        print(f"Error inserting data: {e}")

# 커서 및 연결 종료
cursor.close()
conn.close()

print("Data inserted successfully.")
