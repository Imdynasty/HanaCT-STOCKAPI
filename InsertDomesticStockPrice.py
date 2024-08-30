import pandas as pd
import chardet
import oracledb

# Oracle 데이터베이스 연결 설정
dsn = oracledb.makedsn('localhost', 1521, service_name='xe')
conn = oracledb.connect(user='system', password='pass', dsn=dsn)

print("Successfully connected to Oracle Database")

# cursor 객체 생성
cursor = conn.cursor()

# CSV 파일 경로
csv_file_path = "/Users/smin/Downloads/373220 과거 데이터.csv"

# 파일의 인코딩을 자동으로 감지
with open(csv_file_path, 'rb') as file:
    result = chardet.detect(file.read())
    encoding = result['encoding']

print(f"Detected file encoding: {encoding}")

# CSV 파일 읽기 (감지된 인코딩 사용)
df = pd.read_csv(csv_file_path, encoding=encoding)

# 데이터프레임 열 이름을 DB 열 이름에 맞게 변경
df.columns = ['trading_date', 'close_price', 'open_price', 'high_price', 'low_price', 'volume', 'change_percentage']

# stockcode 컬럼 추가 (assuming the stock code is '086790')
df['stockcode'] = '373220'

# 날짜 형식 변환 (assuming the date format is YYYY-MM-DD in the CSV with spaces)
df['trading_date'] = pd.to_datetime(df['trading_date'].str.replace(' ', ''), format='%Y-%m-%d')

# 거래량과 변동 %에 있는 쉼표 및 문자 제거 및 변환 처리
df['close_price'] = df['close_price'].str.replace(',', '').astype(float)
df['open_price'] = df['open_price'].str.replace(',', '').astype(float)
df['high_price'] = df['high_price'].str.replace(',', '').astype(float)
df['low_price'] = df['low_price'].str.replace(',', '').astype(float)
df['volume'] = df['volume'].str.replace('K', '000').str.replace('M', '000000').str.replace(',', '').astype(float)
df['change_percentage'] = df['change_percentage'].str.replace('%', '').astype(float)

# 데이터프레임의 데이터 타입 출력 (디버깅용)
print(df.dtypes)
print(df.head())

# 데이터 삽입 쿼리
insert_query = """
    INSERT INTO SECURITIES_DOMESTIC_STOCKPRICES (
        stockcode, trading_date, close_price, open_price, high_price, low_price, volume, change_percentage
    ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
"""

# 데이터베이스에 데이터 삽입
rows_inserted = 0
for index, row in df.iterrows():
    try:
        cursor.execute(insert_query, {
            '1': row['stockcode'],
            '2': row['trading_date'].date(),
            '3': row['close_price'],
            '4': row['open_price'],
            '5': row['high_price'],
            '6': row['low_price'],
            '7': row['volume'],
            '8': row['change_percentage']
        })
        rows_inserted += 1
        if index % 100 == 0:  # Optional: Commit every 100 rows
            conn.commit()
    except oracledb.DatabaseError as e:
        print(f"Error inserting row {index}: {e}")

# 최종 커밋 (데이터베이스에 변경사항 저장)
conn.commit()

print(f"Data inserted successfully, total rows inserted: {rows_inserted}")

# 연결 닫기
cursor.close()
conn.close()
