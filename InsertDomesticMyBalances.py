import oracledb
import kis_auth as ka
import kis_domstk as kb

# 토큰 발급
ka.auth()

# Oracle 데이터베이스 연결 설정
dsn = oracledb.makedsn('localhost', 1521, service_name='xe')
conn = oracledb.connect(user='system', password='pass', dsn=dsn)

print("Successfully connected to Oracle Database")

# cursor 객체 생성
cursor = conn.cursor()

# 데이터를 받아옵니다.
rt_data3 = kb.get_inquire_balance_lst()

# 필요한 열만 선택 보유수량, 등락율, 매입금액, 평가손익률, 평가손익금, 종목명, 현재가, 매입평균가, 종목번호
required_columns = ['hldg_qty', 'fltt_rt', 'pchs_amt', 'evlu_pfls_rt', 'evlu_pfls_amt', 'prdt_name', 'prpr',
                    'pchs_avg_pric', 'pdno']
filtered_data = rt_data3[required_columns]

# MERGE SQL문 (Oracle용)
merge_sql = """
    MERGE INTO SECURITIES_DOMESTIC_BALANCE target
    USING DUAL
    ON (target.ACCOUNT_ID = :account_id AND target.PDNO = :pdno)
    WHEN MATCHED THEN
        UPDATE SET
            HLDG_QTY = :hldg_qty,
            FLTT_RT = :fltt_rt,
            PCHS_AMT = :pchs_amt,
            EVLU_PFLS_RT = :evlu_pfls_rt,
            EVLU_PFLS_AMT = :evlu_pfls_amt,
            PRDT_NAME = :prdt_name,
            PRPR = :prpr,
            PCHS_AVG_PRIC = :pchs_avg_pric
    WHEN NOT MATCHED THEN
        INSERT (ACCOUNT_ID, HLDG_QTY, FLTT_RT, PCHS_AMT, EVLU_PFLS_RT, EVLU_PFLS_AMT, PRDT_NAME, PRPR, PCHS_AVG_PRIC, PDNO)
        VALUES (:account_id, :hldg_qty, :fltt_rt, :pchs_amt, :evlu_pfls_rt, :evlu_pfls_amt, :prdt_name, :prpr, :pchs_avg_pric, :pdno)
"""

# 모든 행을 순회하며 데이터베이스에 삽입 또는 업데이트
for index, row in filtered_data.iterrows():
    try:
        # 데이터를 사전 형식으로 준비
        data = {
            'account_id': '종합위탁',  # 실제 계좌 ID로 대체
            'hldg_qty': row['hldg_qty'],
            'fltt_rt': row['fltt_rt'],
            'pchs_amt': row['pchs_amt'],
            'evlu_pfls_rt': row['evlu_pfls_rt'],
            'evlu_pfls_amt': row['evlu_pfls_amt'],
            'prdt_name': row['prdt_name'],
            'prpr': row['prpr'],
            'pchs_avg_pric': row['pchs_avg_pric'],
            'pdno': row['pdno']
        }

        # 데이터 출력
        print(f"Processing row {index}: {data}")

        # MERGE SQL문 실행
        cursor.execute(merge_sql, data)

        # SQL 실행 후 상태 출력
        print(f"Row {index} processed successfully")

    except oracledb.DatabaseError as e:
        print(f"Error inserting/updating row {index}: {e}")
        continue

# 커밋 및 연결 종료
try:
    conn.commit()
    print("Transaction committed")
except oracledb.DatabaseError as e:
    print(f"Error committing transaction: {e}")

cursor.close()
conn.close()

print("Data insertion/updation process completed.")
