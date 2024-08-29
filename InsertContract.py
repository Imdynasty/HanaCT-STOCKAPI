import oracledb
import pandas as pd
from datetime import datetime
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
rt_data4 = kb.get_inquire_daily_ccld_lst(inqr_strt_dt="20240620", ccld_dvsn="01")

# 필요한 열만 선택: 주문 날짜, 주문 시간, 주문 번호 등
required_columns = [
    'ord_dt',                # 주문 날짜
    'ord_tmd',               # 주문 시간
    'odno',                  # 주문 번호
    'sll_buy_dvsn_cd_name',  # 매도/매수 구분 코드 이름
    'prdt_name',             # 상품 이름 (종목명)
    'ord_qty',               # 주문 수량
    'tot_ccld_qty',          # 총 체결 수량
    'tot_ccld_amt',          # 총 체결 금액
    'pdno',                  # 종목 번호 (상품 코드)
    'ord_unpr',              # 주문 단가
    'avg_prvs',              # 평균 이전 가격
    'ccld_cndt_name'         # 체결 조건 가격
]
filtered_data = rt_data4[required_columns]

# 날짜 형식을 Oracle이 인식할 수 있는 형태로 변환
filtered_data['ord_dt'] = filtered_data['ord_dt'].apply(lambda x: datetime.strptime(x, '%Y%m%d').strftime('%Y-%m-%d'))

# MERGE SQL문 (Oracle용)
merge_sql = """
    MERGE INTO SECURITIES_DOMESTIC_CONTRACT target
    USING DUAL
    ON (target.ODNO = :odno)
    WHEN MATCHED THEN
        UPDATE SET
            ORD_DT = TO_DATE(:ord_dt, 'YYYY-MM-DD'),  -- Oracle의 DATE 형식으로 변환
            ORD_TMD = :ord_tmd,
            SLL_BUY_DVSN_CD_NAME = :sll_buy_dvsn_cd_name,
            PRDT_NAME = :prdt_name,
            ORD_QTY = :ord_qty,
            TOT_CCLD_QTY = :tot_ccld_qty,
            TOT_CCLD_AMT = :tot_ccld_amt,
            PDNO = :pdno,
            ORD_UNPR = :ord_unpr,
            AVG_PRVS = :avg_prvs,
            CCLD_CNDT_NAME = :ccld_cndt_name
    WHEN NOT MATCHED THEN
        INSERT (ORD_DT, ORD_TMD, ODNO, SLL_BUY_DVSN_CD_NAME, PRDT_NAME, ORD_QTY, TOT_CCLD_QTY, TOT_CCLD_AMT, PDNO, ORD_UNPR, AVG_PRVS, CCLD_CNDT_NAME)
        VALUES (TO_DATE(:ord_dt, 'YYYY-MM-DD'), :ord_tmd, :odno, :sll_buy_dvsn_cd_name, :prdt_name, :ord_qty, :tot_ccld_qty, :tot_ccld_amt, :pdno, :ord_unpr, :avg_prvs, :ccld_cndt_name)
"""

# 데이터 삽입 및 업데이트
for index, row in filtered_data.iterrows():
    try:
        # 데이터를 사전 형식으로 준비
        data = {
            'ord_dt': row['ord_dt'],  # 'YYYY-MM-DD' 형식의 날짜
            'ord_tmd': row['ord_tmd'],
            'odno': row['odno'],
            'sll_buy_dvsn_cd_name': row['sll_buy_dvsn_cd_name'],
            'prdt_name': row['prdt_name'],
            'ord_qty': row['ord_qty'],
            'tot_ccld_qty': row['tot_ccld_qty'],
            'tot_ccld_amt': row['tot_ccld_amt'],
            'pdno': row['pdno'],
            'ord_unpr': row['ord_unpr'],
            'avg_prvs': row['avg_prvs'],
            'ccld_cndt_name': row['ccld_cndt_name']
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
