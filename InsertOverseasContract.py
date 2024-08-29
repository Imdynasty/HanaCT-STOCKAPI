import oracledb
import pandas as pd
import kis_auth as ka
import kis_ovrseastk as ko  # 해외 주식 관련 모듈 (가정)

# 토큰 발급
ka.auth()

# Oracle 데이터베이스 연결 설정
dsn = oracledb.makedsn('localhost', 1521, service_name='xe')
conn = oracledb.connect(user='system', password='pass', dsn=dsn)

print("Successfully connected to Oracle Database")

# cursor 객체 생성
cursor = conn.cursor()

# 데이터를 받아옵니다.
rt_Data5 = ko.get_overseas_inquire_ccnl(st_dt="20240620", ccld_dv="00")

# 필요한 열만 선택: 해외 체결 내역 데이터
required_columns2 = [
    'ord_dt',               # 주문 날짜
    'ord_tmd',              # 주문 시간
    'odno',                 # 주문 번호
    'sll_buy_dvsn_cd_name', # 매도/매수 구분 코드 이름
    'prdt_name',            # 상품 이름 (종목명)
    'ft_ord_qty',           # 주문 수량
    'ft_ccld_qty',          # 총 체결 수량
    'ft_ccld_amt3',         # 총 체결 금액
    'pdno',                 # 종목 번호 (상품 코드)
    'ft_ord_unpr3',         # 주문 단가
    'tr_crcy_cd'            # 통화코드
]

filtered_data2 = rt_Data5[required_columns2]

# MERGE SQL문 (Oracle용)
merge_sql = """
    MERGE INTO SECURITIES_OVERSEAS_CONTRACT target
    USING DUAL
    ON (target.ODNO = :odno)
    WHEN MATCHED THEN
        UPDATE SET
            ORD_DT = TO_DATE(:ord_dt, 'YYYY-MM-DD'),
            ORD_TMD = :ord_tmd,
            SLL_BUY_DVSN_CD_NAME = :sll_buy_dvsn_cd_name,
            PRDT_NAME = :prdt_name,
            FT_ORD_QTY = :ft_ord_qty,
            FT_CCLD_QTY = :ft_ccld_qty,
            FT_CCLD_AMT3 = :ft_ccld_amt3,
            PDNO = :pdno,
            FT_ORD_UNPR3 = :ft_ord_unpr3,
            TR_CRCY_CD = :tr_crcy_cd
    WHEN NOT MATCHED THEN
        INSERT (ORD_DT, ORD_TMD, ODNO, SLL_BUY_DVSN_CD_NAME, PRDT_NAME, FT_ORD_QTY, FT_CCLD_QTY, FT_CCLD_AMT3, PDNO, FT_ORD_UNPR3, TR_CRCY_CD)
        VALUES (TO_DATE(:ord_dt, 'YYYY-MM-DD'), :ord_tmd, :odno, :sll_buy_dvsn_cd_name, :prdt_name, :ft_ord_qty, :ft_ccld_qty, :ft_ccld_amt3, :pdno, :ft_ord_unpr3, :tr_crcy_cd)
"""

# 데이터 삽입 및 업데이트
for index, row in filtered_data2.iterrows():
    try:
        # 데이터를 사전 형식으로 준비
        data = {
            'ord_dt': row['ord_dt'],  # 'YYYYMMDD' 형식의 날짜
            'ord_tmd': row['ord_tmd'],
            'odno': row['odno'],
            'sll_buy_dvsn_cd_name': row['sll_buy_dvsn_cd_name'],
            'prdt_name': row['prdt_name'],
            'ft_ord_qty': row['ft_ord_qty'],
            'ft_ccld_qty': row['ft_ccld_qty'],
            'ft_ccld_amt3': row['ft_ccld_amt3'],
            'pdno': row['pdno'],
            'ft_ord_unpr3': row['ft_ord_unpr3'],
            'tr_crcy_cd': row['tr_crcy_cd']
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
