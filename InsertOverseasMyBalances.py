import oracledb
import kis_auth as ka
import kis_ovrseastk as ko  # 해외 주식 데이터 관련 모듈 (가정)

# 토큰 발급
ka.auth()

# Oracle 데이터베이스 연결 설정
dsn = oracledb.makedsn('localhost', 1521, service_name='xe')
conn = oracledb.connect(user='system', password='pass', dsn=dsn)

print("Successfully connected to Oracle Database")

# cursor 객체 생성
cursor = conn.cursor()

# 데이터를 받아옵니다.
rt_data3 = ko.get_overseas_inquire_balance_lst()
account_id = "종합위탁"

# 필요한 열만 선택
required_columns3 = [
    'ovrs_pdno', 'ovrs_item_name', 'frcr_evlu_pfls_amt', 'evlu_pfls_rt',
    'pchs_avg_pric', 'ovrs_stck_evlu_amt', 'frcr_pchs_amt1', 'now_pric2', 'ovrs_cblc_qty'
]
filtered_data = rt_data3[required_columns3]

# 기존 데이터 삭제
delete_sql = "DELETE FROM SECURITIES_OVERSEAS_BALANCE WHERE ACCOUNT_ID = :account_id"
try:
    cursor.execute(delete_sql, {'account_id': account_id})
    print("Existing data deleted successfully")
except oracledb.DatabaseError as e:
    print(f"Error deleting existing data: {e}")
    conn.rollback()  # 오류 발생 시 롤백
    cursor.close()
    conn.close()
    exit(1)

# MERGE SQL문 (Oracle용)
merge_sql = """
MERGE INTO SECURITIES_OVERSEAS_BALANCE target
USING DUAL
ON (target.ACCOUNT_ID = :ACCOUNT_ID AND target.OVRS_PDNO = :ovrs_pdno)
WHEN MATCHED THEN
    UPDATE SET
        OVRS_ITEM_NAME = :ovrs_item_name,
        FRCR_EVLU_PFLS_AMT = :frcr_evlu_pfls_amt,
        EVLU_PFLS_RT = :evlu_pfls_rt,
        PCHS_AVG_PRIC = :pchs_avg_pric,
        OVRS_STCK_EVLU_AMT = :ovrs_stck_evlu_amt,
        FRCR_PCHS_AMT1 = :frcr_pchs_amt1,
        NOW_PRIC2 = :now_pric2,
        OVRS_CBLC_QTY = :ovrs_cblc_qty
WHEN NOT MATCHED THEN
    INSERT (
        ACCOUNT_ID, OVRS_PDNO, OVRS_ITEM_NAME, FRCR_EVLU_PFLS_AMT,
        EVLU_PFLS_RT, PCHS_AVG_PRIC, OVRS_STCK_EVLU_AMT, FRCR_PCHS_AMT1, NOW_PRIC2, OVRS_CBLC_QTY
    )
    VALUES (
        :ACCOUNT_ID, :ovrs_pdno, :ovrs_item_name, :frcr_evlu_pfls_amt, 
        :evlu_pfls_rt, :pchs_avg_pric, :ovrs_stck_evlu_amt, :frcr_pchs_amt1, :now_pric2, :ovrs_cblc_qty
    )
"""

for index, row in filtered_data.iterrows():
    try:
        # Prepare the data dictionary with consistent keys
        data = {
            'ACCOUNT_ID': account_id,
            'ovrs_pdno': row['ovrs_pdno'],
            'ovrs_item_name': row['ovrs_item_name'],
            'frcr_evlu_pfls_amt': row['frcr_evlu_pfls_amt'],
            'evlu_pfls_rt': row['evlu_pfls_rt'],
            'pchs_avg_pric': row['pchs_avg_pric'],
            'ovrs_stck_evlu_amt': row['ovrs_stck_evlu_amt'],
            'frcr_pchs_amt1': row['frcr_pchs_amt1'],
            'now_pric2': row['now_pric2'],
            'ovrs_cblc_qty': row['ovrs_cblc_qty']
        }

        # Debugging output
        print(f"Processing row {index}: {data}")

        # Execute the MERGE SQL statement
        cursor.execute(merge_sql, data)

        # Confirm successful processing
        print(f"Row {index} processed successfully")

    except oracledb.DatabaseError as e:
        # Print the error for the specific row and continue
        print(f"Error inserting/updating row {index}: {e}")
        continue

# Final commit and cleanup
try:
    conn.commit()
    print("Transaction committed")
except oracledb.DatabaseError as e:
    print(f"Error committing transaction: {e}")

cursor.close()
conn.close()

print("Data insertion/updation process completed.")
