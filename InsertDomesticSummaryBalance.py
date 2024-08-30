
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
rt_data5 = kb.get_inquire_balance_obj()

# 필요한 열만 선택하여 데이터베이스에 삽입합니다.
account_id = "종합위탁"  # 이 값을 실제 계좌 ID로 대체하세요.
# 총평가금액
tot_evlu_amt = rt_data5['tot_evlu_amt'].iloc[0] if 'tot_evlu_amt' in rt_data5 else '0'
# 매입합계금액
pchs_amt_smtl_amt = rt_data5['pchs_amt_smtl_amt'].iloc[0] if 'pchs_amt_smtl_amt' in rt_data5 else '0'
# 평가금액합계금액
evlu_amt_smtl_amt = rt_data5['evlu_amt_smtl_amt'].iloc[0] if 'evlu_amt_smtl_amt' in rt_data5 else '0'
# 실평가손익합계금액
evlu_pfls_smtl_amt = rt_data5['evlu_pfls_smtl_amt'].iloc[0] if 'evlu_pfls_smtl_amt' in rt_data5 else '0'
# 예수금
dnca_tot_amt = rt_data5['dnca_tot_amt'].iloc[0] if 'dnca_tot_amt' in rt_data5 else '0'
# 예수금1
nxdy_excc_amt = rt_data5['nxdy_excc_amt'].iloc[0] if 'nxdy_excc_amt' in rt_data5 else '0'
# 예수금2
prvs_rcdl_excc_amt = rt_data5['prvs_rcdl_excc_amt'].iloc[0] if 'prvs_rcdl_excc_amt' in rt_data5 else '0'

# 데이터를 삽입하기 위한 MERGE 구문
sql = """
MERGE INTO SECURITIES_BALANCE_SUMMARY sb
USING (SELECT :account_id AS account_id FROM dual) src
ON (sb.ACCOUNT_ID = src.account_id)
WHEN MATCHED THEN
    UPDATE SET
        TOT_EVLU_AMT = :tot_evlu_amt,
        PCHS_AMT_SMTL_AMT = :pchs_amt_smtl_amt,
        EVLU_AMT_SMTL_AMT = :evlu_amt_smtl_amt,
        EVLU_PFLS_SMTL_AMT = :evlu_pfls_smtl_amt,
        DNCA_TOT_AMT = :dnca_tot_amt,
        NXDY_EXCC_AMT = :nxdy_excc_amt,
        PRVS_RCDL_EXCC_AMT = :prvs_rcdl_excc_amt
WHEN NOT MATCHED THEN
    INSERT (
        ACCOUNT_ID, TOT_EVLU_AMT, PCHS_AMT_SMTL_AMT, EVLU_AMT_SMTL_AMT,
        EVLU_PFLS_SMTL_AMT, DNCA_TOT_AMT, NXDY_EXCC_AMT, PRVS_RCDL_EXCC_AMT
    )
    VALUES (
        :account_id, :tot_evlu_amt, :pchs_amt_smtl_amt, :evlu_amt_smtl_amt, 
        :evlu_pfls_smtl_amt, :dnca_tot_amt, :nxdy_excc_amt, :prvs_rcdl_excc_amt
    )
"""

# 변수를 바인딩하고 SQL문 실행
cursor.execute(sql, account_id=account_id, tot_evlu_amt=tot_evlu_amt,
               pchs_amt_smtl_amt=pchs_amt_smtl_amt, evlu_amt_smtl_amt=evlu_amt_smtl_amt,
               evlu_pfls_smtl_amt=evlu_pfls_smtl_amt, dnca_tot_amt=dnca_tot_amt,
               nxdy_excc_amt=nxdy_excc_amt, prvs_rcdl_excc_amt=prvs_rcdl_excc_amt)

# 커밋
conn.commit()

# 커서 및 연결 종료
cursor.close()
conn.close()

print("Data inserted successfully.")

