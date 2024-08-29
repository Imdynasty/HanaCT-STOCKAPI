import oracledb
import kis_auth as ka
import kis_ovrseastk as ko

# 토큰 발급
ka.auth()

# Oracle 데이터베이스 연결 설정
dsn = oracledb.makedsn('localhost', 1521, service_name='xe')
conn = oracledb.connect(user='system', password='pass', dsn=dsn)

print("Successfully connected to Oracle Database")

# cursor 객체 생성
cursor = conn.cursor()

# rt_data7에서 필요한 데이터를 받아옵니다.
rt_data7 = ko.get_overseas_inquire_balance()

# 해외총손익, 총수익률, 매입금액 합계
ovrs_tot_pfls = rt_data7['ovrs_tot_pfls'].iloc[0] if 'ovrs_tot_pfls' in rt_data7 else '0'
tot_pftrt = rt_data7['tot_pftrt'].iloc[0] if 'tot_pftrt' in rt_data7 else '0'
frcr_buy_amt_smtl1 = rt_data7['frcr_buy_amt_smtl1'].iloc[0] if 'frcr_buy_amt_smtl1' in rt_data7 else '0'

# rt_data8에서 필요한 데이터를 받아옵니다.
rt_data8 = ko.get_overseas_inquire_present_balance()

# 총자산금액, 평가금액합계
tot_asst_amt = rt_data8['tot_asst_amt'].iloc[0] if 'tot_asst_amt' in rt_data8 else '0'
evlu_amt_smtl = rt_data8['evlu_amt_smtl'].iloc[0] if 'evlu_amt_smtl' in rt_data8 else '0'

# 계좌 ID 설정
account_id = "종합위탁"  # 이 값을 실제 계좌 ID로 대체하세요.

# 데이터를 삽입하기 위한 MERGE 구문
sql = """
MERGE INTO SECURITIES_OVERSEAS_BALANCE_SUMMARY sb
USING (SELECT :account_id AS account_id FROM dual) src
ON (sb.ACCOUNT_ID = src.account_id)
WHEN MATCHED THEN
    UPDATE SET
        OVRS_TOT_PFLS = :ovrs_tot_pfls,
        TOT_PFTRT = :tot_pftrt,
        FRCR_BUY_AMT_SMTL1 = :frcr_buy_amt_smtl1,
        TOT_ASST_AMT = :tot_asst_amt,
        EVLU_AMT_SMTL = :evlu_amt_smtl
WHEN NOT MATCHED THEN
    INSERT (
        ACCOUNT_ID, OVRS_TOT_PFLS, TOT_PFTRT, FRCR_BUY_AMT_SMTL1,
        TOT_ASST_AMT, EVLU_AMT_SMTL
    )
    VALUES (
        :account_id, :ovrs_tot_pfls, :tot_pftrt, :frcr_buy_amt_smtl1, 
        :tot_asst_amt, :evlu_amt_smtl
    )
"""

# 변수를 바인딩하고 SQL문 실행
cursor.execute(sql, account_id=account_id, ovrs_tot_pfls=ovrs_tot_pfls,
               tot_pftrt=tot_pftrt, frcr_buy_amt_smtl1=frcr_buy_amt_smtl1,
               tot_asst_amt=tot_asst_amt, evlu_amt_smtl=evlu_amt_smtl)

# 커밋
conn.commit()

# 커서 및 연결 종료
cursor.close()
conn.close()

print("Data inserted successfully.")
