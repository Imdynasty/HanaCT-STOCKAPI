#kis_domstk module 을 찾을 수 없다는 에러가 나는 경우 sys.path에 kis_domstk.py 가 있는 폴더를 추가해준다.
import kis_auth as ka
import kis_ovrseastk as ko
import pandas as pd


# 토큰 발급
ka.auth()

rt_data3 = ko.get_inquire_balance_lst()
# # 필요한 열만 선택 보유수량, 등락율, 매입금액, 평가손익률, 평가손익금,, 종목명, 현재가,매입평균가('pchs_avg_pric',),종목번호('pdno')
required_columns = ['hldg_qty', 'fltt_rt', 'pchs_amt', 'evlu_pfls_rt','evlu_pfls_amt', 'prdt_name','prpr','pchs_avg_pric','pdno']
filtered_data = rt_data3[required_columns]
# 선택된 데이터 출력
print(filtered_data)

