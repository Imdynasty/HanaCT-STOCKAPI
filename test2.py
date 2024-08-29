#kis_domstk module 을 찾을 수 없다는 에러가 나는 경우 sys.path에 kis_domstk.py 가 있는 폴더를 추가해준다.
import kis_auth as ka
import kis_domstk as kb
import kis_ovrseastk as ko
import pandas as pd

# pandas 출력 옵션 설정: 모든 열과 행이 표시되도록
pd.set_option('display.max_columns', None)  # 모든 열을 출력
pd.set_option('display.max_rows', None)     # 모든 행을 출력
pd.set_option('display.width', None)        # 줄바꿈 없이 출력
pd.set_option('display.max_colwidth', None)



# 토큰 발급
ka.auth()

#주식일별주문체조회
# rt_data = kb.get_inquire_daily_ccld_lst(dv="01")
# print(rt_data)


# # [국내주식] 기본시세 > 주식현재가 체결 (종목번호 6자리)
# rt_data = kb.get_inquire_price(itm_no="005380")
# print(rt_data)

# [국내주식] 주문/계좌 > 주식현금주문 (매수매도구분 buy,sell + 종목번호 6자리 + 주문수량 + 주문단가)
# # 지정가 기준이며 시장가 옵션(주문구분코드)을 사용하는 경우 kis_domstk.py get_order_cash 수정요망!
# rt_data1 = kb.get_order_cash(ord_dv="buy",itm_no="005930", qty=2, unpr=90000)
# print(rt_data1.KRX_FWDG_ORD_ORGNO + "+" + rt_data1.ODNO + "+" + rt_data1.ORD_TMD) # 주문접수조직번호+주문접수번호+주문시각
# #
# # # # [국내주식] 기본시세 > 주식현재가 시세 (종목번호 6자리)
# rt_data = kb.get_inquire_price(itm_no="071050")
# print(rt_data.stck_prpr+ " " + rt_data.prdy_vrss)    # 현재가, 전일대비
# #
# #
# # # 주식계좌잔고 종목별 List를 DataFrame 으로 반환
# 주식계좌잔고 종목별 List를 DataFrame으로 반환
# rt_data3 = kb.get_inquire_balance_lst()
# # # 필요한 열만 선택 보유수량, 등락율, 매입금액, 평가손익률, 평가손익금,, 종목명, 현재가,매입평균가('pchs_avg_pric',),종목번호('pdno')
# required_columns = ['hldg_qty', 'fltt_rt', 'pchs_amt', 'evlu_pfls_rt','evlu_pfls_amt', 'prdt_name','prpr','pchs_avg_pric','pdno']
# filtered_data = rt_data3[required_columns]
# # 선택된 데이터 출력
# print(filtered_data)

# rt_data5 = kb.get_inquire_balance_obj();
# # 필요한 열만 선택 총평가금액, 매입금액합계금액,평가금액합계금액,실평가손익합계금액
# required_columns2 = ['tot_evlu_amt','pchs_amt_smtl_amt','evlu_amt_smtl_amt','evlu_pfls_smtl_amt']
# filtered_data2 = rt_data5[required_columns2]
# #evlu_amt_smtl_amt / pchs_amt_smtl_amt =>> 평가금액합계금액 / 매입금액합계금액 = 평가손익률
# # 선택된 데이터 출력
# print(filtered_data2)
#
# rt_data6 = kb.get_inquire_balance_obj();
# # 필요한 열만 선택 예수금, 예수금1, 예수금2
# required_columns3 = ['dnca_tot_amt','nxdy_excc_amt','prvs_rcdl_excc_amt']
# filtered_data3 = rt_data6[required_columns3]
# #evlu_amt_smtl_amt / pchs_amt_smtl_amt =>> 평가금액합계금액 / 매입금액합계금액 = 평가손익률
# # 선택된 데이터 출력
# print(filtered_data3)


# [해외주식]# 주식계좌잔고 종목별 List를 DataFrame으로 반환
#
# rt_data4 = ko.get_overseas_inquire_balance_lst()
# #해외상품번호,해외종목명,외화평가손익금액,평가손익률, 매입평균가격, 해외주식평가금액, 외화매입금액,현재가격
# required_columns3 = ['ovrs_pdno','ovrs_item_name','frcr_evlu_pfls_amt','evlu_pfls_rt','pchs_avg_pric','ovrs_stck_evlu_amt','frcr_pchs_amt1','now_pric2','ovrs_cblc_qty']
# filtered_data3 = rt_data4[required_columns3]
# # 선택된 데이터 출력
# print(filtered_data3)
# #
# # [해외주식]# 주식계좌잔고 종목별 List를 DataFrame으로 반환
# rt_data7 = ko.get_overseas_inquire_balance()
# #  해외총손익, 총수익률, 매입금액 합계,
# required_columns7 = ['ovrs_tot_pfls','tot_pftrt','frcr_buy_amt_smtl1']
# filtered_data7 = rt_data7[required_columns7]
# # 선택된 데이터 출력
# print(filtered_data7)
# #
# rt_data8 =ko.get_overseas_inquire_present_balance()
# # 총자산금액, 평가금액합계
# required_columns8 = ['tot_asst_amt','evlu_amt_smtl']
# filtered_data8 = rt_data8[required_columns8]
# # 선택된 데이터 출력
# print(filtered_data8)



# 국내체결내역, ccld_dvsn="02" 이면 미체결 내역 00 전체 01 체결,
# rt_Data5 = kb.get_inquire_daily_ccld_lst(inqr_strt_dt="20240620", ccld_dvsn="01")
# required_columns2 = [
#     'ord_dt',                # 주문 날짜
#     'ord_tmd',               # 주문 시간
#     'odno',                  # 주문 번호
#     'sll_buy_dvsn_cd_name',  # 매도/매수 구분 코드 이름
#     'prdt_name',             # 상품 이름 (종목명)
#     'ord_qty',               # 주문 수량
#     'tot_ccld_qty',          # 총 체결 수량
#     'tot_ccld_amt',          # 총 체결 금액
#     'pdno',                  # 종목 번호 (상품 코드)
#     'ord_unpr',              # 주문 단가
#     'avg_prvs',              # 평균 이전 가격
#     'ccld_cndt_name',         # 체결 조건 가격
# ]
# filtered_data2 = rt_Data5[required_columns2]
# print(filtered_data2)

rt_Data5 = ko.get_overseas_inquire_ccnl(st_dt="20240620",ccld_dv="00")
required_columns2 = [
    'ord_dt',                # 주문 날짜
    'ord_tmd',               # 주문 시간
    'odno',                  # 주문 번호
    'sll_buy_dvsn_cd_name',  # 매도/매수 구분 코드 이름
    'prdt_name',             # 상품 이름 (종목명)
    'ft_ord_qty',               # 주문 수량
    'ft_ccld_qty',          # 총 체결 수량
    'ft_ccld_amt3',          # 총 체결 금액
    'pdno',                  # 종목 번호 (상품 코드)
    'ft_ord_unpr3',              # 주문 단가
    'tr_crcy_cd',         # 통화코드
]
filtered_data2 = rt_Data5[required_columns2]
print(filtered_data2)
