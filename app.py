from datetime import datetime

from flask import Flask, request,jsonify
from flask_cors import CORS
import oracledb
import kis_auth as ka
import kis_domstk as kb
import kis_ovrseastk as ko

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["http://localhost:3000"]}})

# KIS API 인증
ka.auth()

# Oracle 데이터베이스 연결 설정
dsn = oracledb.makedsn('localhost', 1521, service_name='xe')
conn = oracledb.connect(user='system', password='pass', dsn=dsn)
global_tot_evlu_amt = 0.0
global_tot_asst_amt = 0.0
@app.route('/update_balance', methods=['POST'])
def update_balance():
    try:
        cursor = conn.cursor()

        # 기존 국내 주식 데이터 삭제
        delete_sql_domestic = "DELETE FROM SECURITIES_DOMESTIC_BALANCE WHERE ACCOUNT_ID = :account_id"
        cursor.execute(delete_sql_domestic, {'account_id': '종합위탁'})
        print("Existing domestic data deleted successfully")

        # 국내 주식 데이터 처리
        rt_data_domestic = kb.get_inquire_balance_lst()
        required_columns_domestic = ['hldg_qty', 'fltt_rt', 'pchs_amt', 'evlu_pfls_rt', 'evlu_pfls_amt', 'prdt_name', 'prpr',
                                     'pchs_avg_pric', 'pdno']
        filtered_data_domestic = rt_data_domestic[required_columns_domestic]

        merge_sql_domestic = """
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

        for index, row in filtered_data_domestic.iterrows():
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
            cursor.execute(merge_sql_domestic, data)

        # 기존 해외 주식 데이터 삭제
        delete_sql_overseas = "DELETE FROM SECURITIES_OVERSEAS_BALANCE WHERE ACCOUNT_ID = :account_id"
        cursor.execute(delete_sql_overseas, {'account_id': '종합위탁'})
        print("Existing overseas data deleted successfully")

        # 해외 주식 데이터 처리
        rt_data_overseas = ko.get_overseas_inquire_balance_lst()
        account_id = "종합위탁"
        required_columns_overseas = [
            'ovrs_pdno', 'ovrs_item_name', 'frcr_evlu_pfls_amt', 'evlu_pfls_rt',
            'pchs_avg_pric', 'ovrs_stck_evlu_amt', 'frcr_pchs_amt1', 'now_pric2', 'ovrs_cblc_qty'
        ]
        filtered_data_overseas = rt_data_overseas[required_columns_overseas]

        merge_sql_overseas = """
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

        for index, row in filtered_data_overseas.iterrows():
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
            cursor.execute(merge_sql_overseas, data)

        # 커밋
        conn.commit()
        cursor.close()

        return jsonify({"status": "success", "message": "Both domestic and overseas data updated successfully"}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/domestic/order', methods=['POST'])
def place_domestic_order():
    try:
        # 요청 데이터 파싱
        data = request.json
        ord_dv = data.get('ord_dv', '')
        itm_no = data.get('itm_no', '')
        qty = data.get('qty', 0)
        unpr = data.get('unpr', 0)

        # 주식 주문 실행
        order_result = kb.get_order_cash(
            ord_dv=ord_dv,
            itm_no=itm_no,
            qty=qty,
            unpr=unpr
        )

        # 주문 결과 확인
        if order_result is not None:
            return jsonify({"status": "success", "data": order_result.to_dict(orient='records')})
        else:
            return jsonify({"status": "error", "message": "Order failed"}), 400

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# 해외 주식 주문 라우트
@app.route('/overseas/order', methods=['POST'])
def place_overseas_order():
    try:
        # 요청 데이터 파싱
        data = request.json

        ord_dv = data.get('ord_dv', '')
        excg_cd = data.get('excg_cd', 'NASD')  # 거래소 코드
        itm_no = data.get('itm_no', '')
        qty = data.get('qty', 0)
        unpr = data.get('unpr', 0)
        # print(f"Received exchange code: {excg_cd}")
        # 해외 주식 주문 실행
        order_result = ko.get_overseas_order(
            ord_dv=ord_dv,
            excg_cd=excg_cd,
            itm_no=itm_no,
            qty=qty,
            unpr=unpr
        )

        # 주문 결과 확인
        if order_result is not None:
            return jsonify({"status": "success", "data": order_result.to_dict(orient='records')})
        else:
            return jsonify({"status": "error", "message": "Order failed"}), 400

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/update_domestic_balance_summary', methods=['POST'])
def update_domestic_balance_summary():
    global global_tot_evlu_amt  # 전역 변수 사용

    try:
        cursor = conn.cursor()

        # 국내 주식 잔고 요약 데이터 처리
        rt_data_domestic = kb.get_inquire_balance_obj()
        account_id = "종합위탁"  # 실제 계좌 ID로 대체하세요.


        # 국내 주식 데이터를 가져와서 필요한 필드만 선택
        tot_evlu_amt = rt_data_domestic['tot_evlu_amt'].iloc[0] if 'tot_evlu_amt' in rt_data_domestic else '0'
        global_tot_evlu_amt = tot_evlu_amt
        pchs_amt_smtl_amt = rt_data_domestic['pchs_amt_smtl_amt'].iloc[
            0] if 'pchs_amt_smtl_amt' in rt_data_domestic else '0'
        evlu_amt_smtl_amt = rt_data_domestic['evlu_amt_smtl_amt'].iloc[
            0] if 'evlu_amt_smtl_amt' in rt_data_domestic else '0'
        evlu_pfls_smtl_amt = rt_data_domestic['evlu_pfls_smtl_amt'].iloc[
            0] if 'evlu_pfls_smtl_amt' in rt_data_domestic else '0'
        dnca_tot_amt = rt_data_domestic['dnca_tot_amt'].iloc[0] if 'dnca_tot_amt' in rt_data_domestic else '0'
        nxdy_excc_amt = rt_data_domestic['nxdy_excc_amt'].iloc[0] if 'nxdy_excc_amt' in rt_data_domestic else '0'
        prvs_rcdl_excc_amt = rt_data_domestic['prvs_rcdl_excc_amt'].iloc[
            0] if 'prvs_rcdl_excc_amt' in rt_data_domestic else '0'

        # 국내 주식 데이터를 삽입/업데이트하는 MERGE SQL문
        sql_domestic = """
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

        # SQL문 실행
        cursor.execute(sql_domestic, account_id=account_id, tot_evlu_amt=tot_evlu_amt,
                       pchs_amt_smtl_amt=pchs_amt_smtl_amt, evlu_amt_smtl_amt=evlu_amt_smtl_amt,
                       evlu_pfls_smtl_amt=evlu_pfls_smtl_amt, dnca_tot_amt=dnca_tot_amt,
                       nxdy_excc_amt=nxdy_excc_amt, prvs_rcdl_excc_amt=prvs_rcdl_excc_amt)
        # accountTotal을 계산하여 SECURITIES_HANA_ACCOUNTS 테이블의 CASH 열에 업데이트
        # 커밋
        conn.commit()
        cursor.close()

        return jsonify({"status": "success", "message": "Domestic balance summary updated successfully"}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/update_overseas_balance_summary', methods=['POST'])
def update_overseas_balance_summary():
    global global_tot_asst_amt  # 전역 변수 사용
    try:
        cursor = conn.cursor()

        # 해외 주식 잔고 요약 데이터 처리
        rt_data_overseas = ko.get_overseas_inquire_balance()
        rt_data_present_balance = ko.get_overseas_inquire_present_balance()
        account_id = "종합위탁"  # 실제 계좌 ID로 대체하세요.

        # 해외 주식 데이터를 가져와서 필요한 필드만 선택
        ovrs_tot_pfls = rt_data_overseas['ovrs_tot_pfls'].iloc[0] if 'ovrs_tot_pfls' in rt_data_overseas else '0'
        tot_pftrt = rt_data_overseas['tot_pftrt'].iloc[0] if 'tot_pftrt' in rt_data_overseas else '0'
        frcr_buy_amt_smtl1 = rt_data_overseas['frcr_buy_amt_smtl1'].iloc[0] if 'frcr_buy_amt_smtl1' in rt_data_overseas else '0'
        tot_asst_amt = rt_data_present_balance['tot_asst_amt'].iloc[0] if 'tot_asst_amt' in rt_data_present_balance else '0'
        global_tot_asst_amt = tot_asst_amt  # 전역 변수 업데이트

        evlu_amt_smtl = rt_data_present_balance['evlu_amt_smtl'].iloc[0] if 'evlu_amt_smtl' in rt_data_present_balance else '0'

        # 해외 주식 데이터를 삽입/업데이트하는 MERGE SQL문
        sql_overseas = """
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

        # SQL문 실행
        cursor.execute(sql_overseas, account_id=account_id, ovrs_tot_pfls=ovrs_tot_pfls,
                       tot_pftrt=tot_pftrt, frcr_buy_amt_smtl1=frcr_buy_amt_smtl1,
                       tot_asst_amt=tot_asst_amt, evlu_amt_smtl=evlu_amt_smtl)
        # accountTotal을 계산하여 SECURITIES_HANA_ACCOUNTS 테이블의 CASH 열에 업데이트
        account_total = float(global_tot_evlu_amt) + float(global_tot_asst_amt)
        update_cash_total(cursor, account_id, account_total)
        # 커밋
        conn.commit()
        cursor.close()

        return jsonify({"status": "success", "message": "Overseas balance summary updated successfully"}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

def update_cash_total(cursor, account_id, balance_amt):
    """
    SECURITIES_HANA_ACCOUNTS 테이블의 CASH 열을 업데이트하는 함수
    :param cursor: Oracle 커서 객체
    :param account_id: 계좌 ID
    :param balance_amt: 업데이트할 총액 (tot_evlu_amt 또는 tot_asst_amt)
    """
    sql_update_cash = """
    UPDATE SECURITIES_HANA_ACCOUNTS
    SET CASH = :balance_amt
    WHERE ACCOUNT_ID = :account_id
    """
    cursor.execute(sql_update_cash, balance_amt=balance_amt, account_id=account_id)

@app.route('/update_contracts', methods=['POST'])
def update_contracts():
    try:
        cursor = conn.cursor()

        # 1. 일반 계약 데이터 처리
        rt_data_contract = kb.get_inquire_daily_ccld_lst(inqr_strt_dt="20240620", ccld_dvsn="01")
        required_columns_contract = [
            'ord_dt', 'ord_tmd', 'odno', 'sll_buy_dvsn_cd_name', 'prdt_name', 'ord_qty',
            'tot_ccld_qty', 'tot_ccld_amt', 'pdno', 'ord_unpr', 'avg_prvs', 'ccld_cndt_name'
        ]
        filtered_data_contract = rt_data_contract[required_columns_contract]
        filtered_data_contract['ord_dt'] = filtered_data_contract['ord_dt'].apply(lambda x: datetime.strptime(x, '%Y%m%d').strftime('%Y-%m-%d'))

        merge_sql_contract = """
            MERGE INTO SECURITIES_DOMESTIC_CONTRACT target
            USING DUAL
            ON (target.ODNO = :odno)
            WHEN MATCHED THEN
                UPDATE SET
                    ORD_DT = TO_DATE(:ord_dt, 'YYYY-MM-DD'),
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

        for index, row in filtered_data_contract.iterrows():
            data = {
                'ord_dt': row['ord_dt'],
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
            cursor.execute(merge_sql_contract, data)

        # 2. 취소된 계약 데이터 처리
        rt_data_cancel_contract = kb.get_inquire_daily_ccld_lst(inqr_strt_dt="20240620", ccld_dvsn="02")
        required_columns_cancel_contract = [
            'ord_dt', 'ord_tmd', 'odno', 'sll_buy_dvsn_cd_name', 'prdt_name', 'ord_qty',
            'tot_ccld_qty', 'tot_ccld_amt', 'pdno', 'ord_unpr', 'avg_prvs', 'ccld_cndt_name'
        ]
        filtered_data_cancel_contract = rt_data_cancel_contract[required_columns_cancel_contract]
        filtered_data_cancel_contract['ord_dt'] = filtered_data_cancel_contract['ord_dt'].apply(lambda x: datetime.strptime(x, '%Y%m%d').strftime('%Y-%m-%d'))

        merge_sql_cancel_contract = """
            MERGE INTO SECURITIES_DOMESTIC_CANCELCONTRACT target
            USING DUAL
            ON (target.ODNO = :odno)
            WHEN MATCHED THEN
                UPDATE SET
                    ORD_DT = TO_DATE(:ord_dt, 'YYYY-MM-DD'),
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

        for index, row in filtered_data_cancel_contract.iterrows():
            data = {
                'ord_dt': row['ord_dt'],
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
            cursor.execute(merge_sql_cancel_contract, data)

        # 커밋 및 연결 종료
        conn.commit()
        cursor.close()

        return jsonify({"status": "success", "message": "Contracts and cancel contracts updated successfully"}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/update_overseas_contract', methods=['POST'])
def update_overseas_contract():
    try:
        cursor = conn.cursor()

        # 데이터를 받아옵니다.
        rt_data5 = ko.get_overseas_inquire_ccnl(st_dt="20240620", ccld_dv="00")

        # 필요한 열만 선택: 해외 체결 내역 데이터
        required_columns2 = [
            'ord_dt', 'ord_tmd', 'odno', 'sll_buy_dvsn_cd_name', 'prdt_name',
            'ft_ord_qty', 'ft_ccld_qty', 'ft_ccld_amt3', 'pdno', 'ft_ord_unpr3', 'tr_crcy_cd'
        ]
        filtered_data2 = rt_data5[required_columns2]

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

                # MERGE SQL문 실행
                cursor.execute(merge_sql, data)

            except oracledb.DatabaseError as e:
                print(f"Error inserting/updating row {index}: {e}")
                continue

        # 커밋 및 연결 종료
        conn.commit()
        cursor.close()

        return jsonify({"status": "success", "message": "Overseas contracts updated successfully"}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500



if __name__ == '__main__':
    app.run()
