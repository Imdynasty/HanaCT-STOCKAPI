from flask import Flask, jsonify
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

# 주문 데이터 처리 라우트
@app.route('/update_balance', methods=['POST'])
def update_balance():
    try:
        cursor = conn.cursor()

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

# 애플리케이션 실행
if __name__ == '__main__':
    app.run(debug=True)
