from flask import Flask, request, jsonify
from flask_cors import CORS  # CORS 패키지 임포트
import kis_auth as ka
import kis_domstk as kb

# 플라스크 애플리케이션 초기화
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["http://localhost:3000"]}})
# KIS API 인증
ka.auth()

@app.route('/domestic/order', methods=['POST'])
def place_order():
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
            print(order_result);
        else:
            return jsonify({"status": "error", "message": "Order failed"}), 400

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
