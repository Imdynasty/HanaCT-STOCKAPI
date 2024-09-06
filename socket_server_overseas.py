import json

import eventlet

eventlet.monkey_patch()  # 이 부분을 최상단에 둡니다.

from flask import Flask
from flask_socketio import SocketIO, emit
import kis_auth_real as ka
import kis_ovrseastk_real as ko
from threading import Event, Thread, Lock

# KIS 인증
ka.auth()

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# 전역 변수로 stop_event와 thread 관리
stop_event = Event()
current_thread = None
thread_lock = Lock()


@socketio.on('connect')
def handle_connect():
    print('Client connected')


@socketio.on('disconnect')
def handle_disconnect():
    global current_thread
    print('Client disconnected')
    stop_event.set()  # 클라이언트 연결 해제 시 작업 중단
    if current_thread is not None:
        current_thread.join()  # 스레드가 완전히 종료될 때까지 대기


@socketio.on('request_orderbook')
def handle_orderbook_request(data):
    global stop_event, current_thread

    with thread_lock:
        if current_thread is not None:
            stop_event.set()  # 기존 작업 중단 요청
            current_thread.join()  # 기존 스레드가 종료될 때까지 대기
            stop_event.clear()  # 새로운 작업 시작을 위해 이벤트 리셋

        stock_code = data.get('code')
        excd = 'NAS'
        div = '02'

        if not stock_code or not excd or not div:
            emit('error', {'message': 'Stock code, exchange code, and div are required'})
            return

        print(f"Fetching data for stock code: {stock_code}, exchange code: {excd}, div: {div}")

        # 새로운 작업을 시작
        current_thread = Thread(target=fetch_orderbook_data, args=(stock_code, excd, div))
        current_thread.start()


def fetch_orderbook_data(stock_code, excd, div):
    while not stop_event.is_set():
        try:
            # 데이터를 받아옴
            ask_data = ko.get_overseas_price_inquire_asking_price(itm_no=stock_code, excd=excd, div=div)

            # 받아온 데이터를 콘솔에 출력
            # print("Received DataFrame:")
            # print(ask_data)

            # 데이터를 딕셔너리로 변환하여 출력
            ask_data_dict = ask_data.to_dict(orient='records')
            print("Data converted to dict:")
            print(ask_data_dict)

            # 데이터를 클라이언트로 전송
            if ask_data_dict:  # 데이터가 존재하는 경우
                socketio.emit('orderbook_update', ask_data_dict[0])  # 첫 번째 레코드를 전송
            else:
                socketio.emit('orderbook_update', {})
        except Exception as e:
            print(f"Error fetching data: {str(e)}")
            socketio.emit('error', {'message': f"Error fetching data: {str(e)}"})
        socketio.sleep(1)  # 1초마다 데이터 갱신





if __name__ == '__main__':
    # socketio.run()을 사용하여 Eventlet 또는 Gevent와 함께 실행
    socketio.run(app, port=5001)
