import eventlet
eventlet.monkey_patch()  # 이 부분을 최상단에 둡니다.

from flask import Flask
from flask_socketio import SocketIO, emit
import kis_auth as ka
import kis_domstk as kb
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
        if not stock_code:
            emit('error', {'message': 'Stock code is required'})
            return

        print(f"Fetching data for stock code: {stock_code}")

        # 새로운 작업을 시작
        current_thread = Thread(target=fetch_orderbook_data, args=(stock_code,))
        current_thread.start()

def fetch_orderbook_data(stock_code):
    while not stop_event.is_set():
        ask_data = kb.get_inquire_asking_price_exp_ccn(itm_no=stock_code)
        ask_data_dict = ask_data.to_dict(orient='records')
        print(ask_data_dict)
        socketio.emit('orderbook_update', ask_data_dict)
        socketio.sleep(1)  # 1초마다 데이터 갱신

if __name__ == '__main__':
    # socketio.run()을 사용하여 Eventlet 또는 Gevent와 함께 실행
    socketio.run(app, port=5002)
