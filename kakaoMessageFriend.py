import json
import requests

# 토큰 로드
with open(r"/Users/240729after다운로드/kakao_code_friend2.json", "r") as fp:
    tokens = json.load(fp)

# 헤더 설정
headers = {
    "Authorization": "Bearer " + tokens["access_token"]
}

# 친구 목록 가져오기
url = "https://kapi.kakao.com/v1/api/talk/friends"
result = requests.get(url, headers=headers).json()
friends_list = result.get("elements")
print("친구 목록:", friends_list)

if friends_list:
    # 첫 번째 친구의 UUID 가져오기
    friend_id = friends_list[0].get("uuid")
    print("첫 번째 친구의 UUID:", friend_id)

    # 템플릿 메시지 보내기 (list 형식)
    url = "https://kapi.kakao.com/v1/api/talk/friends/message/default/send"
    template_object = {
        "object_type": "list",
        "header_title": "주식매매 알림",
        "header_link": {
            "web_url": "https://www.naver.com",
            "mobile_web_url": "https://m.naver.com"
        },
        "contents": [
            {
                "title": "주식매수",
                "description": "매수 확인",
                "image_url": "https://via.placeholder.com/800x400.png?text=Item+1",  # 예제 이미지 URL
                "link": {
                    "web_url": "https://www.naver.com",
                    "mobile_web_url": "https://m.naver.com"
                }
            },
            {
                "title": "주식매도",
                "description": "매도 확인",
                "image_url": "https://github.com/Imdynasty/HanaCT-KoreaLogo/blob/main/%E1%84%82%E1%85%A9%E1%86%BC%E1%84%92%E1%85%A7%E1%86%B8.png",  # 예제 이미지 URL
                "link": {
                    "web_url": "https://www.google.com",
                    "mobile_web_url": "https://m.google.com"
                }
            }
        ],
        "buttons": [
            {
                "title": "전체보기",
                "link": {
                    "web_url": "https://www.naver.com",
                    "mobile_web_url": "https://m.naver.com"
                }
            }
        ]
    }

    data = {
        'receiver_uuids': json.dumps([friend_id]),  # UUID를 리스트 형식으로 전달
        "template_object": json.dumps(template_object)
    }

    response = requests.post(url, headers=headers, data=data)

    # 결과 확인
    response_data = response.json()
    if response.status_code == 200 and 'successful_receiver_uuids' in response_data:
        print(f"친구에게 템플릿 메시지 전송 성공. UUID: {response_data['successful_receiver_uuids']}")
    else:
        print('친구에게 템플릿 메시지 전송 실패. Error: ' + str(response_data))
else:
    print("메시지를 보낼 친구가 없습니다.")
