import json
import requests

# 토큰 로드

def send_kakao_message(contracts):
    with open(r"/Users/240729after다운로드/kakao_code_friend2.json", "r") as fp:
        tokens = json.load(fp)

    headers = {
        "Authorization": "Bearer " + tokens["access_token"],
        "Content-Type": "application/x-www-form-urlencoded"
    }

    url = "https://kapi.kakao.com/v1/api/talk/friends"
    result = requests.get(url, headers=headers).json()
    friends_list = result.get("elements")
    print("친구 목록:", friends_list)

    if friends_list:
        friend_id = friends_list[0].get("uuid")
        print("첫 번째 친구의 UUID:", friend_id)
        receiver_uuids = [friend_id]
        url = "https://kapi.kakao.com/v1/api/talk/friends/message/default/send"

        contents = []
        for _, row in contracts.iterrows():
            sll_buy_dvsn_cd_name = row['sll_buy_dvsn_cd_name']
            prdt_name = row['prdt_name']
            tot_ccld_qty = row['tot_ccld_qty']
            tot_ccld_amt = row['tot_ccld_amt']
            ord_dt = row['ord_dt']
            contents.append({
                "title": f"{sll_buy_dvsn_cd_name} -> {prdt_name}",
                "description": f"수량: {tot_ccld_qty}, 금액: {tot_ccld_amt}, 날짜: {ord_dt}",
                "link": {
                    "web_url": "https://www.naver.com",
                    "mobile_web_url": "https://m.naver.com"
                }
            })

        template_object = {
            "object_type": "feed",  # 'list' 대신 'feed' 타입으로 시도
            "content": {
                "title": "전량체결통보 안내",
                "description": "주문 체결 내역을 확인하세요.",
                "image_url": "https://example.com/your_image.jpg",  # 필수는 아니지만 이미지가 있으면 추가
                "link": {
                    "web_url": "https://www.naver.com",
                    "mobile_web_url": "https://m.naver.com"
                },
                "buttons": [
                    {
                        "title": "자세히 보기",
                        "link": {
                            "web_url": "https://www.naver.com",
                            "mobile_web_url": "https://m.naver.com"
                        }
                    }
                ]
            },
            "social": {
                "like_count": 100,
                "comment_count": 200
            }
        }

        data = {
            'receiver_uuids': json.dumps(receiver_uuids),
            "template_object": json.dumps(template_object, ensure_ascii=False)
        }

        response = requests.post(url, headers=headers, data=data)

        response_data = response.json()
        if response.status_code == 200 and 'successful_receiver_uuids' in response_data:
            print(f"친구에게 템플릿 메시지 전송 성공. UUID: {response_data['successful_receiver_uuids']}")
        else:
            print('친구에게 템플릿 메시지 전송 실패. Error: ' + str(response_data))
    else:
        print("메시지를 보낼 친구가 없습니다.")
