import requests

url = 'https://kauth.kakao.com/oauth/token'
rest_api_key = 'd63a5b8143145db14ba09ae43f903efc'
redirect_uri = 'https://example.com/oauth'
authorize_code = 'IIZPaHvrayOKOX_JW8xgh8Bh4TqOrk2j2aFW9YJmKAfA9HTVuMnrBQAAAAQKKclfAAABkftKnLHmTYKY7N6ACw' # 친구에게 보내기 받은 인증 코드

data = {
    'grant_type':'authorization_code',
    'client_id':rest_api_key,
    'redirect_uri':redirect_uri,
    'code': authorize_code,
    }

response = requests.post(url, data=data)
tokens = response.json()
print(tokens)

# json 저장
import json
#1.
with open(r"/Users/240729after다운로드/kakao_code_friend2.json","w") as fp:
    json.dump(tokens, fp)