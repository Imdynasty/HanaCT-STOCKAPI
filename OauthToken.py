import requests

url = 'https://kauth.kakao.com/oauth/token'
rest_api_key = 'bae3a57a56671d99d18bb00a44a68829'
redirect_uri = 'https://example.com/oauth'
authorize_code = 'nlDgKhotSww252Xqh4X1wSMuMZMZk8G1W-AvldfwDTD9lRO_XZ_NswAAAAQKPXLqAAABkbaIRHrSDh85zpcCzQ'

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
with open(r"/Users/240729after다운로드/kakao_code.json","w") as fp:
    json.dump(tokens, fp)