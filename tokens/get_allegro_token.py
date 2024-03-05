import base64
import hashlib
import json
import secrets
import string
import time
import requests
from pathlib import Path

CLIENT_ID = "1292761fda044d57b2bd1d28b1a4a316"          # enter the Client_ID of the application
CLIENT_SECRET = "Xn1nI63fjW9dWI7gE8xf6FssolgRRgkVknqBkdMUsHLEXsSbVnPEcqFVCJKOGpa4"     # wprowadź Client_Secret aplikacji
REDIRECT_URI = "http://localhost:8000"       # wprowadź redirect_uri
AUTH_URL = "https://allegro.pl/auth/oauth/authorize"
TOKEN_URL = "https://allegro.pl/auth/oauth/token"
TOKEN_PATH = Path(__file__).parent.joinpath("allegro_token.json")


def generate_code_verifier():
    code_verifier = ''.join((secrets.choice(string.ascii_letters) for i in range(40)))
    return code_verifier


def generate_code_challenge(code_verifier):
    hashed = hashlib.sha256(code_verifier.encode('utf-8')).digest()
    base64_encoded = base64.urlsafe_b64encode(hashed).decode('utf-8')
    code_challenge = base64_encoded.replace('=', '')
    return code_challenge


def get_authorization_code(code_verifier):
    code_challenge = generate_code_challenge(code_verifier)
    authorization_redirect_url = f"{AUTH_URL}?response_type=code&client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}" \
                                 f"&code_challenge_method=S256&code_challenge={code_challenge}"
    print("Zaloguj do Allegro - skorzystaj z url w swojej przeglądarce oraz wprowadź authorization code ze zwróconego url: ")
    print(f"--- {authorization_redirect_url} ---")
    authorization_code = input('code: ')
    return authorization_code


def get_token_info(authorization_code, code_verifier):
    try:
        data = {'grant_type': 'authorization_code', 'code': authorization_code,
                'redirect_uri': REDIRECT_URI, 'code_verifier': code_verifier}
        access_token_response = requests.post(TOKEN_URL, data=data, verify=False,
                                              allow_redirects=False)
        response_body = json.loads(access_token_response.text)
        return response_body
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)
    
def get_new_token(token):
    try:
        data = {'grant_type': 'refresh_token', 'refresh_token': token,'redirect_uri': REDIRECT_URI}
        access_token_response = requests.post(TOKEN_URL, data=data, verify=False,
                                              allow_redirects=False, auth=(CLIENT_ID, CLIENT_SECRET))
        tokens = json.loads(access_token_response.text)
        return tokens
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)

def get_responce_code(token):
    try:
        url = "https://api.allegro.pl/sale/categories"
        headers = {'Authorization': 'Bearer ' + token, 'Accept': "application/vnd.allegro.public.v1+json", "Connection": "close"}
        categories_result = requests.get(url, headers=headers, verify=False)
        return categories_result.status_code
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)
    
def get_access_token():
    with open(TOKEN_PATH,"r") as file:
        token_info = json.load(file)
        
    access_token = token_info["access_token"]
    token_time = token_info["time"]
    if (time.time() - token_time) < (23 * 1800):
        return access_token
    
    refresh_token = token_info["refresh_token"]
    new_token_info = get_new_token(refresh_token)
    new_token_info["time"] = time.time()
    
    with open(TOKEN_PATH,"w") as file:
        json.dump(new_token_info,file,indent=4,ensure_ascii=False)
        
    return new_token_info["access_token"]

def main():
    code_verifier = generate_code_verifier()
    authorization_code = get_authorization_code(code_verifier)
    tokens = get_token_info(authorization_code, code_verifier)
    tokens["time"] = time.time()
    with open(TOKEN_PATH,"w") as file:
        json.dump(tokens,file,indent=4,ensure_ascii=False)

if __name__ == "__main__":
    print(get_access_token())