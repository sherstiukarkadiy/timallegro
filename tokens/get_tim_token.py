import json
import requests
import time

url = "https://www.tim.pl/graphql"
tim_token_path = "/Users/macair/Desktop/Inglobus/Ready allegro.pl/tokens/tim_token.json"


def get_tim_token():
    with open(tim_token_path,"r") as file:
        info = json.load(file)
    token = info["token"]
    token_time = info["time"]
    
    if (time.time() - token_time) >= 1750:
        token = request_token()
        token_time = time.time()
        with open(tim_token_path,"w") as file:
            data = {"token": token, "time": token_time}
            json.dump(data,file,indent=4,ensure_ascii=False)
        time.sleep(0.5)

    return token
   

def request_token(): 
    global token_time
    schema = """
    mutation{
    login (apiUsername:"inglobus@googlemail.com"
    apiPassword:"OPIfMgLGq6PNSKAHT0qebzSJWZ7saq4l"
    webUsername:"inglobus@googlemail.com"
    webPassword:"ge%gHJYdW1@")
    {token}}
    """
    response = requests.post(url=url, json={"query": schema})
    return response.json()["data"]["login"]["token"]

if __name__ == "__main__":
    token = get_tim_token()
    print(token)
    # token_time = time.time()
    # with open(tim_token_path,"w") as file:
    #     data = {"token": token, "time": token_time}
    #     json.dump(data,file,indent=4,ensure_ascii=False)
    # ended by allegro
    