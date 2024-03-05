import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from .al_requests_calculator import call_counter

@call_counter
def post_product(token: str, product_json: dict) -> requests.Request:
    """post product to allegro api

    Args:
        token (str): access token
        product_json (dict): product data in json type

    Raises:
        SystemExit: Request to exit from the interpreter.

    Returns:
        requests.Request: responce from allegro api
    """
    
    # product_json = str(product_json)
    try:
        url = "https://api.allegro.pl/sale/product-offers"
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36','Authorization': 'Bearer ' + token, 'Accept': "application/vnd.allegro.public.v1+json", 'Content-type':'application/vnd.allegro.public.v1+json', "Connection": "close"}
        main_categories_result = requests.post(url,json=product_json, headers=headers, verify=False)
        return main_categories_result
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)

@call_counter   
def patch_offer(token: str, offer_id: str,data: dict) -> dict:
    """putch product to allegro api

    Args:
        token (str): access token
        product_json (dict): product data in json type

    Raises:
        SystemExit: Request to exit from the interpreter.

    Returns:
        requests.Request: responce from allegro api
    """
    try:
        url = f"https://api.allegro.pl/sale/product-offers/{offer_id}"
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36','Authorization': 'Bearer ' + token, 'Accept': "application/vnd.allegro.public.v1+json", 'Content-type':'application/vnd.allegro.public.v1+json', "Connection": "close"}
        return requests.patch(url,json = data, headers=headers, verify=False,timeout=10)
        # return categories_result
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err) 