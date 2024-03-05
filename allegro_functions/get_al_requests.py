from functools import lru_cache
import sys
import requests
from .al_requests_calculator import call_counter
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

@call_counter
def get_tax_rates(token: str,category_id: str) -> dict:
    """Get tax rates from allegro api

    Args:
        token (str): access token 
        category_id (str): allegro category id

    Raises:
        SystemExit: Request to exit from the interpreter.

    Returns:
        dict: Tax rates dictionary from allegro api
    """
    
    try:
        url = f"https://api.allegro.pl/sale/tax-settings?category.id={category_id}"
        headers = {'Authorization': 'Bearer ' + token, 'Accept': "application/vnd.allegro.public.v1+json", "Connection": "close"}
        categories_result = requests.get(url, headers=headers, verify=False)
        categories = categories_result.json()
        return categories
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)

@call_counter
def get_impliedWarranty(token: str) -> dict:
    """Get list of user implied warranties

    Args:
        token (str): access token 

    Raises:
        SystemExit: Request to exit from the interpreter.

    Returns:
        dict: implied warranties dictionary from allegro api
    """
    
    try:
        url = f"https://api.allegro.pl/after-sales-service-conditions/implied-warranties"
        headers = {'Authorization': 'Bearer ' + token, 'Accept': "application/vnd.allegro.public.v1+json", "Connection": "close"}
        categories_result = requests.get(url, headers=headers, verify=False)
        categories = categories_result.json()
        return categories
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)

@call_counter  
def get_returnPolicy(token: str) -> dict:
    """Get list of user return policies

    Args:
        token (str): access token 

    Raises:
        SystemExit: Request to exit from the interpreter.

    Returns:
        dict: return policies dictionary from allegro api
    """
    
    try:
        url = f"https://api.allegro.pl/after-sales-service-conditions/return-policies"
        headers = {'Authorization': 'Bearer ' + token, 'Accept': "application/vnd.allegro.public.v1+json", "Connection": "close"}
        categories_result = requests.get(url, headers=headers, verify=False)
        categories = categories_result.json()
        return categories
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)

@call_counter  
def get_warranty(token: str) -> dict:
    """Get list of user waranties

    Args:
        token (str): access token 

    Raises:
        SystemExit: Request to exit from the interpreter.

    Returns:
        dict: waranties dictionary from allegro api
    """
    
    try:
        url = f"https://api.allegro.pl/after-sales-service-conditions/warranties"
        headers = {'Authorization': 'Bearer ' + token, 'Accept': "application/vnd.allegro.public.v1+json", "Connection": "close"}
        categories_result = requests.get(url, headers=headers, verify=False)
        categories = categories_result.json()
        return categories
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)

@call_counter
@lru_cache(maxsize=None)
def get_parameters(token: str, category_id: str) -> dict:
    """Get parameters information from allegro api

    Args:
        token (str): access token 
        category_id (str): allegro category id

    Raises:
        SystemExit: Request to exit from the interpreter.

    Returns:
        dict: parameters dictionary from allegro api
    """
    try:
        url = f"https://api.allegro.pl/sale/categories/{category_id}/parameters"
        headers = {'Authorization': 'Bearer ' + token, 'Accept': "application/vnd.allegro.public.v1+json", "Connection": "close"}
        categories_result = requests.get(url, headers=headers, verify=False)
        categories = categories_result.json()
        return categories
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)

@call_counter   
def get_operation_info(token,url):
    try:
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=1)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        headers = {'Authorization': 'Bearer ' + token, 'Accept': "application/vnd.allegro.public.v1+json", "Connection": "close"}
        operation = session.get(url, headers=headers, verify=False, timeout=10)
        return operation
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)

@call_counter   
def get_product(token,url):
    try:
        headers = {'Authorization': 'Bearer ' + token, 'Accept': "application/vnd.allegro.public.v1+json", "Connection": "close"}
        operation = requests.get(url, headers=headers, verify=False)
        return operation
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)
    
@call_counter
@lru_cache(maxsize=None)
def get_cat_info(token: str, category_id: str) -> dict:
    try:
        url = f"https://api.allegro.pl/sale/categories/{category_id}"
        headers = {'Authorization': 'Bearer ' + token, 'Accept': "application/vnd.allegro.public.v1+json", "Connection": "close"}
        categories_result = requests.get(url, headers=headers, verify=False)
        categories = categories_result.json()
        return categories
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)

@call_counter   
def delete_offer(token: str, offer_id: str) -> dict:
    try:
        url = f"https://api.allegro.pl/sale/offers/{offer_id}"
        headers = {'Authorization': 'Bearer ' + token, 'Accept': "application/vnd.allegro.public.v1+json", "Connection": "close"}
        categories_result = requests.delete(url, headers=headers, verify=False)
        categories = categories_result
        return categories
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)
    

def get_marketplaces(token: str) -> dict:
    """Get parameters information from allegro api

    Args:
        token (str): access token 
        category_id (str): allegro category id

    Raises:
        SystemExit: Request to exit from the interpreter.

    Returns:
        dict: parameters dictionary from allegro api
    """
    try:
        url = f"https://api.allegro.pl/marketplaces"
        headers = {'Authorization': 'Bearer ' + token, 'Accept': "application/vnd.allegro.public.v1+json", "Connection": "close"}
        categories_result = requests.get(url, headers=headers, verify=False)
        categories = categories_result.json()
        return categories
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)