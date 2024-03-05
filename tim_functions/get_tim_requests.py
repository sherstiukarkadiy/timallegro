from datetime import datetime
import time
import requests
def download_products_file(token):
    products_file = "/Users/macair/Desktop/Inglobus/Ready allegro.pl/databases/tim/new_products.csv"
    schema = """
        query{
        productsFeed{products_feed_file_url
        }
        }"""
    
    header = {"X-Api-Token": f"{token}", "Connection": "close"}
    status = 0
    while status != 200:
        responce = requests.post(url="https://www.tim.pl/graphql", json={"query": schema}, headers= header)
        status = responce.status_code
        if status != 200:
            time.sleep(1)
    
    url = responce.json()["data"]["productsFeed"]["products_feed_file_url"]
    r = requests.get(url, allow_redirects=True)
    content = r.content.decode("utf-8").replace(";",",")
    open(products_file,"w").write(content)
    
    return products_file

def download_invertory_file(token):
        inventory_file = "/Users/macair/Desktop/Inglobus/Ready allegro.pl/databases/tim/inventory.csv"
        schema = """
            query{
            inventory{stock_file_url}
            }"""
        
        header = {"X-Api-Token": f"{token}", "Connection": "close"}
        status = 0
        while status != 200:
            responce = requests.post(url="https://www.tim.pl/graphql", json={"query": schema}, headers= header)
            status = responce.status_code
            if status != 200:
                time.sleep(1)
                
        if responce.status_code == 200:
            url = responce.json()["data"]["inventory"]["stock_file_url"]
            r = requests.get(url, allow_redirects=True)
            content = r.content.decode("utf-8").replace(";",",")
            open(inventory_file,"w").write(content)
        
        return inventory_file

def download_prices_file(token):
    prices_file = "/Users/macair/Desktop/Inglobus/Ready allegro.pl/databases/tim/new_prices.csv"
    schema = """
        query{
        priceList{link}
        }
    """
    
    header = {"X-Api-Token": f"{token}", "Connection": "close"}
    status = 0
    while status != 200:
        responce = requests.post(url="https://www.tim.pl/graphql", json={"query": schema}, headers= header)
        status = responce.status_code
        if status != 200:
            time.sleep(1)
        
    
    url = responce.json()["data"]["priceList"]["link"]
    r = requests.get(url, allow_redirects=True)
    content = r.content.decode("utf-8").replace(";",",")
    open(prices_file,"w").write(content)
    
    return prices_file

def get_product_info(token, product_id):
    
    schema = """
    query{
    product(id:""" +f'"{product_id}"' + """){
    _id
    ean
    name
    price{
    value
    label}
    can_be_returned
    stock{
    qty
    unit}
    series{name}
    ref_num
    manufacturer{name}
    main_category{name
    subcategories{name
    subcategories{name}}}
    attributes_block{
    label
    value}
    is_wire
    is_vendor
    shipping{
    type
    time}
    series_block{sku}
    crossells_block{sku}
    upsells_block{sku}
    accessories_block{sku}
    default_image
    available
    package_size}}
    """

    header = {"X-Api-Token": f"{token}", "Connection": "close"}
    response = requests.post(url="https://www.tim.pl/graphql", json={"query": schema}, headers= header)
    if response.status_code == 200:
        return response.json()["data"]["product"]

if __name__ == "__main__":
    download_prices_file("a03a253e2fa705e55b7018307f3b7e31")