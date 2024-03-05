import csv
import json
import time
from tim_functions.get_tim_requests import download_invertory_file, download_prices_file, download_products_file

from tokens.get_tim_token import get_tim_token

RESULTS = "/Users/macair/Desktop/Inglobus/Ready allegro.pl/databases/results.csv"
AFTPR_DT = "/Users/macair/Desktop/Inglobus/Ready allegro.pl/databases/after_process.csv"
READY_DT = "/Users/macair/Desktop/Inglobus/Ready allegro.pl/databases/ready_offers.csv"
SORTED_DT = "/Users/macair/Desktop/Inglobus/Ready allegro.pl/databases/sorted_database.csv"

def find_EAN(some_dict,new_dict):
    for k,el in some_dict.items():
        if k in new_dict:
            el["EAN"] = new_dict[k]["EAN"]
        else:
            el["EAN"] = ""
    return some_dict

def find_prices(some_dict,price_dict):
    for k,el in some_dict.items():
        if k in price_dict:
            el["price"] = price_dict[k]["price"] 
        else:
            el["price"] = 0
    return some_dict

def sort(some_list: list) -> list:
    new_dict = {}
    for el in some_list:
        if el["EAN"] not in new_dict:
            new_dict[el["EAN"]] = []
        new_dict[el["EAN"]].append(el)
    del some_list
    
    for k,v in new_dict.items():
        if k:
            new_dict[k] = sorted(v,key=lambda x: x["price"],reverse=True)[:2]
        
    
    new_list = []
    for value in new_dict.values():
        new_list.extend(value)
    
    return new_list

def add_new_products(new_dict: dict,old_dict: dict):
    for k in old_dict.keys():
        if k not in new_dict:
            old_dict[k]["comment"] = "Not on Tim"
        elif old_dict[k]["comment"] == "Not on Tim":
            old_dict[k]["comment"] = ""
        else:
            continue
    
    for k in new_dict.keys():
        if k not in old_dict and k:
            prod = {
                "SKU": new_dict[k]["SKU"],
                "category_id":"319054",
                "status": "",
                "allegro_id": "",
                "available_in_parcel_locker": "",
                "comment":""
            }
            old_dict[new_dict[k]["SKU"]] = prod
    
    return old_dict

def find_cat_id(product_dict: dict) -> list:
    with open(RESULTS,"r") as file:
        reader = csv.DictReader(file)
        rows = {row["SKU"]: row["category"] for row in reader}
    with open("/Users/macair/Desktop/Inglobus/Ready allegro.pl/categories.json","r") as file:
        categories = json.load(file)
        
    for k,v in product_dict.items():
        if k in rows:
            v["category"] = rows[k]
        elif v["category"] in categories:
            v["category"] = categories[v["category"]]
        else:
            v["category"] = "110248"
    
    return list(product_dict.values())
            

def update_database():
    start = time.time()
    while True:
        try:
            tim_token = get_tim_token()
            break
        except:
            time.sleep(1)
    print("Updating database")
    with open(download_products_file(tim_token),"r") as file:
        reader = csv.DictReader(file)
        new_products = {row["SKU"]: row for row in reader if row["SKU"]}
    
    with open(download_invertory_file(tim_token),"r") as file:
        reader = csv.DictReader(file)
        inventory = {row["sku"]: row for row in reader}
    
    with open("/Users/macair/Desktop/Inglobus/Ready allegro.pl/databases/tim/new_prices.csv","r") as file:
        reader = csv.reader(file)
        price_dict = {row[1]: (float(row[-1].replace(",",".")) if row[-1] else 0) for row in reader}
    
    with open(RESULTS,"r") as file:
        reader = csv.DictReader(file)
        statuses = {row["SKU"]: row for row in reader}
    
    keys = list(new_products.keys())
    for k in keys :
        if k not in inventory:
            new_products.pop(k)
            
    
    for k,v in new_products.items():
        v["shipping_cost"] = inventory[k]["shipping_cost"]
        v["unit"] = inventory[k]["unit"]
        v["qty"] = inventory[k]["qty"]
        v["shipping"] = inventory[k]["shipping"]
        
        if k in price_dict:
            v["price"] = price_dict[k]
        else:
            v["price"] = 0
        
        if k in statuses:
            v["status"] = statuses[k]["status"]
            v["allegro_id"] = statuses[k]["allegro_id"]
        else:
            v["status"] = ""
            v["allegro_id"] = ""
            
        extra_fields = ["ID","reference_number","can_be_returned","is_wire","available","logistic_height","logistic_width","logistic_length","logistic_weight","is_vendor",""]
        for field in extra_fields:
            v.pop(field)
        
    new_products = find_cat_id(new_products)
    
    print("Sorting")
    
    keys = new_products[0].keys()
    sorted_products = sort(new_products)
    with open(SORTED_DT,"w") as file:
        writer = csv.DictWriter(file,keys)
        writer.writeheader()
        writer.writerows(sorted_products)
        
    print("total time is: "+str(time.time()-start))

if __name__ == "__main__":
    update_database()