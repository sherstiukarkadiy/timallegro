from collections import UserDict
import csv
import re
import time

# from bs4 import BeautifulSoup
import requests
from allegro_functions.get_al_requests import get_operation_info, get_parameters
from allegro_functions.post_al_requests import patch_offer, post_product
from tokens.get_allegro_token import get_access_token
import concurrent.futures
from threading import Lock
from updater import update_database
from pathlib import Path

# import allegro_functions.al_requests_calculator as calc
# from watchpoints import watch
# watch(calc.__counter__,callback = calc.more_than_10000)

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

RESULTS = Path(__file__).parent.joinpath("databases/results.csv")
AFTPR_DT = Path(__file__).parent.joinpath("databases/after_process.csv")
READY_DT = Path(__file__).parent.joinpath("databases/ready_offers.csv")
SORTED_DT = Path(__file__).parent.joinpath("databases/sorted_database.csv")
LOCK = Lock()

# def get_thumbnail(search_term):
#     try:
#         url = rf'https://www.google.no/search?q={search_term}&client=opera&hs=cTQ&source=lnms&tbm=isch&sa=X&safe=active&ved=0ahUKEwig3LOx4PzKAhWGFywKHZyZAAgQ_AUIBygB&biw=1920&bih=982'

#         headers = {"Connection": "close"}

#         page = requests.get(url, headers=headers, verify=False).text
#         soup = BeautifulSoup(page, 'html.parser')


#         for raw_img in soup.find_all('img'):
#             link = raw_img.get('src')
#             if link and link.startswith("https://"):
#                 return link
#             pass

#     except Exception as e:
#         sError = f"""Error: {e}
#         search_term: {search_term}
#         """
#         print(sError)
#     pass


def get_shipment_rate(avopl, shiping_price):

    prices = [
        "20",
        "25",
        "30",
        "35",
        "40",
        "45",
        "50",
        "55",
        "60",
        "65",
        "70",
        "75",
        "80",
        "85",
        "90",
        "95",
        "100",
        "105",
        "110",
        "115",
        "120",
        "125",
    ]

    with_parcel_lock = {
        "standard": "ce3f5829-12f2-4928-afb8-171af4feff36",
        "20": "9cad2b29-0794-416c-82d5-04b81a2aa4f5",
        "25": "39aeb2b3-c43c-4a35-a339-772f6b47e472",
        "30": "c6d19488-fa44-4743-ad34-3104e74c03be",
        "35": "e849b64c-e4b9-4dd8-9a84-733b9331a2f4",
        "40": "9b077c74-2077-451e-9e95-0fd86acd74bd",
        "45": "5354f75e-4938-465d-abbb-1b13feba3060",
        "50": "8aa7d67a-39b6-4489-9ef2-d2cd7cc00ef0",
        "55": "c7aa95f1-03f0-4698-88a1-c9c05b323046",
        "60": "2b764e64-a7d2-4493-b0d1-e47f1a11aa54",
        "65": "b37c239b-0d39-4e6f-94eb-adeb67d3cc5e",
        "70": "1d8c7a50-7990-4679-8029-c7ec2288d42c",
        "75": "aecae57d-ed10-4794-9076-5ee57b5cc833",
        "80": "92e9c70a-5ffd-49f5-b922-b2dcfe3be01c",
        "85": "0348fade-f1aa-4851-a8a3-68278d0f97c8",
        "90": "df56cc13-4491-44df-8ab0-0f98854c44d4",
        "95": "f8bae143-404c-4071-a42e-e77761737290",
        "100": "edf1ebf8-50c4-42fb-b476-99300621fb81",
        "105": "8907055b-2bbb-466c-a02d-6e4443241373",
        "110": "4d5bab65-12e0-4fd5-8d38-4597282cddcc",
        "115": "c488ea05-5ab4-4c23-ac79-548a3105d5fb",
        "120": "186630eb-84f8-40ec-874f-80ab7f7c5750",
        "125": "17178f2d-a25e-4287-9963-f961ba15c9e4",
        "130": "2f78beb7-c751-4ed7-af83-a6379a0058d9",
        "135": "63d59457-2365-49b2-b568-bd3ead39f28e",
        "140": "953e8f6e-b7b2-42ce-a4ee-295404146fb3",
        "145": "4224cc1b-e4fc-4239-bb91-64913293eda1",
        "150": "2e17c631-7985-4ce0-a1d6-907ad0550887",
    }

    just_courier = {
        "standard": "de189796-d0d5-4ef5-b017-28aeff2dc935",
        "20": "59adcec5-1bea-417e-b4c7-12bfb37c5efd",
        "25": "4b2c4ab1-e95a-4523-90e1-ef9f150b0213",
        "30": "5f5ba823-7e23-4987-89d4-8f4ccf617b20",
        "35": "40a1cca9-6df2-49f4-aa8e-d970bb179b91",
        "40": "389bdd58-2fd3-4342-928e-65a71e9fb742",
        "45": "1cc7241a-aba0-4266-a32f-4c6166391c7a",
        "50": "cf3c070e-08bd-4e43-be34-b44330ece27b",
        "55": "aa7a8a74-fb6d-41be-b888-3505694e7713",
        "60": "58a98381-a259-4ba7-b4c2-ac54a278c106",
        "65": "ea3e8a17-6849-4950-84ac-53182dc6496b",
        "70": "d442fe2b-80c3-41bc-93c9-493f6fc985bb",
        "75": "01ceb466-acd4-41c4-8f6a-3af723330788",
        "80": "df73f444-e966-47aa-9fda-e66fad1545d7",
        "85": "903a18c3-6a20-4b09-86f6-1a829d299600",
        "90": "f7bcacda-64f9-4504-9f8b-1661c5f07b04",
        "95": "25c9669f-7eac-48cc-ac7c-daab6d1b2576",
        "100": "83b43ef0-8094-4572-b6b2-1c872baedccb",
        "105": "6bf8b15e-e8df-4239-8d87-78eee4903145",
        "110": "7d6c173d-15b4-4e05-9754-952faaf10a6f",
        "115": "34d583fb-09a5-4a82-98af-5396ee10b139",
        "120": "66c94c36-beb9-4e25-b75c-a8e8607ee7fd",
        "125": "54f787da-e3c7-4e7c-9dd7-f00bcad8333e",
        "130": "23369f77-aa06-4514-8d53-33a658ac5680",
        "135": "c5bf6213-5c26-4cea-acd7-174d9b755e57",
        "140": "dd1cb2a0-2f9e-40cd-ab14-e59353c69a65",
        "145": "2deda430-aef4-4f63-b5d2-1f81506e86ad",
        "150": "bd03c0a4-2202-471d-bbf5-e6cc7cf8653d",
    }

    if not shiping_price:
        key = "standard"
    else:
        for price in prices:
            if shiping_price <= int(price):
                key = price
                break
        else:
            key = prices[-1]

    if avopl:
        return with_parcel_lock[key]
    else:
        return just_courier[key]


class Product(UserDict):

    def __init__(self, product_info: dict) -> None:
        self.data = {
            "SKU": product_info["SKU"],
            "name": product_info["name"],
            "category": product_info["category"],
            "allegro_id": product_info["allegro_id"],
            "EAN": product_info["EAN"],
            "available_in_parcel_locker": product_info["available_in_parcel_locker"],
            "shipping": product_info["shipping"],
            "qty": product_info["qty"],
            "unit": product_info["unit"],
            "producer_name": product_info["producer_name"],
            "price": product_info["price"],
            "shipping_cost": product_info["shipping_cost"],
            "default_image": product_info["default_image"],
            "package_size": product_info["package_size"],
        }


class AbsPostProduct(UserDict):

    def __init__(self) -> None:
        self.data = {
            "productSet": [
                {
                    "product": {
                        "name": None,
                        "category": {"id": None},
                        "id": None,
                        "idType": "GTIN",
                        "parameters": [],
                    },
                    "quantity": {"value": None},
                }
            ],
            "b2b": {"buyableOnlyByBusiness": False},
            "stock": {"available": None, "unit": None},
            "delivery": {"handlingTime": None, "shippingRates": {"id": None}},
            "publication": {
                "marketplaces": {
                    "base": {"id": "allegro-pl"},
                    "additional": [{"id": "allegro-cz"}, {"id": "allegro-sk"}],
                },
                "status": "ACTIVE",
                "endedBy": "USER",
            },
            "additionalMarketplaces": {
                "allegro-cz": {
                    "sellingMode": {"price": {"amount": None, "currency": "CZK"}}
                },
                "allegro-sk": {
                    "sellingMode": {"price": {"amount": None, "currency": "EUR"}}
                },
            },
            "language": "pl-PL",
            "category": {"id": None},
            "parameters": [],
            "afterSalesServices": {
                "impliedWarranty": {"id": "72e2a37e-bf4a-4202-9ff1-2e3c068914ae"},
                "returnPolicy": {"id": "2298bb96-3a89-4c55-8eaf-057262c7e1b5"},
                "warranty": {"id": "e27b2f32-2776-415c-a839-a45e4541d926"},
            },
            "name": None,
            "payments": {"invoice": "VAT"},
            "sellingMode": {
                "format": "BUY_NOW",
                "price": {"amount": None, "currency": "PLN"},
            },
            "location": {
                "city": "Wrocław",
                "countryCode": "PL",
                "postCode": "53-612",
                "province": "DOLNOSLASKIE",
            },
            "external": {"id": None},
            "taxSettings": {
                "rates": [
                    {"rate": "23.00", "countryCode": "PL"},
                    {"rate": "21.00", "countryCode": "CZ"},
                ],
                "subject": "GOODS",
            },
            "messageToSellerSettings": {"mode": "OPTIONAL"},
            "description": None,
            "sizeTable": None,
            "promotion": None,
            "attachments": None,
            "contact": None,
        }


class AbsPutchProduct(UserDict):

    def __init__(self) -> None:
        self.data = {
            "stock": {"available": None, "unit": None},
            "sellingMode": {
                "format": "BUY_NOW",
                "price": {"amount": None, "currency": "PLN"},
            },
            "delivery": {"handlingTime": None, "shippingRates": {"id": None}},
            "additionalMarketplaces": {
                "allegro-cz": {
                    "sellingMode": {"price": {"amount": None, "currency": "CZK"}},
                    "publication": {"state": "APPROVED"},
                },
                "allegro-sk": {
                    "sellingMode": {"price": {"amount": None, "currency": "EUR"}},
                    "publication": {"state": "APPROVED"},
                },
            },
            "publication": {
                "marketplaces": {
                    "base": {"id": "allegro-pl"},
                    "additional": [{"id": "allegro-cz"}, {"id": "allegro-sk"}],
                },
                "status": "ACTIVE",
                "endedBy": "USER",
            },
        }


class ProductPoster(AbsPostProduct):
    i = 0

    def __init__(self, product: Product) -> None:
        ProductPoster.i += 1
        # print(self.i)
        if ProductPoster.i % 1000 == 0:
            print(self.i)
        super().__init__()

        self.SKU = product["SKU"]
        self.data["productSet"][0]["product"]["category"]["id"] = self.data["category"][
            "id"
        ] = product["category"]
        self.data["external"]["id"] = product["SKU"]
        self.avopl = (
            int(product["available_in_parcel_locker"])
            if product["available_in_parcel_locker"]
            else 0
        )

        try:
            with LOCK:
                al_token = get_access_token()
        except:
            self.data = {"error": "Allegro failure"}
            return

        self.stock = int(product["qty"].split(".")[0])
        if self.stock < 20:
            self.data = {"error": "Not enough qty"}
            return

        if product["package_size"] and product["package_size"].lower() != "paczka":
            self.data = {"error": "Shipping is not matching"}
            return

        self.data["stock"]["available"] = self.stock

        self.shipping = product["shipping"]
        if not self.shipping or not self.shipping.strip():
            self.data = {"error": "Shipping is not matching"}
            return
        elif (
            "niedostępny" in self.shipping.lower()
            or "na zamówienie" in self.shipping.lower()
        ):
            self.data = {"error": "Shipping is not matching"}
            return
        elif "dni" in self.shipping:
            self.data = {"error": "Shipping is not matching"}
            return

        self.shipping = re.search(r"\d+", self.shipping).group()
        if int(self.shipping) > 72:
            self.data = {"error": "Shipping is not matching"}
            return
        self.data["delivery"]["handlingTime"] = "PT" + self.shipping + "H"

        self.name = (
            product["name"].replace("—", " ").replace("™", " ").replace("⌑", " ")
        )
        self.shipment_price = (
            product["shipping_cost"] if product["shipping_cost"] else 0
        )
        self.shipment_price = float(self.shipment_price)
        if "bębnowy" in self.name.lower():
            self.shipment_price = self.shipment_price + 25
        self.data["delivery"]["shippingRates"]["id"] = get_shipment_rate(
            self.avopl, self.shipment_price
        )
        del self.avopl

        self.qty = re.search(r"\d+szt", self.name.replace(" ", ""))

        self.new_name = []
        for i in self.name.split():
            if len(i) > 30:
                self.new_name.append(i[:30])
                self.new_name.append(i[30:])
            self.new_name.append(i)
        self.name = " ".join(self.new_name)
        del self.new_name

        while len(self.name) > 75:
            self.name = " ".join(self.name.split()[:-1])
        self.data["name"] = self.name

        while len(self.name) > 50:
            self.name = " ".join(self.name.split()[:-1])
        self.data["productSet"][0]["product"]["name"] = self.name

        if not self.qty:
            self.qty = 1
        else:
            self.qty = re.search(r"\d+", self.qty.group())
            self.qty = int(self.qty.group())

        del self.name

        self.ean = product["EAN"]
        if self.ean:
            self.data["productSet"][0]["product"]["id"] = self.ean
            self.ex = {"id": "225693", "name": "EAN (GTIN)", "values": [self.ean]}
            self.data["productSet"][0]["product"]["parameters"].append(self.ex)

        try:
            self.allegro_parameters = get_parameters(al_token, product["category"])[
                "parameters"
            ]
        except:
            self.data = {"error": "Allegro failure"}
            return

        self.param_names = [x["name"] for x in self.allegro_parameters]

        self.marka = product["producer_name"].title()

        if "Marka" in self.param_names:
            self.ex = {"name": "Marka", "values": [self.marka]}
            self.data["productSet"][0]["product"]["parameters"].append(self.ex)

        if "Producent" in self.param_names:
            self.ex = {"name": "Producent", "values": [self.marka]}
            self.data["productSet"][0]["product"]["parameters"].append(self.ex)

        if "Kod producenta" in self.param_names and self.ean:
            self.kod_pr = self.ean[3:7]
            self.ex = {"name": "Kod producenta", "values": [self.kod_pr]}
            self.data["productSet"][0]["product"]["parameters"].append(self.ex)

        del self.ean
        del self.param_names
        del self.marka

        self.ex = {"name": "Stan", "values": ["Nowy"]}

        self.data["parameters"].append(self.ex)
        del self.ex

        # self.price = "".join(product["price"].split()[:-1]).replace(",",".")
        self.price = float(product["price"]) * 1.23 * 1.35
        if not self.price:
            self.data = {"error": "No price"}
            return

        i = 1
        while self.price < 1:
            i += 1
            self.price = self.price * i
        self.qty = self.qty * i
        self.data["productSet"][0]["quantity"]["value"] = self.qty

        if product["unit"] == "para":
            self.data["stock"]["unit"] = "PAIR"
        elif product["unit"] == "opak.":
            self.data["stock"]["unit"] = "SET"
        else:
            self.data["stock"]["unit"] = "UNIT"

        self.data["sellingMode"]["price"]["amount"] = round(self.price, 1)
        self.data["additionalMarketplaces"]["allegro-cz"]["sellingMode"]["price"][
            "amount"
        ] = round(self.price * 5.3, 0)
        self.data["additionalMarketplaces"]["allegro-sk"]["sellingMode"]["price"][
            "amount"
        ] = round(self.price / 4.3, 2)

        if product["default_image"]:
            self.data["productSet"][0]["product"]["images"] = [product["default_image"]]
            self.data["images"] = [product["default_image"]]
        # else:
        #     self.name = product["name"].replace("—"," ").replace("™"," ").replace("⌑"," ")
        #     image = get_thumbnail(self.name)
        #     if image:
        #         self.data["productSet"][0]["product"]["images"] = [image]
        #         self.data["images"] = [image]

        del self.qty
        if int(self.shipping) > 48 or self.shipment_price > 40:
            self.data["publication"]["marketplaces"].pop("additional")
            self.data.pop("additionalMarketplaces")
        del self.shipping

    def post_product(self):
        if "error" in self.data:
            return (self.SKU, {"status": "error: " + self.data["error"]})

        try:
            with LOCK:
                al_token = get_access_token()
            self.result = post_product(al_token, self.data)
            while self.result.status_code == 202:
                self.sleep = int(self.result.headers["retry-after"])
                self.link = self.result.headers["location"]
                time.sleep(self.sleep)
                self.result = get_operation_info(al_token, self.link)

            if self.result.status_code == 201 or self.result.status_code == 200:
                return (
                    self.SKU,
                    {
                        "status": "status: "
                        + str(self.result.status_code)
                        + " succesfuly posted",
                        "allegro_id": self.result.json()["id"],
                    },
                )
            elif self.result.status_code == 400:
                return (
                    self.SKU,
                    {
                        "status": "status: "
                        + str(self.result.status_code)
                        + " Problems with authorization"
                    },
                )

            elif "CATEGORY_MISMATCH" in [
                i["code"] for i in self.result.json()["errors"]
            ]:
                return (
                    self.SKU,
                    {
                        "status": "status: "
                        + str(self.result.status_code)
                        + " category mismatch, must be "
                        + self.result.json()["errors"][0]["message"].split("category")[
                            -1
                        ]
                    },
                )
            else:
                try:
                    self.status = (
                        "status: "
                        + str(self.result.status_code)
                        + " "
                        + self.result.json()["errors"][0]["message"]
                    )
                    if "Unprocessable" in self.status:
                        print(self.result.json())
                    if (
                        "The provided parameter" in self.status
                        or "Value already exists" in self.status
                    ):
                        return self.repost()
                    elif "Platforms other than" in self.status:
                        self.data.pop("additionalMarketplaces")
                        self.data["publication"]["marketplaces"].pop("additional")
                        return self.post_product()
                    return (
                        self.SKU,
                        {
                            "status": "status: "
                            + str(self.result.status_code)
                            + " "
                            + self.result.json()["errors"][0]["message"]
                        },
                    )
                except:
                    return (
                        self.SKU,
                        {"status": "Undefined " + str(self.result.status_code)},
                    )
        except:
            return (self.SKU, {"status": "error: Allegro error"})

    def repost(self, status=None):
        if status:
            self.status = status

        if "error" in self.data:
            return (self.SKU, {"status": "error: " + self.data["error"]})

        self.parameters = self.data["productSet"][0]["product"]["parameters"]

        if "The provided parameter" in self.status:
            self.category_name = (
                re.search(r"'\w+\s?\w+'", self.status).group().replace("'", "")
            )
            if self.category_name == "Marka" or self.category_name == "Producent":
                self.new_value = re.findall(r"\w+\s?-?\w+\(", self.status)[-1].replace(
                    "(", ""
                )
            elif self.category_name == "Kod producenta":
                self.new_value = (
                    self.status.split("value ")[-1].replace("(", "").replace(")", "")
                )
            else:
                return (self.SKU, {"status": self.status})

            for i in self.parameters:
                if i["name"] == self.category_name:
                    i["values"] = [self.new_value]
                    break
                else:
                    pass
            else:
                self.parameters.append(
                    {"name": self.category_name, "values": [self.new_value]}
                )

        else:
            self.category_id = re.search(r"parameter\.id='.+'", self.status).group()
            self.category_id = (
                re.search(r"'.+'", self.category_id).group().replace("'", "")
            )

            self.new_value = re.search(r"value='.+'", self.status).group()
            self.new_value = re.search(r"'.+'", self.new_value).group().replace("'", "")
            for i in self.allegro_parameters:
                if i["id"] == self.category_id:
                    self.category_name = i["name"]
                    break

            for i in self.parameters:
                if i["name"] == self.category_name:
                    i["values"] = [self.new_value]
                    break
                else:
                    pass
            else:
                self.parameters.append({"name": self.name, "values": [self.new_value]})

        del self.category_name
        del self.new_value

        self.data["productSet"][0]["product"]["parameters"] = self.parameters
        del self.parameters

        try:
            with LOCK:
                al_token = get_access_token()
            self.result = post_product(al_token, self.data)
            while self.result.status_code == 202:
                self.sleep = int(self.result.headers["retry-after"])
                self.link = self.result.headers["location"]
                time.sleep(self.sleep)
                self.result = get_operation_info(al_token, self.link)

            if self.result.status_code == 201 or self.result.status_code == 200:
                return (
                    self.SKU,
                    {
                        "status": "status: "
                        + str(self.result.status_code)
                        + " succesfuly posted",
                        "allegro_id": self.result.json()["id"],
                    },
                )
            elif self.result.status_code == 400:
                return (
                    self.SKU,
                    {
                        "status": "status: "
                        + str(self.result.status_code)
                        + " Problems with authorization"
                    },
                )

            elif "CATEGORY_MISMATCH" in [
                i["code"] for i in self.result.json()["errors"]
            ]:
                return (
                    self.SKU,
                    {
                        "status": "status: "
                        + str(self.result.status_code)
                        + " category mismatch, must be "
                        + self.result.json()["errors"][0]["message"].split("category")[
                            -1
                        ]
                    },
                )
            else:
                try:
                    self.status = (
                        "status: "
                        + str(self.result.status_code)
                        + " "
                        + self.result.json()["errors"][0]["message"]
                    )
                    if (
                        "The provided parameter" in self.status
                        or "Value already exists" in self.status
                    ):
                        return self.repost()
                    elif "Platforms other than" in self.status:
                        self.data.pop("additionalMarketplaces")
                        self.data["publication"]["marketplaces"].pop("additional")
                        return self.post_product()
                    return (self.SKU, {"status": self.status})
                except:
                    return (
                        self.SKU,
                        {"status": "Undefined" + str(self.result.status_code)},
                    )
        except:
            return (self.SKU, {"status": "error: Allegro error"})


class ProductPutcher(AbsPutchProduct):
    i = 0
    end = {"publication": {"status": "ENDED"}}

    def __init__(self, product: Product) -> None:
        ProductPutcher.i += 1
        # print(self.i)
        if ProductPutcher.i % 1000 == 0:
            print(self.i)
        super().__init__()
        self.SKU = product["SKU"]
        self.of_id = product["allegro_id"]
        self.avopl = (
            int(product["available_in_parcel_locker"])
            if product["available_in_parcel_locker"]
            else 0
        )

        if product["package_size"] and product["package_size"].lower() != "paczka":
            self.data = self.end
            return

        self.stock = int(product["qty"].split(".")[0])
        if self.stock < 20:
            self.data = self.end
            return

        self.shipping = product["shipping"]
        if not self.shipping or not self.shipping.strip():
            self.data = self.end
            return
        elif (
            "niedostępny" in self.shipping.lower()
            or "na zamówienie" in self.shipping.lower()
        ):
            self.data = self.end
            return
        elif "dni" in self.shipping:
            self.data = self.end
            return

        self.shipping = re.search(r"\d+", self.shipping).group()
        if int(self.shipping) > 78:
            self.data = self.end
            return
        self.data["delivery"]["handlingTime"] = "PT" + self.shipping + "H"

        self.data["stock"]["available"] = self.stock
        del self.stock

        self.name = (
            product["name"].replace("—", " ").replace("™", " ").replace("⌑", " ")
        )

        self.shipment_price = (
            product["shipping_cost"] if product["shipping_cost"] else 0
        )
        self.shipment_price = float(self.shipment_price)
        if "bębnowy" in self.name.lower():
            self.shipment_price = self.shipment_price + 25
        self.data["delivery"]["shippingRates"]["id"] = get_shipment_rate(
            self.avopl, self.shipment_price
        )

        del self.avopl
        self.qty = re.search(r"\d+szt", self.name.replace(" ", ""))
        del self.name

        if not self.qty:
            self.qty = 1
            self.data["stock"]["unit"] = "UNIT"
        else:
            self.qty = re.search(r"\d+", self.qty.group())
            self.qty = int(self.qty.group())

        # self.price = "".join(self.tim_info["price"]["value"].split()[:-1]).replace(",",".")
        self.price = float(product["price"]) * 1.23 * 1.35
        if not self.price:
            self.data = self.end
            return

        i = 1
        while self.price < 1:
            i += 1
            self.price = self.price * i
        self.qty = self.qty * i
        if product["unit"] == "para":
            self.data["stock"]["unit"] = "PAIR"
        elif product["unit"] == "opak.":
            self.data["stock"]["unit"] = "SET"
        else:
            self.data["stock"]["unit"] = "UNIT"

        self.data["sellingMode"]["price"]["amount"] = round(self.price, 1)
        self.data["additionalMarketplaces"]["allegro-cz"]["sellingMode"]["price"][
            "amount"
        ] = round(self.price * 5.3, 0)
        self.data["additionalMarketplaces"]["allegro-sk"]["sellingMode"]["price"][
            "amount"
        ] = round(self.price / 4.3, 2)

        del self.qty
        del self.price
        if int(self.shipping) > 48 or self.shipment_price > 40:
            self.data["publication"] = {
                "status": "ACTIVE",
                "marketplaces": {"additional": []},
            }
            self.data["additionalMarketplaces"] = {}
        del self.shipping

    def putch_product(self):
        try:
            with LOCK:
                al_token = get_access_token()
            if self.data["publication"]["status"] == "ENDED":
                patch_offer(al_token, self.of_id, self.data)
                return (self.SKU, {"status": "ended"})

            self.result = patch_offer(al_token, self.of_id, self.data)
            while self.result.status_code == 202:
                self.sleep = int(self.result.headers["retry-after"])
                self.link = self.result.headers["location"]
                time.sleep(self.sleep)
                self.result = get_operation_info(al_token, self.link)
                del self.sleep

            if self.result.status_code == 201 or self.result.status_code == 200:
                return (
                    self.SKU,
                    {
                        "status": "status: "
                        + str(self.result.status_code)
                        + " succesfuly putched",
                        "allegro_id": self.result.json()["id"],
                    },
                )
            elif self.result.status_code == 400:
                return (
                    self.SKU,
                    {
                        "status": "status: "
                        + str(self.result.status_code)
                        + " Problems with authorization"
                    },
                )

            elif "CATEGORY_MISMATCH" in [
                i["code"] for i in self.result.json()["errors"]
            ]:
                return (
                    self.SKU,
                    {
                        "status": "status: "
                        + str(self.result.status_code)
                        + " category mismatch, must be "
                        + self.result.json()["errors"][0]["message"].split("category")[
                            -1
                        ]
                    },
                )
            else:
                patch_offer(al_token, self.of_id, {"publication": {"status": "ENDED"}})
                try:
                    return (
                        self.SKU,
                        {
                            "status": "status: "
                            + str(self.result.status_code)
                            + " "
                            + self.result.json()["errors"][0]["message"]
                        },
                    )
                except:
                    return (
                        self.SKU,
                        {"status": "Undefined error" + str(self.result.status_code)},
                    )
        except:
            return (self.SKU, {"status": "error: Allegro error"})


class ProductEnder(ProductPutcher):

    def __init__(self, product) -> None:
        self.of_id = product["allegro_id"]
        self.SKU = product["SKU"]

    def end_product(self):
        end = {"publication": {"status": "ENDED"}}

        try:
            with LOCK:
                al_token = get_access_token()
            self.result = patch_offer(al_token, self.of_id, end)
            while self.result.status_code == 202:
                self.sleep = int(self.result.headers["retry-after"])
                self.link = self.result.headers["location"]
                time.sleep(self.sleep)
                self.result = get_operation_info(al_token, self.link)

            if self.result.status_code == 201 or self.result.status_code == 200:
                return (
                    self.SKU,
                    {
                        "status": "status: "
                        + str(self.result.status_code)
                        + " succesfuly ended",
                        "allegro_id": self.result.json()["id"],
                    },
                )
            elif self.result.status_code == 400:
                return (
                    self.SKU,
                    {
                        "status": "status: "
                        + str(self.result.status_code)
                        + " Problems with authorization"
                    },
                )

            elif "CATEGORY_MISMATCH" in [
                i["code"] for i in self.result.json()["errors"]
            ]:
                return (
                    self.SKU,
                    {
                        "status": "status: "
                        + str(self.result.status_code)
                        + " category mismatch, must be "
                        + self.result.json()["errors"][0]["message"].split("category")[
                            -1
                        ]
                    },
                )
            else:
                patch_offer(al_token, self.of_id, {"publication": {"status": "ENDED"}})
                try:
                    return (
                        self.SKU,
                        {
                            "status": "status: "
                            + str(self.result.status_code)
                            + " "
                            + self.result.json()["errors"][0]["message"]
                        },
                    )
                except:
                    return (
                        self.SKU,
                        {"status": "Undefined error" + str(self.result.status_code)},
                    )
        except:
            return (self.SKU, {"status": "error: Allegro error"})


def get_prod_to_end():
    with open(SORTED_DT, "r") as file:
        reader = csv.DictReader(file)
        sorted_products = [row["SKU"] for row in reader]

    with open(READY_DT, "r") as file:
        reader = csv.DictReader(file)
        ended_products = []
        for row in reader:
            if "succesfuly p" in row["status"] and row["SKU"] not in sorted_products:
                ended_products.append(row)

    return ended_products


def get_putch_datas():
    end_products_list = get_prod_to_end()

    with open(READY_DT, "r") as file:
        reader = csv.DictReader(file)
        ready = {row["SKU"]: row for row in reader}

    with open(SORTED_DT, "r") as file:
        reader = csv.DictReader(file)

        putch_products = []
        end_products = end_products_list
        i = 0
        for row in reader:

            if not row["allegro_id"]:
                continue
            elif row["status"] != "error: Allegro error" and row == ready[row["SKU"]]:
                continue
            elif row["status"] == "NIE DOTYCHY":
                continue
            else:
                putch_products.append(Product(row))
                i += 1

            if i % 1000 == 0:
                yield putch_products, end_products
                putch_products = []
                end_products = []

    yield putch_products, end_products


def get_post_datas():
    with open(SORTED_DT, "r") as file:
        reader = csv.DictReader(file)

        post_products = []
        i = 0
        for row in reader:

            if row["allegro_id"]:
                continue
            elif (
                "status: 422 At least one image should be attached to add product"
                in row["status"]
            ):
                # post_products.append(Product(row))
                # i+=1
                pass
            else:
                post_products.append(Product(row))
                i += 1

                if i % 1000 == 0:
                    yield post_products
                    post_products = []

    yield post_products


def get_missmatch_datas():

    with open(AFTPR_DT, "r") as file:
        reader = csv.DictReader(file)
        post_products = [row for row in reader if "category mismatch" in row["status"]]

    return post_products


def after_process():
    with open(AFTPR_DT, "r") as file:
        reader = csv.DictReader(file)
        rows_1 = {row["SKU"]: row for row in reader}

    with open(RESULTS, "r") as file:
        reader = csv.DictReader(file)
        rows_2 = {row["SKU"]: row for row in reader}

    rows_2.update(rows_1)
    del rows_1
    new_rows = []
    new_rows = list(rows_2.values())
    del rows_2
    keys = new_rows[0].keys()

    with open(RESULTS, "w") as file:
        writer = csv.DictWriter(file, keys)
        writer.writeheader()
        writer.writerows(new_rows)

    ready_products = [row for row in new_rows if row["allegro_id"]]
    with open(READY_DT, "w") as file:
        writer = csv.DictWriter(file, keys)
        writer.writeheader()
        writer.writerows(ready_products)


def end_products_fun(end_products):
    print("Creating ending data ....")
    end_classes = list(map(lambda x: ProductEnder(x), end_products))
    print("Ending products qty: " + str(len(end_classes)))
    main_dict = {x["SKU"]: x for x in end_products}
    del end_products
    print("Processing ending data ....")

    end_results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
        end_results.update(dict(executor.map(lambda x: x.end_product(), end_classes)))
    del end_classes

    for k, v in main_dict.items():
        v.update(end_results[k])

    return main_dict


def putch_products_fun(putch_products):
    print("-" * 40)
    print("Creating putch data ....")
    putch_classes = list(map(ProductPutcher, putch_products))
    print("Putching products qty: " + str(len(putch_classes)))
    print("Processing putching data ....")
    main_dict = {x["SKU"]: x for x in putch_products}
    del putch_products

    putch_results = {}
    start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
        putch_results.update(
            dict(executor.map(lambda x: x.putch_product(), putch_classes))
        )
    print("time - " + str(time.time() - start))
    del putch_classes

    for k, v in main_dict.items():
        if k in putch_results:
            v.update(putch_results[k])

    return main_dict


def post_products_fun(post_products):
    print("-" * 40)
    print("Creating post data ....")
    post_classes = list(map(lambda x: ProductPoster(x), post_products))
    print("Posting products qty: " + str(len(post_classes)))
    print("Processing posting data ....")
    main_dict = {x["SKU"]: x for x in post_products}
    del post_products

    post_results = {}
    start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
        post_results.update(
            dict(executor.map(lambda x: x.post_product(), post_classes))
        )
    print("time - " + str(time.time() - start))
    del post_classes

    for k, v in main_dict.items():
        if k in post_results:
            v.update(post_results[k])

    return main_dict


if __name__ == "__main__":
    # aftpr_keys = [
    #     "SKU",
    #     "name",
    #     "EAN",
    #     "producer_name",
    #     "category",
    #     "shipping",
    #     "package_size",
    #     "available_in_parcel_locker",
    #     "default_image",
    #     "shipping_cost",
    #     "unit",
    #     "qty",
    #     "price",
    #     "status",
    #     "allegro_id",
    # ]
    # update_database()

    # with open(AFTPR_DT, "w") as file:
    #     writer = csv.DictWriter(file, aftpr_keys)
    #     writer.writeheader()

    # putch_products_generator = get_putch_datas()

    # start = time.time()
    # for putch_products, end_products in putch_products_generator:
    #     main_dict = putch_products_fun(putch_products)
    #     del putch_products
    #     main_dict.update(end_products_fun(end_products))
    #     del end_products
    #     print("Saving ....")
    #     with open(AFTPR_DT, "a") as file:
    #         writer = csv.DictWriter(file, aftpr_keys)
    #         writer.writerows(main_dict.values())

    # print("\nTime lost: " + str(time.time() - start))
    # print("-" * 100, end="\n\n")

    # post_products_generator = get_post_datas()
    # start = time.time()
    # for post_products in post_products_generator:
    #     main_dict = post_products_fun(post_products)
    #     print("Saving ....")
    #     with open(AFTPR_DT, "a") as file:
    #         writer = csv.DictWriter(file, aftpr_keys)
    #         writer.writerows(main_dict.values())
    # print("\nTime lost: " + str(time.time() - start))
    # print("-" * 100, end="\n\n")

    # start = time.time()
    # missmatch_products = get_missmatch_datas()
    # for row in missmatch_products:
    #     new_category = (
    #         re.search(r"\(\d+\)", row["status"])
    #         .group()
    #         .replace("(", "")
    #         .replace(")", "")
    #     )
    #     row["category"] = new_category

    # main_dict = post_products_fun(list(map(Product, missmatch_products)))
    # print("Saving ....")
    # with open(AFTPR_DT, "r") as file:
    #     reader = csv.DictReader(file)
    #     old_rows = {row["SKU"]: row for row in reader}
    # old_rows.update(main_dict)
    # with open(AFTPR_DT, "w") as file:
    #     writer = csv.DictWriter(file, aftpr_keys)
    #     writer.writeheader()
    #     writer.writerows(old_rows.values())
    # del main_dict
    # print("\nTime lost: " + str(time.time() - start))
    # print("-" * 100, end="\n\n")

    print("After-processing ...")
    after_process()
    print("Succesfully ended!")
