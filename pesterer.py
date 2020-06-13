import csv
import json
import urllib.request
from dataclasses import dataclass
from typing import List, Dict

URLFMT = "https://www.decathlon.it/it/ChooseStore_getStoresWithAvailability?storeFullId={storeFullId}&productId={productId}&_={timestamp}"


@dataclass
class PhysicalStore:
    store_code: str
    store_name: str
    store_id: str
    store_unknown_param_1: str
    store_unknown_param_2: str
    store_availability: str


@dataclass
class Product:
    product_id: str
    name: str
    color: str
    size: str
    availability: Dict[str, bool] = None


def main():
    products: List[Product] = list()
    store_ids: List[str] = list()

    with open('products.csv', encoding='utf-8-sig') as products_file, open('stores.csv', encoding='utf-8-sig') as stores_file:
        products_file.readline()
        stores_file.readline()

        products_csv = csv.reader(products_file, delimiter=';')
        stores_csv = csv.reader(stores_file, delimiter=';')
        for row in products_csv:
            products.append(Product(*row))

        for row in stores_csv:
            store_ids.append(row[0])

    for product in products:
        print("Product %s..." % product.product_id)
        product.availability = dict()
        for store_full_id in store_ids:
            formatted_url = URLFMT.format(
                storeFullId=store_full_id,
                productId=product.product_id,
                timestamp="0"
            )

            with urllib.request.urlopen(formatted_url) as url:
                data = json.loads(url.read().decode())
                physical_store = PhysicalStore(*data['physicalStoreList'][0])
                product.availability[physical_store.store_name] = bool(physical_store.store_availability != 'N')
                # print("\t" + physical_store.store_name)

        for k,v in product.availability.items():
            print("%s: %s" % (k, v))


if __name__ == "__main__":
    main()
