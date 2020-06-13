"""
Chiede a Decathlon la disponibilità dei prodotti censiti in DB nei negozi censiti su csv.
Se trova differenze dall'ultima esecuzione, notifica.
"""

import copy
import csv
import json
import sqlite3
import threading
import urllib.request
from dataclasses import dataclass, field
from typing import Dict, List, Optional

URLFMT = "https://www.decathlon.it/it/ChooseStore_getStoresWithAvailability?storeFullId={storeFullId}&productId={productId}&_={timestamp}"

CREATE_STATEMENT = """ CREATE TABLE IF NOT EXISTS products (
                        id integer PRIMARY KEY,
                        name text NOT NULL,
                        color text,
                        size text,
                        availability text
                    ); """


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
    availability: List[str] = field(default_factory=list)


def thread_function(product: Product, store_full_id: str):
    """ Recupera la disponibilità da Decathlon e aggiorna il prodotto """
    formatted_url = URLFMT.format(
        storeFullId=store_full_id,
        productId=product.product_id,
        timestamp="0"
    )

    with urllib.request.urlopen(formatted_url) as url:
        data = json.loads(url.read().decode())
        physical_store = PhysicalStore(*data['physicalStoreList'][0])
        if physical_store.store_availability == 'Y':
            product.availability.append(store_full_id)


def create_connection(db_file: str) -> Optional[sqlite3.Connection]:
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        # cur = conn.cursor()
        # cur.execute(CREATE_STATEMENT)
    except Exception as e:
        print(e)
    return conn


def get_products(conn: sqlite3.Connection) -> List[Product]:
    cur = conn.cursor()
    cur.execute("""SELECT id, name, color, size, availability FROM products""")
    products: List[Product] = list()
    for row in cur.fetchall():
        product = Product(*row[:-1])
        product.availability = row[-1].split(',') if row[-1] != '' else []
        products.append(product)

    return products


def update_product(conn: sqlite3.Connection, product: Product):
    print("Aggiornamento della disponibilità del prodotto %s" %
          product.product_id)
    cur = conn.cursor()
    cur.execute("""UPDATE products SET availability = ? WHERE id = ?""",
                (','.join(product.availability), product.product_id))


def main():
    conn = create_connection('status.db')
    old_products: List[Product] = get_products(conn)
    new_products: List[Product] = copy.deepcopy(old_products)
    store_ids: List[str] = list()

    with open('stores.csv', encoding='utf-8-sig') as stores_file:
        stores_file.readline()
        stores_csv = csv.reader(stores_file, delimiter=';')
        for row in stores_csv:
            store_ids.append(row[0])

    threads = list()
    print("Creo i thread...")
    for product in new_products:
        product.availability = list()

        for store_full_id in store_ids:
            thread = threading.Thread(
                target=thread_function, args=(product, store_full_id))
            threads.append(thread)
            thread.start()  # begin thread execution

    for thread in threads:
        thread.join()

    for old_product, new_product in zip(old_products, new_products):
        old_product.availability.sort()
        new_product.availability.sort()
        if old_product.availability != new_product.availability:
            update_product(conn, new_product)

    conn.commit()


if __name__ == "__main__":
    main()
