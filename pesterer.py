"""
Chiede a Decathlon la disponibilità dei prodotti nei negozi censiti in DB.
Se trova differenze dall'ultima esecuzione, notifica.
"""

import concurrent.futures
import json
import sqlite3
import sys
import urllib.request
from dataclasses import dataclass, field
from typing import Dict, List, Optional

URLFMT = "https://www.decathlon.it/it/ChooseStore_getStoresWithAvailability?storeFullId={storeFullId}&productId={productId}&_={timestamp}"

@dataclass
class Store:
    store_full_id: str
    store_id: int
    store_description: str


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


@dataclass(order=True)
class ProductAvailability:
    product_id: str
    store_id: int
    availability: int = 0


def thread_function(product: Product, store: Store) -> ProductAvailability:
    """ Recupera la disponibilità da Decathlon e restituisce un oggetto con le informazioni"""
    formatted_url = URLFMT.format(
        storeFullId=store.store_full_id,
        productId=product.product_id,
        timestamp="0"
    )

    with urllib.request.urlopen(formatted_url) as url:
        data = json.loads(url.read().decode())
        physical_store = PhysicalStore(*data['physicalStoreList'][0])
        products_list: dict = data['nbProductsList']
        store_id = list(products_list.keys())[
            0] if products_list else store.store_id
        availability = products_list.get(store_id, 0)

        print("%s %s %s a %s: %s (%d)" %
              (product.name, product.color, product.size,
               physical_store.store_name, physical_store.store_availability,
               availability))

        return ProductAvailability(product.product_id, store_id, availability)


def create_connection(db_file: str) -> Optional[sqlite3.Connection]:
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        # cur = conn.cursor()
        # cur.execute(CREATE_STATEMENT)
    except Exception as e:
        print(e)
    return conn


def get_stores(conn: sqlite3.Connection, only_favs=True) -> List[Store]:
    cur = conn.cursor()
    query = """SELECT store_full_id, store_id, store_description FROM stores"""
    if only_favs:
        query += """ WHERE favorite = 'Y'"""
    cur.execute(query)
    stores: List[Store] = [Store(*row) for row in cur.fetchall()]
    return stores


def get_products(conn: sqlite3.Connection, only_favs=True) -> List[Product]:
    cur = conn.cursor()
    query = """SELECT id, name, color, size FROM products"""
    if only_favs:
        query += """ WHERE favorite = 'Y'"""
    cur.execute(query)
    products: List[Product] = [Product(*row) for row in cur.fetchall()]
    return products


def get_product_availability(conn: sqlite3.Connection, product_availability: ProductAvailability) -> Optional[ProductAvailability]:
    cur = conn.cursor()
    query = """SELECT product_id, store_id, availability FROM product_availability WHERE product_id = ? AND store_id = ?"""
    cur.execute(query, (product_availability.product_id,
                        product_availability.store_id))
    result = cur.fetchone()
    if result:
        return ProductAvailability(*result)
    else:
        return None


def update_product_availability(conn: sqlite3.Connection, product_availability: ProductAvailability):
    print("Aggiornamento della disponibilità del prodotto %s" %
          product_availability.product_id)
    cur = conn.cursor()
    cur.execute("""UPDATE product_availability SET availability = ? WHERE product_id = ? AND store_id = ?""",
                (
                    product_availability.availability,
                    product_availability.product_id,
                    product_availability.store_id
                )
                )


def insert_product_availability(conn: sqlite3.Connection, product_availability: ProductAvailability):
    print("Inserimento della disponibilità del prodotto %s precedentemente non censita" %
          product_availability.product_id)
    cur = conn.cursor()
    cur.execute("""INSERT INTO product_availability (product_id, store_id, availability) VALUES (?,?,?)""",
                (
                    product_availability.product_id,
                    product_availability.store_id,
                    product_availability.availability
                )
                )


def main(only_favs=True):
    conn = create_connection('status.db')
    products: List[Product] = get_products(conn, only_favs)
    stores: List[Store] = get_stores(conn, only_favs)

    print("Creo i thread...")
    product_availabilities: List[ProductAvailability] = list()
    futures: List[concurrent.futures.Future] = list()
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        for product in products:
            for store in stores:
                futures.append(executor.submit(
                    thread_function, product, store))

        product_availabilities = [f.result() for f in futures]

    print("Terminata scansione da Decathlon.it")

    for product_availability in product_availabilities:
        old_availability: ProductAvailability = get_product_availability(
            conn, product_availability)
        if not old_availability:
            insert_product_availability(conn, product_availability)
        elif old_availability.availability != product_availability.availability:
            update_product_availability(conn, product_availability)
            # invia mail di cambio disponibilità
            # se la disponibilità è >1, invia mail urgente (?)

    conn.commit()


if __name__ == "__main__":
    only_favs = True
    if len(sys.argv) > 1 and sys.argv[1] == '-a':
        only_favs = False
    main(only_favs)
