"""
Chiede a Decathlon la disponibilità dei prodotti nei negozi censiti in DB.
Se trova differenze dall'ultima esecuzione, notifica.
"""
import argparse
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


def get_store(conn: sqlite3.Connection, store_full_id: str) -> Optional[Store]:
    cur = conn.cursor()
    query = """SELECT store_full_id, store_id, store_description FROM stores WHERE store_full_id = ?"""
    cur.execute(query, (store_full_id,))
    result = cur.fetchone()
    if result:
        return Store(*result)
    else:
        return None


def get_stores(conn: sqlite3.Connection, only_favs=True) -> List[Store]:
    cur = conn.cursor()
    query = """SELECT store_full_id, store_id, store_description FROM stores"""
    if only_favs:
        query += """ WHERE favorite = 'Y'"""
    cur.execute(query)
    stores: List[Store] = [Store(*row) for row in cur.fetchall()]
    return stores


def get_product(conn: sqlite3.Connection, product_id: str) -> Optional[Product]:
    cur = conn.cursor()
    query = """SELECT id, name, color, size FROM products WHERE id = ?"""
    cur.execute(query, (product_id,))
    result = cur.fetchone()
    if result:
        return Product(*result)
    else:
        return None


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
    product = get_product(conn, product_availability.product_id)
    assert product is not None
    store = get_store(conn, "007" + (str(product_availability.store_id).zfill(5)) * 2)
    assert store is not None

    print("Aggiornamento della disponibilità del prodotto %s %s %s (%d pezz%s presso %s)" % (
        product.name,
        product.color,
        product.size,
        product_availability.availability,
        "o" if product_availability.availability == 1 else "i",
        store.store_description))
    cur = conn.cursor()
    cur.execute("""UPDATE product_availability SET availability = ? WHERE product_id = ? AND store_id = ?""", (
        product_availability.availability,
        product_availability.product_id,
        product_availability.store_id))


def insert_product_availability(conn: sqlite3.Connection, product_availability: ProductAvailability):
    product = get_product(conn, product_availability.product_id)
    assert product is not None
    store = get_store(conn, "007" + (str(product_availability.store_id).zfill(5)) * 2)
    assert store is not None

    print("Inserimento della disponibilità del prodotto %s %s %s (%d pezz%s presso %s) precedentemente non censita" % (
        product.name,
        product.color,
        product.size,
        product_availability.availability,
        "o" if product_availability.availability == 1 else "i",
        store.store_description))
    cur = conn.cursor()
    cur.execute("""INSERT INTO product_availability (product_id, store_id, availability) VALUES (?,?,?)""", (
        product_availability.product_id,
        product_availability.store_id,
        product_availability.availability))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--all", help="elabora tutti i prodotti e negozi, non solo i preferiti",
                        dest="only_favs", action="store_false")
    parser.add_argument("-n", "--thread-number", help="numero di thread da usare per le richieste HTTP", 
                        type=int, default=16)
    return parser.parse_args()


def main(only_favs=True, threads=16):
    conn = create_connection('status.db')
    products: List[Product] = get_products(conn, only_favs)
    stores: List[Store] = get_stores(conn, only_favs)

    print("Creo i thread...")
    product_availabilities: List[ProductAvailability] = list()
    futures: List[concurrent.futures.Future] = list()
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
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
    args = parse_args()
    main(args.only_favs, args.thread_number)
