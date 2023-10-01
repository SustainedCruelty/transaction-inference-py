#!/usr/bin/python3 
import duckdb
import pandas as pd
import numpy as np
import requests
import os
import time
import logging
import math

from queue import PriorityQueue
from datetime import datetime, timedelta

def stmt_create_transactions_table() -> str:
    return """
    CREATE TABLE transactions (
        ORDERID BIGINT,
        TYPEID INTEGER,
        QUANTITY INTEGER,
        VOLUMEREMAIN INTEGER,
        REGIONID BIGINT,
        LOCATIONID BIGINT,
        SYSTEMID INTEGER,
        PRICE DECIMAL(20, 2),
        ISBUY BOOLEAN,
        FILLED BOOLEAN,
        TIMESTAMP BIGINT,
        METHOD VARCHAR(20)
    );
    """

def stmt_create_updates_table() -> str:
    return """
    CREATE TABLE updates (
        ORDERID BIGINT,
        TYPEID INTEGER,
        VOLUMETOTAL INTEGER,
        VOLUMEREMAIN INTEGER,
        REGIONID BIGINT,
        LOCATIONID BIGINT,
        SYSTEMID INTEGER,
        AMOUNT DECIMAL(20, 2),
        ISBUY BOOLEAN,
        TIMESTAMP BIGINT
    );
    """

def stmt_infer_transactions_simple(region: int, ts: int) -> str: 
    return f"""
    INSERT INTO transactions
    SELECT
        t1.order_id AS ORDERID,
        t1.type_id AS TYPEID,
        t1.volume_remain - t2.volume_remain AS QUANTITY,
        t2.volume_remain AS VOLUMEREMAIN,
        {region} AS REGIONID,
        t1.location_id AS LOCATIONID,
        t1.system_id AS SYSTEMID,
        t2.price AS PRICE,
        t2.is_buy_order AS ISBUY,
        FALSE AS FILLED,
        {ts} AS TIMESTAMP,
        'SIMPLE' AS METHOD
    FROM old_orders_{region} t1
    INNER JOIN new_orders_{region} t2
    ON t1.order_id = t2.order_id
    AND t2.volume_remain < t1.volume_remain;
    """

def stmt_infer_buy_order_fills(region: int, ts: int, volume_multiplier: float = 1.0) -> str:
    return f"""
    WITH missing_buy_orders AS (
        SELECT
            t1.*
        FROM old_orders_{region} t1
        LEFT JOIN new_orders_{region} t2
        ON t1.order_id = t2.order_id 
        WHERE t1.is_buy_order = TRUE
        AND t2.order_id IS NULL
    ), grouped_buy_orders AS (
        SELECT 
            type_id, 
            system_id, 
            MAX(price) AS MAX_BUY, 
            SUM(volume_total - volume_remain) AS VOLUME 
        FROM old_orders_{region}
        WHERE is_buy_order = TRUE
        GROUP BY type_id, system_id
    )
    INSERT INTO transactions
    SELECT 
        t1.order_id AS ORDERID,
        t1.type_id AS TYPEID,
        t1.volume_remain AS QUANTITY,
        0 AS VOLUMEREMAIN,
        {region} AS REGIONID,
        t1.location_id AS LOCATIONID,
        t1.system_id AS SYSTEMID,
        t1.price AS PRICE,
        t1.is_buy_order AS ISBUY,
        TRUE AS FILLED,
        {ts} AS TIMESTAMP,
        'ADVANCED_BUY' AS METHOD
    FROM missing_buy_orders t1
    INNER JOIN grouped_buy_orders t2
    ON t1.type_id = t2.type_id
    AND t1.volume_remain < ({volume_multiplier} * t2.VOLUME)
    AND t1.system_id = t2.system_id
    AND t1.price = t2.MAX_BUY;      
    """

def stmt_infer_sell_order_fills(region: int, ts: int, volume_multiplier: float = 1.0) -> str:
    return f"""
    WITH missing_sell_orders AS (
        SELECT
            t1.*
        FROM old_orders_{region} t1
        LEFT JOIN new_orders_{region} t2
        ON t1.order_id = t2.order_id 
        WHERE t1.is_buy_order = FALSE
        AND t2.order_id IS NULL
    ), grouped_sell_orders AS (
        SELECT 
            type_id, 
            system_id, 
            MIN(PRICE) AS MIN_SELL, 
            SUM(volume_total - volume_remain) AS VOLUME 
        FROM old_orders_{region}
        WHERE is_buy_order = FALSE
        GROUP BY type_id, system_id
    )
    INSERT INTO transactions
    SELECT 
        t1.order_id AS ORDERID,
        t1.type_id AS TYPEID,
        t1.volume_remain AS QUANTITY,
        0 AS VOLUMEREMAIN,
        {region} AS REGIONID,
        t1.location_id AS LOCATIONID,
        t1.system_id AS SYSTEMID,
        t1.price AS PRICE,
        t1.is_buy_order AS ISBUY,
        TRUE AS FILLED,
        {ts} AS TIMESTAMP,
        'ADVANCED_SELL' AS METHOD
    FROM missing_sell_orders t1
    INNER JOIN grouped_sell_orders t2
    ON t1.type_id = t2.type_id
    AND t1.system_id = t2.system_id
    AND t1.volume_remain < ({volume_multiplier} * t2.VOLUME)
    AND t1.price = MIN_SELL;     
    """

def stmt_infer_updates(region: int, ts: int) -> str:
    return f"""
    INSERT INTO updates
    SELECT
        t1.order_id AS ORDERID,
        t1.type_id AS TYPEID,
        t1.volume_total AS VOLUMETOTAL,
        t1.volume_remain AS VOLUMEREMAIN,
        {region} AS REGIONID,
        t1.location_id AS LOCATIONID,
        t1.system_id AS SYSTEMID,
        (t1.price - t2.price) AS AMOUNT,
        t1.is_buy_order AS ISBUY,
        {ts} AS TIMESTAMP
    FROM old_orders_{region} t1
    INNER JOIN new_orders_{region} t2
    ON t1.order_id = t2.order_id
    AND t1.price != t2.price;
    """

START_TS = math.floor(datetime.now().timestamp())

ORDERS_TYPES = {
    'duration': np.uint32,
    'is_buy_order': 'bool',
    'issued': 'datetime64',
    'location_id': np.uint64,
    'min_volume': np.uint32,
    'order_id': np.uint64,
    'price': np.float32,
    'range': 'object',
    'system_id': np.uint32,
    'type_id': np.uint32,
    'volume_remain':np.uint32,
    'volume_total': np.uint32
}

log_format = '[%(asctime)s] [%(levelname)s] - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)

BASE_URL = 'https://esi.evetech.net/latest/markets/%d/orders/?datasource=tranquility&order_type=all&page=%d'
DATE_FORMAT = "%a, %d %b %Y %H:%M:%S GMT"
regions = [10000030]

orderbooks_fetched, etags_per_page = {}, {}
region_requests = PriorityQueue()
for r in regions:
    resp = requests.head(BASE_URL % (r, 1))
    expiry = datetime.strptime(resp.headers['Expires'], DATE_FORMAT)
    region_requests.put((expiry + timedelta(seconds=1), r))
    orderbooks_fetched[r] = 0
    etags_per_page[r] = {}

conn = duckdb.connect(database=':memory:', read_only=False)
conn.sql(stmt_create_transactions_table())
conn.sql(stmt_create_updates_table())

while not region_requests.empty() > 0:

    req = region_requests.get()
    current_expiry, region = req
    current_expiry_ts = math.floor(current_expiry.timestamp())

    wait_time = (current_expiry - datetime.now()).total_seconds()
    logging.info(f"waiting for {wait_time} seconds to fetch region {region}")
    if wait_time > 0:
        time.sleep(wait_time)

    new_expiry, new_expiry_ts = None, None
    pages = 1

    page, retries = 1, 0

    while page <= pages:
        if retries > 4:
            logging.error("too many retries, aborting")
            os._exit(1)

        headers = {}
        if page in etags_per_page[region]:
            headers['If-None-Match'] = etags_per_page[region][page]

        resp = requests.get(BASE_URL % (region, page), headers=headers)
        if resp.status_code not in [200, 304]:
            logging.error(f"request for page {page} returned status code {resp.status_code}")
            logging.warning(f" retrying in {5 ** retries} second(s)...")
            time.sleep(5 ** retries)
            retries += 1
            continue

        etag = resp.headers['ETag']
        etags_per_page[region][page] = etag

        if page == 1:
            if resp.status_code == 304:
                logging.debug("matching etags for page 1")
                conn.sql(f"CREATE TABLE new_orders_{region} AS SELECT * FROM old_orders_{region} WHERE etag = '{etag}';")
            else:
                orders_df = pd.DataFrame.from_records(resp.json()).astype(ORDERS_TYPES)
                conn.sql(f"CREATE TABLE new_orders_{region} AS SELECT *, {region} AS region_id, 1 AS page, '{etag}' as etag FROM orders_df;")

            pages = int(resp.headers['X-Pages'])
            logging.debug(f"region {region} has {pages} pages")

            new_expiry = datetime.strptime(resp.headers['Expires'], DATE_FORMAT)
            new_expiry_ts = math.floor(new_expiry.timestamp())

        else:
            if resp.status_code == 304:
                logging.debug(f"matching etags for page {page}")
                conn.sql(f"INSERT INTO new_orders_{region} SELECT * FROM old_orders_{region} WHERE etag = '{etag}';")
            else:
                orders_df = pd.DataFrame.from_records(resp.json()).astype(ORDERS_TYPES)
                conn.sql(f"INSERT INTO new_orders_{region} SELECT *, {region} AS region_id, {page} AS page, '{etag}' as etag FROM orders_df;")

        logging.info(f"fetched page {page} for region {region}")

        retries = 0
        page += 1

    orderbooks_fetched[region] += 1

    if orderbooks_fetched[region] > 1:
        conn.sql(stmt_infer_transactions_simple(region, current_expiry_ts))
        conn.sql(stmt_infer_buy_order_fills(region, current_expiry_ts, 0.2))
        conn.sql(stmt_infer_sell_order_fills(region, current_expiry_ts, 0.2))

        if not os.path.exists('./transactions/'):
            os.makedirs('./transactions/')
                 
        logging.info(f"inferred transactions for region {region}")
        conn.sql("SELECT * FROM transactions;").to_csv(f'./transactions/transactions_{START_TS}.csv', sep=';', header=True)

        conn.sql(stmt_infer_updates(region, current_expiry_ts))

        if not os.path.exists('./updates/'):
            os.makedirs('./updates/')

        logging.info(f"inferred order updates for region {region}")
        conn.sql("SELECT * FROM updates;").to_csv(f'./updates/updates_{START_TS}.csv', sep=';', header=True)

    conn.sql(f"DROP TABLE IF EXISTS old_orders_{region};")
    conn.sql(f"ALTER TABLE new_orders_{region} RENAME TO old_orders_{region};")

    region_requests.put((new_expiry + timedelta(seconds=1), region))
