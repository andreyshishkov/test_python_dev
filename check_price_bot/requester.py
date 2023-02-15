import asyncio
import aiohttp
import psycopg
from datetime import datetime


PATH = 'https://api.binance.com/api/v3/ticker/price'

params = {
    'symbol': 'XRPUSDT'
}


# database work
def create_table_prices():
    with psycopg.connect('dbname=demo user=postgres') as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS prices(
                price REAL,
                request_date TIMESTAMP
                );
            """
        )


def get_max_price():
    with psycopg.connect('dbname=demo user=postgres') as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    MAX(price)
                FROM
                    prices
                WHERE
                    EXTRACT(EPOCH FROM (NOW() - request_date)) <= 3600;
                """
            )
            price = cur.fetchone()[0]
    return price


def insert_price(log_time, price):
    with psycopg.connect('dbname=demo user=postgres', autocommit=True) as conn:
        with conn.cursor() as cur:
            data = (price, log_time)
            cur.execute(
                """
                INSERT INTO prices
                VALUES (%s, %s)
                """,
                data
            )


# Web connection and processing
async def get_data(session: aiohttp.ClientSession) -> None:
    while True:
        async with session.get(url=PATH, params=params) as response:
            data = await response.json()
            log_time = datetime.now()
            price = float(data.get('price'))
            insert_price(log_time, price)

            max_price = get_max_price()
            if price < 0.99 * max_price:
                print('Цена упала на 1% по отношению к максимальной за час')


async def main():
    async with aiohttp.ClientSession() as session:
        tasks = []
        for _ in range(3):
            task = asyncio.create_task(get_data(session))
            tasks.append(task)
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
