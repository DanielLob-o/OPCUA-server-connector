import os
import pytz
import psycopg2
import logging
from psycopg2.extras import RealDictCursor
from datetime import datetime

tzspain = pytz.timezone("Europe/Madrid")

DDBB_INFO = {
    "user": os.getenv('POSTGRES_USER', ""),
    "password": os.getenv('POSTGRES_PASSWORD', ""),
    "host": os.getenv('POSTGRES_HOST', ""),
    "port": os.getenv('POSTGRES_PORT', ""),
    "database": os.getenv('POSTGRES_DB', "")
}


def get_nodes():
    ids = []
    nodes = []
    try:
        with psycopg2.connect(**DDBB_INFO) as con:
            with con.cursor() as cur:
                cur.execute(f"""select dpi.metric_id,nodo
                                from elliot.asset a 
                                join elliot.metric m on a.id=m.asset_id 
                                join elliot.dms_plcs_info dpi on m.id=dpi.metric_id 
                                where a.id =7""")
                nodes_info = cur.fetchall()
                for i in range(len(nodes_info) - 1):
                    ids.append(nodes_info[i][0])
                    nodes.append(nodes_info[i][1])

        return ids, nodes
    except (Exception, psycopg2.Error) as error:
        logging.exception(f"Error while connecting to PostgreSQL {error, Exception}")


def get_lamps():
    ids = []
    try:
        with psycopg2.connect(**DDBB_INFO) as con:
            with con.cursor() as cur:
                cur.execute(f"""select m.id
                                from elliot.metric m
                                where m.id>560
                                order by m.id""")
                lamps_info = cur.fetchall()
                for i in range(len(lamps_info) - 1):
                    ids.append(lamps_info[i][0])
        return ids
    except (Exception, psycopg2.Error) as error:
        logging.exception(f"Error while connecting to PostgreSQL {error, Exception}")


def insert_OPC_DATA(ids, values):
    data_to_insert = []
    time = [datetime.now(tz=tzspain)] * len(ids)
    data_to_insert = zip(ids, time, values)

    try:
        with psycopg2.connect(**DDBB_INFO) as con:
            with con.cursor() as cur:
                query = '''insert into elliot.metric_numeric_data(metric_id, ts, value)values %s'''
                psycopg2.extras.execute_values(cur, query, data_to_insert)
                con.commit()
                logging.info('OPCUA data successfully inserted.')
    except (Exception, psycopg2.Error) as error:
        logging.exception(f"Error while connecting to PostgreSQL {error, Exception}")

