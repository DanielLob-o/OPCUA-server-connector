import logging

import opcua_utils as ou
import time
from postgres import *
import socket

SLEEP_TIME = int(os.getenv('SLEEP_TIME', 5))
opc_ip = os.getenv('OPCUAIP', "")
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.WARNING)

lamps_nodes = ['ns=2;s=unit/xSide.ab8SPModuelLamp[0][', 'ns=2;s=unit/xSide.ab8SPModuelLamp[1][',
               'ns=2;s=unit/xSide.ab8SPModuelLamp[2][', 'ns=2;s=unit/xSide.ab8SPModuelLamp[3][',
               'ns=2;s=unit/xSide.ab8SPModuelLamp[4][', 'ns=2;s=unit/xSide.ab8SPModuelLamp[5][']


def main():
    try:
        # Get nodes from DDBB
        str_list = []
        insert_list = []
        logging.info(opc_ip)
        OPC_CLIENT = ou.Client('opc.tcp://192.168.2.36:4840')
        # Create connection to OPCUA before looping into reading variables
        OPC_CLIENT.connect()
        if OPC_CLIENT is not None:
            logging.warning('Connected to OPC-UA: Reading...')
        else:
            logging.warning("Error connecting to OPC-UA")

        while True:
            vals_oven = []
            try:


                # Read node variables
                ids, nodes = get_nodes()
                lamps_id = get_lamps()
                # Read OPCUA nodes
                values = OPC_CLIENT.get_values(nodes)
                # Values parsing bool to format 0/1
                for i in range(len(values) - 1):
                    if type(values[i]) == bool:
                        values[i] = int(values[i])

                for oven_node in lamps_nodes:
                    node = OPC_CLIENT.get_node(oven_node)
                    if len(vals_oven) == 0:
                        vals_oven.append(node.get_data_value().Value.Value)
                    else:
                        vals_oven[0].extend(node.get_data_value().Value.Value)
                logging.warning('Data read: Inserting')

                insert_OPC_DATA(ids, values)
                insert_OPC_DATA(lamps_id, vals_oven[0])

                logging.warning('Data inserted: Disconnect - Sleeping')
                time.sleep(4)

            except Exception as e:
                logging.warning(e)
                try:
                    logging.error("Closing connection")
                    OPC_CLIENT.disconnect()
                except:
                    logging.error("Can't close connection")
                time.sleep(60)
                logging.info("Trying to reconnect")
                OPC_CLIENT.connect()
                if OPC_CLIENT is not None:
                    logging.info("Connected")
                else:
                    logging.info("Cant connect to OPCUA")
            finally:
                time.sleep(SLEEP_TIME)
    except Exception as e:
        logging.exception(e)


if __name__ == '__main__':
    main()
