from spade import quit_spade
import time
import datetime
from spade.agent import Agent
from spade.behaviour import CyclicBehaviour, PeriodicBehaviour
from spade.template import Template
from spade.message import Message
import sys
import pandas as pd
import logging
import argparse
import operative_functions as opf
import os


class LogAgent(Agent):
    class LogBehav(CyclicBehaviour):
        async def run(self):
            global wait_msg_time, logger, log_status_var
            if log_status_var == "on":
                msg = await self.receive(timeout=wait_msg_time)  # wait for a message for 20 seconds
                if msg:
                    print(f"received msg number {self.counter}")
                    logger.info(msg.body)
                    self.counter += 1
                else:
                    logger.debug(f"Log_agent didn't receive any msg in the last {wait_msg_time}s")
            elif log_status_var == "stand-by":
                print(log_status_var)
                logger.debug(f"Log agent status: {log_status_var}")
                log_status_var = "on"
                logger.debug(f"Log agent status: {log_status_var}")
                print(log_status_var)
            else:
                logger.debug(f"Log agent status: {log_status_var}")
                log_status_var = "stand-by"
                logger.debug(f"Log agent status: {log_status_var}")

        async def on_end(self):
            await self.agent.stop()

        async def on_start(self):
            self.counter = 1

    async def setup(self):
        b = self.LogBehav()
        template = Template()
        template.metadata = {"performative": "inform"}
        self.add_behaviour(b, template)


if __name__ == "__main__":
    """Parser parameters"""
    parser = argparse.ArgumentParser(description='Log parser')
    parser.add_argument('-an', '--agent_number', type=int, metavar='', required=False, default=1, help='agent_number: from CA_01, will be 1')
    parser.add_argument('-v', '--verbose', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], metavar='', required=False, default='DEBUG', help='verbose: amount of information saved')
    parser.add_argument('-w', '--wait_msg_time', type=int, metavar='', required=False, default=5, help='wait_msg_time: time in seconds to wait for a msg')
    parser.add_argument('-st', '--stop_time', type=int, metavar='', required=False, default=84600, help='stop_time: time in seconds where agent')
    args = parser.parse_args()

    my_dir = os.getcwd()
    my_name = os.path.basename(__file__)[:-3]
    my_full_name = opf.my_full_name(my_name, args.agent_number)
    wait_msg_time = args.wait_msg_time
    log_status_var = "Stand-by"

    """Logger info"""
    logger = logging.getLogger(__name__)
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(pathname)s:%(message)s')  # parameters saved to log file. message will be the *.json
    file_handler = logging.FileHandler(f'{my_dir}/{my_name}.log')  # log file
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    if args.verbose == "DEBUG":
        logger.setLevel(logging.DEBUG)
    elif args.verbose == "INFO":
        logger.setLevel(logging.INFO)
    elif args.verbose == "WARNING":
        logger.setLevel(logging.WARNING)
    elif args.verbose == "ERROR":
        logger.setLevel(logging.ERROR)
    elif args.verbose == "CRITICAL":
        logger.setLevel(logging.CRITICAL)
    else:
        print('not valid verbosity')
    logger.debug(f"{my_name}_agent started")

    """XMPP info"""
    log_jid = opf.agent_jid(my_dir, my_full_name)
    log_passwd = opf.agent_passwd(my_dir, my_full_name)
    log_agent = LogAgent(log_jid, log_passwd)
    future = log_agent.start(auto_register=True)
    future.result()


    """Counter"""
    stop_time = datetime.datetime.now() + datetime.timedelta(seconds=args.stop_time)
    while datetime.datetime.now() < stop_time:
        time.sleep(1)
    else:
        log_agent.stop()
        log_status_var = "off"
        logger.critical(f"{my_full_name}_agent stopped, coil_status_var: {log_status_var}")
        quit_spade()
    #while 1:

