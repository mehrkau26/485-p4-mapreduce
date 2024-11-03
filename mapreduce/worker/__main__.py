"""MapReduce framework Worker node."""
from http import server
import os
import tempfile
import logging
import json
import threading
import time
import click
from mapreduce.utils.network import tcp_client
from mapreduce.utils.network import tcp_server
from mapreduce.utils.network import udp_server
from mapreduce.utils.network import udp_client
import mapreduce.utils


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port):
        self.workers = []
        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port
        
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            self.host, self.port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )
        self.signals = {"shutdown": False}
        self.start_worker_thread()
        self.register_worker()
        
    def worker_message(self, message_dict):
        if message_dict["message_type"] == "register_ack":
            print("ack received from manager")
                #self.udp_thread = threading.Thread(target=udp_server, args=(self.host, self.port, self.signals, worker_message))
                #self.udp_thread.start()
        if message_dict["message_type"] == "shutdown":
            # Handle shutdown logic
            self.signals["shutdown"] = True
                #if hasattr(self, 'tcp_thread'):
                    #self.tcp_thread.join()
                #thread.close()
    def start_worker_thread(self):
        self.tcp_thread = threading.Thread(target=tcp_server, args=(self.host, self.port, self.signals, self.worker_message))
        self.tcp_thread.start()
        #thread.join()
    
    def register_worker(self):
        message_dict = {
            "worker_host": self.host,
            "worker_port": self.port,
            "message_type": "register"
        }
        tcp_client(self.manager_host, self.manager_port, message_dict)
        print("registration message sent to manager")
    
    def wait_for_shutdown(self):
        while not self.signals["shutdown"]:
            time.sleep(0.1)
        self.tcp_thread.join()
        print("worker tcp thread joined, worker fully shut down")


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    worker = Worker(host, port, manager_host, manager_port)


    print("main() starting")
    worker.wait_for_shutdown()
    





if __name__ == "__main__":
    main()
