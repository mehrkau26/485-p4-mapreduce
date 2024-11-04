"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import click
import threading
import mapreduce.utils
from mapreduce.utils.network import tcp_server
from mapreduce.utils.network import tcp_client
from mapreduce.utils.network import udp_server
from mapreduce.utils import ThreadSafeOrderedDict
from collections import deque 


# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""

        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        self.host = host
        self.port = port 
        self.job_queue = deque()
        self.lock = threading.Lock()
        self.signals = {"shutdown": False}
        self.worker_dict = ThreadSafeOrderedDict()

    def start_listening_tcp(self):
        self.tcp_thread = threading.Thread(target=tcp_server, args=(self.host, self.port,self.signals, self.job_queue))
        self.tcp_thread.start()
    def start_listening_udp(self):
        self.udp_thread = threading.Thread(target=udp_server, args=(self.host, self.port, self.signals, self.heartbeat_checker))
    def heartbeat_checker(self):
        return True
    def handlemessage(self, message_dict):
        if message_dict["message_type"] == "register":
            with self.lock:
                self.worker_dict[message_dict["worker_port"]] = {
                    'host': message_dict["worker_host"],
                    'status': 'Ready'
                }
            # print("worker added:" + self.worker_dict["worker_port"])
                register_ack = {
                    "message_type": "register_ack"
                }
                tcp_client(message_dict["worker_host"], message_dict["worker_port"], register_ack)
                print("ack sent to worker")
        if message_dict["message_type"] == "shutdown":
            self.signals["shutdown"] = True
            for worker_port, worker_info in self.worker_dict.items():
                worker_host = worker_info["host"]
                tcp_client(worker_host, worker_port, message_dict)
            self.tcp_thread.join()
            print("manager shutting down")
 

@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    manager = Manager(host, port)
    
    manager.start_listening_tcp()
    manager.start_listening_udp()
    

    while not manager.signals["shutdown"]:
        if manager.job_queue:
            job=manager.job_queue.popleft()
            manager.handlemessage(job)
        time.sleep(0.1)
    manager.tcp_thread.join()
    print("manager tcp thread joined, manager fully shut down")



    



if __name__ == "__main__":
    main()