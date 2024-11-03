"""MapReduce framework Manager node."""
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
from mapreduce.worker.__main__ import Worker
from mapreduce.utils import ThreadSafeOrderedDict


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
        self.signals = {"shutdown": False}
        self.lock = threading.Lock()
        self.worker_dict = ThreadSafeOrderedDict()
        
        self.start_tcp_server()
    
    def manager_message(self, message_dict):
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
        elif message_dict["message_type"] == "shutdown":
        # Handle shutdown logic
            self.signals["shutdown"] = True
            for worker_port, worker_info in self.worker_dict.items():
                worker_host = worker_info["host"]
                tcp_client(worker_host, worker_port, message_dict)
            print("manager shutting down")
            #thread.close()
            

        # if message_dict["message_type"] == # a job:
                
    def start_tcp_server(self):
        self.tcp_thread = threading.Thread(target=tcp_server, args=(self.host, self.port, self.signals, self.manager_message))
        self.tcp_thread.start()
    
    def wait_for_shutdown(self):
        while not self.signals["shutdown"]:
            time.sleep(0.1)
        self.tcp_thread.join()
        print("tcp thread joined, manager fully shut down")

        
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

    print("main() starting")
    manager.wait_for_shutdown()

    
    

   



if __name__ == "__main__":
    main()
