
"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import click
import threading
from mapreduce.utils.network import tcp_server
from mapreduce.utils.network import tcp_client
from mapreduce.utils.network import udp_server
from collections import deque 


# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )
        self.host = host
        self.port = port
        self.manager_host = manager_host
        self.manager_port = manager_port 
        self.signals = {"shutdown": False}
        self.job_queue = deque()
        self.tcp_thread = threading.Thread(target=tcp_server, args=(self.host, self.port, self.signals, self.job_queue))
        self.tcp_thread.start()
        self.register()

        while not self.signals["shutdown"]:
            if self.job_queue:
                job=self.job_queue.popleft()
                self.handle_message(job)
            time.sleep(0.1)
        self.tcp_thread.join()
        
    print("worker tcp thread joined, worker fully shut down")
    
    # def start_listening_tcp(self):
    #     self.tcp_thread = threading.Thread(target=tcp_server, args=(self.host, self.port, self.signals, self.job_queue))
    #     self.tcp_thread.start()
    def register(self):
        message_dict = {
            "worker_host": self.host,
            "worker_port": self.port,
            "message_type": "register"
        }
        tcp_client(self.manager_host, self.manager_port, message_dict)
        print("registration message sent to manager")
    
    def handle_message(self, message_dict):
        if message_dict["message_type"] == "register_ack":
            print("ack recieved from manager")
        if message_dict["message_type"] == "shutdown":
            print("shutdown message received")
            self.signals["shutdown"] = True
            self.tcp_thread.join()
            print("worker shutting down")



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

    # worker.start_listening_tcp()
    # worker.register()


if __name__ == "__main__":
    main()
