"""MapReduce framework Manager node."""
from http import server
import os
import tempfile
import logging
import json
import threading
import time
import click
from collections import deque
from mapreduce.utils.network import tcp_client
from mapreduce.utils.network import tcp_server
from mapreduce.utils import ThreadSafeOrderedDict
from mapreduce.utils.job import Job


# Configure logging
LOGGER = logging.getLogger(__name__)

job_id_counter = 0
is_job_running = False

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
        self.job_queue = deque()
        
        self.start_tcp_server()
    
    def get_new_job_id(self):
        global job_id_counter
        self.job_id = job_id_counter
        job_id_counter += 1
        return self.job_id
    
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
            with self.lock:
                self.signals["shutdown"] = True
                for worker_port, worker_info in self.worker_dict.items():
                    worker_host = worker_info["host"]
                    tcp_client(worker_host, worker_port, message_dict)
                print("manager shutting down")
            #thread.close()

        # getting the job from client to manager
        elif message_dict["message_type"] == "new_manager_job":
            with self.lock:
                job_id = self.get_new_job_id()
                message_dict["job_id"] = job_id
                self.job_queue.append(message_dict)
                for item in self.job_queue:
                    print("job", item)
 

    
    def start_tcp_server(self):
        self.tcp_thread = threading.Thread(target=tcp_server, args=(self.host, self.port, self.signals, self.manager_message))
        self.tcp_thread.start()
    
    def start_job_processor(self):
        self.job_processor_thread = threading.Thread(target=self.process_job_queue)

    def run_job(self):
        print("entering new_manager_job")
        while not self.signals["shutdown"] & is_job_running:
                time.sleep(0.1)
                with self.lock:
                    is_job_running = True
                    job = Job(self.job_queue)
    
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
    manager.run_job()

if __name__ == "__main__":
    main()
