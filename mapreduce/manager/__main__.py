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
        self.signals = {"shutdown": False}
        def manager_message(message_dict):
            if message_dict["message_type"] == "shutdown":
            # Handle shutdown logic
                for worker in Worker:
                    worker_host = worker["host"]
                    worker_port = worker["port"]
                    tcp_client(worker_host, worker_port, message_dict)
                #thread.close()
                self.signals["shutdown"] = True

        thread = threading.Thread(target=tcp_server, args=(host, port, self.signals, manager_message))
        thread.start()
        thread.join()

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
    Manager(host, port)

    print("main() starting")
    

    
    

   



if __name__ == "__main__":
    main()
