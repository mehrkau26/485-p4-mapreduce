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
import mapreduce.utils


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

        def worker_message():
            if message["message_type"] == "shutdown":
            # Handle shutdown logic
                signals["shutdown"] = True
                thread.join()
                thread.close()


        self.signals = {"shutdown": False}
        thread = threading.Thread(target=tcp_server, args=(host, port, self.signals, worker_message))
        thread.start()

        #while True:
            #message = tcp_server(manager_host, manager_port, signals, handle_func=worker_message)

        
        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register_ack",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))


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
    Worker(host, port, manager_host, manager_port)


    print("main() starting")
    





if __name__ == "__main__":
    main()
