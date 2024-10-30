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
from worker import workers


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

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        # TODO: you should remove this. This is just so the program doesn't
        # exit immediately!
        LOGGER.debug("IMPLEMENT ME!")
        time.sleep(120)


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
    signals = {"shutdown": False}
    thread = threading.Thread(target=server, args=(signals,))
    thread.start()
    
    while True:
        message = tcp_server(host, port, signals)
        if message["message_type"] == "shutdown":
            # Handle shutdown logic
            signals["shutdown"] = True
            thread.join()
            #LOGGER.info("main() shutting down")
            for worker in workers:
                worker_host = worker['host']
                worker_port = worker['port']
                tcp_client(worker_host, worker_port, "shutdown")
            thread.close()



if __name__ == "__main__":
    main()
