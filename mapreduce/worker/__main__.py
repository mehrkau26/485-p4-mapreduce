"""MapReduce framework Worker node."""
import os
import threading
import logging
import hashlib
import subprocess
import tempfile
import time
import shutil
from collections import deque
import click
from mapreduce.utils.network import tcp_server
from mapreduce.utils.network import tcp_client


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
        self.register()
        self.start_listening_tcp()

        while not self.signals["shutdown"]:
            if self.job_queue:
                job = self.job_queue.popleft()
                self.handle_message(job)
            time.sleep(0.1)
        self.tcp_thread.join()
        print("job tcp thread joined, job fully shut down")

    def start_listening_tcp(self):
        """Start TCP thread."""
        self.tcp_thread = threading.Thread(
            target=tcp_server, args=(self.host, self.port, self.signals,
                                     self.handle_message))
        self.tcp_thread.start()

    def register(self):
        """Register workers."""
        message_dict = {
            "worker_host": self.host,
            "worker_port": self.port,
            "message_type": "register"
        }
        tcp_client(self.manager_host, self.manager_port, message_dict)
        print("registration message sent to manager")

    def handle_message(self, message_dict):
        """Fundamental messaging."""
        if message_dict["message_type"] == "register_ack":
            print("ack recieved from manager")
        if message_dict["message_type"] == "new_map_task":
            print("new map task message received")
            self.handle_map_task(message_dict)

        if message_dict["message_type"] == "shutdown":
            print("shutdown message received")
            self.signals["shutdown"] = True
            # self.tcp_thread.join()
            print("worker shutting down")

    def handle_map_task(self, message_dict):
        """Partition & assign workers."""
        task_id = message_dict["task_id"]
        num_partitions = message_dict["num_partitions"]

        # the temp directory for new_map_task
        with tempfile.TemporaryDirectory(
          prefix=f"mapreduce-local-task{task_id:05d}-") as tmpdir:
            tmp_output_files = []
            for i in range(num_partitions):
                tmp_output_files.append(
                    open(
                        os.path.join(tmpdir, f"maptask{task_id:05d}-part{i:05d}"),
                        "w", encoding="utf-8"
                    )
                )
            LOGGER.info("Created tmpdir %s", tmp_output_files)
            # way to keep track of reducer files

            for input_path in message_dict["input_paths"]:
                print("first file", input_path)
                with open(input_path, encoding="utf-8") as infile:
                    # for line in infile:
                    # print("line", {line})
                    with subprocess.Popen(
                        [message_dict["executable"]],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True,
                    ) as map_process:
                        for line in map_process.stdout:
                            print("line", {line})
                            # partioning manager output files
                            key = line.split('\t')
                            partition_number = (int(hashlib.md5(
                                key[0].encode("utf-8")).hexdigest(),
                                                16)
                                                % num_partitions)
                            # adds line to correct partition output file
                            tmp_output_files[partition_number].write(line)
                            print(f"""writing to:
                                  {tmp_output_files[partition_number]}:
                                  {line}""")
                    # partitioned data is written to output files in temp
                    # directory

            # move to output directory
            for file in tmp_output_files:
                file.close()
                subprocess.run(
                    ["sort", "-o", file.name, file.name], check=True)
                dest_path = os.path.join(
                    message_dict["output_directory"],
                    os.path.basename(file.name))
                print(f"moving {file.name} to {dest_path}")
                shutil.move(file.name, dest_path)

            finished_message = {
                "message_type": "finished",
                "task_id": task_id,
                "worker_host": self.host,
                "worker_port": self.port
            }
            tcp_client(self.manager_host, self.manager_port, finished_message)

        # while not self.signals["shutdown"]:
        # time.sleep(0.1)


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


if __name__ == "__main__":
    main()
