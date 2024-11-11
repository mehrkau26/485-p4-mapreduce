"""MapReduce framework Worker node."""
from concurrent.futures import ThreadPoolExecutor
import contextlib
import os
import threading
import logging
import hashlib
import subprocess
import tempfile
import time
import heapq
import shutil
from collections import deque
import click
from mapreduce.utils.network import tcp_server
from mapreduce.utils.network import tcp_client
from mapreduce.utils.network import udp_server
from mapreduce.utils.network import udp_client


# Configure logging
LOGGER = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 2  # 1 ping is 2 seconds


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
        self.udp_thread = threading.Thread(
             target=self.send_heartbeat)
        self.udp_thread.start()

        while not self.signals["shutdown"]:
            if self.job_queue:
                job = self.job_queue.popleft()
                if job["message_type"] == "new_map_task":
                    self.handle_map_task(job)
                else:
                    self.handle_reduce_task(job)
            time.sleep(0.1)
        self.tcp_thread.join()
        self.udp_thread.join()
        print("job tcp thread joined, job fully shut down")

    def start_listening_tcp(self):
        """Start TCP thread."""
        self.tcp_thread = threading.Thread(
            target=tcp_server, args=(self.host, self.port, self.signals,
                                     self.handle_message))
        self.tcp_thread.start()

    def send_heartbeat(self):
        """Send heartbeat every 2 seconds."""
        print("entering send_heartbeat")
        while not self.signals["shutdown"]:
            print("i'm alive!")
            message_dict = {
                "message_type": "heartbeat",
                "worker_host": self.host,
                "worker_port": self.port
            }
            udp_client(self.manager_host, self.manager_port, message_dict)
            print("sent heartbeat message")
            time.sleep(HEARTBEAT_INTERVAL)
        self.tcp_thread.join()
        print("heartbeat thread joined, shutting down")

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
            print("starting to send heartbeats")
            self.registered = True
        if message_dict["message_type"] == "new_map_task":
            print("new map task message received")
            self.job_queue.append(message_dict)
        if message_dict["message_type"] == "new_reduce_task":
            print("new reduce task message received")
            self.job_queue.append(message_dict)
        if message_dict["message_type"] == "shutdown":
            print("shutdown message received")
            self.signals["shutdown"] = True
            # self.tcp_thread.join()
            print("worker shutting down")

    def handle_map_task(self, message_dict):
        """Partition & assign workers."""
        task_id = message_dict["task_id"]

        # Temporary directory for new map task
        with tempfile.TemporaryDirectory(
          prefix=f"mapreduce-local-task{task_id:05d}-") as tmpdir:
            LOGGER.info("Created temporary output files in directory %s",
                        tmpdir)
            num_parts = message_dict["num_partitions"]
            # Open each input file and process its content
            with contextlib.ExitStack() as stack:
                partition_files = {
                    partition_number: stack.enter_context(open(
                            os.path.join(
                                tmpdir,
                                f"maptask{task_id:05d}-"
                                f"part{partition_number:05d}"
                            ),
                            "a", encoding="utf-8")
                    )
                    for partition_number in range(
                        num_parts)
                }

                def process_input_file(input_path):
                    with open(input_path, encoding="utf-8",
                              buffering=8192) as infile:
                        process = subprocess.Popen(
                            [message_dict["executable"]],
                            stdin=infile,
                            stdout=subprocess.PIPE,
                            text=True
                        )
                        md = hashlib.md5
                        for line in process.stdout:
                            key = line.split('\t', 1)[0]
                            partition_number = int(
                                md(key.encode("utf-8")).hexdigest(),
                                16) % num_parts
                            partition_files[partition_number].write(line)
                        process.stdout.close()
                        process.wait()

                with ThreadPoolExecutor() as executor:
                    executor.map(
                        process_input_file, message_dict["input_paths"])

                # Close all partition files
                for file in partition_files.values():
                    file.close()

            # Move and sort each file to the output directory
            for partition_number in range(num_parts):
                file_path = os.path.join(tmpdir,
                                         f"maptask{task_id:05d}"
                                         f"-part{partition_number:05d}")
                subprocess.run(
                    ["sort", "-o", file_path, file_path], check=True)
                dest_path = os.path.join(
                    message_dict["output_directory"],
                    os.path.basename(file_path))
                LOGGER.info(
                    "Moving sorted file %s to %s", file_path, dest_path)
                shutil.move(file_path, dest_path)

            # Notify task completion
            finished_message = {
                "message_type": "finished",
                "task_id": task_id,
                "worker_host": self.host,
                "worker_port": self.port,
            }
            tcp_client(self.manager_host, self.manager_port, finished_message)

        # while not self.signals["shutdown"]:
        # time.sleep(0.1)

    def handle_reduce_task(self, message_dict):
        """Reduce task."""
        task_id = message_dict["task_id"]
        input_paths = message_dict["input_paths"]
        executable = message_dict["executable"]

        with tempfile.TemporaryDirectory(
          prefix=f"mapreduce-local-task{task_id:05d}-") as tmpdir:
            output_file_path = os.path.join(tmpdir, f"part-{task_id:05d}")
            with contextlib.ExitStack() as stack:
                files = [stack.enter_context(
                    open(input_file,
                         encoding="utf-8"
                         )) for input_file in input_paths]
                with open(output_file_path, "w", encoding="utf-8") as outfile:
                    reduce_process = stack.enter_context(subprocess.Popen(
                        [executable],
                        text=True,
                        stdin=subprocess.PIPE,
                        stdout=outfile,
                    ))
                    for line in heapq.merge(*files):
                        reduce_process.stdin.write(line)

            for file in files:
                file.close()
            # Check the contents of the output file for verification
            with open(output_file_path, 'r', encoding='utf-8') as outfile:
                print(outfile.read())

            dest_path = os.path.join(
                message_dict["output_directory"],
                os.path.basename(output_file_path))
            LOGGER.info(
                "Moving sorted file %s to %s", output_file_path, dest_path)
            shutil.move(output_file_path, dest_path)

        # move to final output directory here

        finished_message = {
                "message_type": "finished",
                "task_id": task_id,
                "worker_host": self.host,
                "worker_port": self.port
            }
        tcp_client(self.manager_host, self.manager_port, finished_message)
        LOGGER.info("Sent finished reducer message to Manager")


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
