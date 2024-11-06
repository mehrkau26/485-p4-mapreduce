
"""MapReduce framework Worker node."""
import os
import logging
import json
import hashlib
import subprocess
import tempfile
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
        self.register()
        self.start_listening_tcp()


        while not self.signals["shutdown"]:
            if self.job_queue:
                job=self.job_queue.popleft()
                self.handle_message(job)
            time.sleep(0.1)
        self.tcp_thread.join()
        print("job tcp thread joined, job fully shut down")

    def start_listening_tcp(self):
         self.tcp_thread = threading.Thread(target=tcp_server, args=(self.host, self.port, self.signals, self.handle_message))
         self.tcp_thread.start()
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
        if message_dict["message_type"] == "new_map_task":
            print("new map task message received")
            self.handle_map_task(message_dict)

        if message_dict["message_type"] == "shutdown":
            print("shutdown message received")
            self.signals["shutdown"] = True
            # self.tcp_thread.join()
            print("worker shutting down")

    def handle_map_task(self, message_dict):
        task_id = message_dict["task_id"]
        input_paths = message_dict["input_paths"]
        executable = message_dict["excutable"]
        num_partitions = message_dict["num_partitions"]
        output_directory = message_dict["output_directory"]

        #the temp directory for new_map_task
        prefix = "mapreduce-local-task{task_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            tmp_output_files = [open(tmpdir / f"part-{i:05d}.txt" for i in range(num_partitions))]
            #for each input file run mapper executable
            partitions = [[] for _ in range(num_partitions)]
            for i, file in tmp_output_files:
                print("partition_")
                partitions[i] = file
            for input_path in input_paths:
                with open(input_path) as infile:
                    with subprocess.Popen(
                        [executable],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True,
                    ) as map_process:
                        for line in map_process.stdout:
                            #partioning manager output files
                            key = line.split('\t')
                            hexdigest = hashlib.md5(key.encode("utf-8")).hexdigest()
                            keyhash = int(hexdigest, base=16)
                            partition_number = keyhash % num_partitions
    
                            #adds line to correct partition output file
                            with open(partitions[partition_number]) as outfile:
                                outfile.write(line)

                    #partitioned data is written to output files in temp directory
                    #for i, partition in enumerate(partitioning):
                        #with open(tmp_output_files[i], 'a') as partitioned_file:
                            #partitioned_file.write('\n'.join(partition))

            # for file in tmp_output_files:
            #     file.close()

            # for i in range(num_partitions):
            #     filename = f"{tmpdir}/maptask{task_id:05d}-{i:05d}"
            #     subprocess.run(["sort", "-o", filename, filename], check=True)

            # for i in range(num_partitions):
            #     source_file = f"{tmpdir}/maptask{task_id:05d}-{i:05d}"
            #     output_dest = f"{output_directory}/maptask{task_id:05d}-part{i:05d}"
            #     os.rename(source_file, output_dest)
            
            # while True:
            #     time.sleep(0.1)
    
    
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