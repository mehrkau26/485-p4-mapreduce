"""MapReduce framework Manager node."""
import os
import shutil
import tempfile
import logging
import time
import threading
from collections import deque
import click
from mapreduce.utils.network import tcp_server
from mapreduce.utils.network import tcp_client
from mapreduce.utils.network import udp_server
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
        self.job_queue = deque()
        self.task_queue = deque()
        self.job_id = 0
        self.lock = threading.Lock()
        self.signals = {"shutdown": False}
        self.worker_dict = ThreadSafeOrderedDict()
        self.start_listening_tcp()
        self.start_listening_udp()

        while not self.signals["shutdown"]:
            if self.job_queue:
                job = self.job_queue.popleft()
                self.make_tasks(job)
                print("check")
            time.sleep(0.1)
        self.tcp_thread.join()
        print("manager tcp thread joined, manager fully shut down")

    def start_listening_tcp(self):
        """Start TCP thread."""
        self.tcp_thread = threading.Thread(
            target=tcp_server, args=(self.host, self.port, self.signals,
                                     self.handlemessage))
        self.tcp_thread.start()

    def start_listening_udp(self):
        """Start UDP thread."""
        self.udp_thread = threading.Thread(
            target=udp_server, args=(self.host, self.port, self.signals,
                                     self.heartbeat_checker))

    def heartbeat_checker(self):
        """Send heartbeat."""
        return True

    def handlemessage(self, message_dict):
        """Handle all messages."""
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
                tcp_client(message_dict["worker_host"],
                           message_dict["worker_port"],
                           register_ack)
                print("ack sent to worker")

        if message_dict["message_type"] == "shutdown":
            self.signals["shutdown"] = True
            LOGGER.info("hello")
            for worker_port, worker_info in self.worker_dict.items():
                worker_host = worker_info["host"]
                tcp_client(worker_host, worker_port, message_dict)
            # self.tcp_thread.join()
            print("manager shutting down")

        if message_dict["message_type"] == "new_manager_job":
            message_dict["finished"] = False
            message_dict["job_id"] = self.job_id
            self.job_queue.append(message_dict)
            self.job_id += 1

        if message_dict["message_type"] == "finished":
            print(
                f"""finished message recieved from:
                {message_dict['worker_port']}""")
            worker_port = message_dict['worker_port']
            self.worker_dict[worker_port]['status'] = "Ready"
    # def next_available_worker(self):
    #     print("found worker")
    #     return self.worker_dict[6001]

    def assign_tasks(self):
        """Assign tasks to workers."""
        while not self.signals["shutdown"]:
            if self.task_queue:
                job = self.task_queue.popleft()
                print(len(self.task_queue))
                assigned = False
                # worker_dict = self.next_available_worker()
                # print("i have a worker!")
                for worker_port, worker_info in self.worker_dict.items():
                    print("looking for worker")
                    if worker_info['status'] == 'Ready':
                        worker_host = worker_info['host']
                        success = tcp_client(worker_host, worker_port, job)
                        if success:
                            print("worker assigned to task")
                            worker_info['status'] = 'Busy'
                            assigned = True
                        else:
                            worker_info['status'] = 'Dead'
                            print("worker is dead")
                            # self.task_queue.appendLeft(job)
                if assigned is False:
                    print("reassigning")
                    self.task_queue.appendleft(job)
                # wait for worker to become available
            time.sleep(0.1)

    def make_tasks(self, job):
        """Add tasks to queue."""
        # delete output dir if it exists
        # create output dir
        if os.path.exists(job["output_directory"]):
            shutil.rmtree(job["output_directory"])
        os.mkdir(job["output_directory"])

        # create temp_dir (mapreduce-shared-jobid(in 5 digits)-d56wiir)
        prefix = f"mapreduce-shared-job{job["job_id"]:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            LOGGER.info("Created tmpdir %s", tmpdir)
            input_files = sorted(os.listdir(job["input_directory"]))
            print("input files", input_files)
            partitions = [[] for _ in range(job["num_mappers"])]

            for i, file in enumerate(input_files):
                print("entering partition logic", i, file)
                task_id = i % job["num_mappers"]
                print("task id", task_id)
                full_path = os.path.join(job["input_directory"], file)
                partitions[task_id].append(full_path)

            # create message_dict for each task_id and add to task_queue
            for taskid, files in enumerate(partitions):
                print(f"Task {taskid}: {files}")
                message_dict = {
                    "message_type": "new_map_task",
                    "task_id": taskid,
                    "input_paths": files,
                    "executable": job["mapper_executable"],
                    "output_directory": tmpdir,
                    "num_partitions": job["num_reducers"]
                }
                self.task_queue.append(message_dict)
            self.assign_tasks()
        while not self.signals["shutdown"]:  # change? until job is completed?
            time.sleep(0.1)
            # DO MAP STAGE WORK THEN SET FINISHED TO TRUE
        LOGGER.info("Cleaned up tmpdir %s", tmpdir)

    def make_reduce_tasks(self, job, shared_dir):
        """Reduce tasks."""
        reduce_tasks = [[] for _ in range(job["num_reducers"])]
        input_files = sorted(os.listdir(shared_dir))

        for file in input_files:
            if file.startswith("maptask") and "part" in file:
                part_num = int(file.split("part")[1])
                reduce_tasks[part_num].append(os.path.join(shared_dir, file))

        for task_id, input_paths in enumerate(reduce_tasks):
            if input_paths:
                message_dict = {
                    "message_type": "new_reduce_task",
                    "task_id": task_id,
                    "executable": job["reducer_executable"],
                    "input_paths": input_paths,
                    "output_directory": job["output_directory"]
                }
                self.task_queue.append(message_dict)

        self.assign_tasks()


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


if __name__ == "__main__":
    main()
