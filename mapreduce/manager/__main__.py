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

HEARTBEAT_INTERVAL = 2  # 1 ping is 2 seconds
HEARTBEAT_TIMEOUT = 10  #10 seconds or 5 pings)

class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        self.job_queue = deque()
        self.task_queue = deque()
        self.job_id = 0
        self.num_tasks = 0
        self.finished_tasks = 0
        self.finished = False
        self.finished_all = False
        self.lock = threading.Lock()
        self.signals = {"shutdown": False}
        self.worker_dict = ThreadSafeOrderedDict()

        self.tcp_thread = threading.Thread(
            target=tcp_server, args=(host, port, self.signals,
                                     self.handlemessage))
        self.tcp_thread.start()

        # udp_thread = threading.Thread(
        #     target=udp_server, args=(host, port, self.signals,
        #                              self.receive_heartbeats))
        # udp_thread.start()
        # fault_tolerance = threading.Thread(target=self.heartbeat_checker)
        # fault_tolerance.start()

        while not self.signals["shutdown"]:
            if self.job_queue:
                job = self.job_queue.popleft()
                self.make_tasks(job)
                self.num_tasks = 0
                self.finished_tasks = 0
                #print("check")
            time.sleep(0.1)
        self.tcp_thread.join()
        print("manager tcp thread joined, manager fully shut down")

    # def receive_heartbeats(self, message_dict):
    #     if message_dict["message_type"] == "heartbeat":
    #         print(f"received heartbeat from {message_dict['worker_port']}")
    #         worker_port = message_dict['worker_port']
    #         if worker_port in self.worker_dict:
    #             self.worker_dict[worker_port]['last_heartbeat'] = float(time.time())

    # def heartbeat_checker(self):
    #     print("start of function heartbeat_checker")
    #     """Send heartbeat."""

    #     while not self.signals["shutdown"]:
    #         current_time = float(time.time())
    #         for worker_port, worker_data in self.worker_dict.items():
    #             last_time = float(worker_data.get('last_heartbeat', 0))
    #             if current_time - last_time > HEARTBEAT_TIMEOUT:
    #                 if self.worker_dict[worker_port]['status'] != 'Dead':
    #                     LOGGER.warning(f"Worker {worker_port} marked as dead due to missed heartbeats.")
    #                     self.worker_dict[worker_port]['status'] = 'Dead'
    #                     self.reassign_tasks(worker_port)
    #         time.sleep(HEARTBEAT_INTERVAL)
    #     self.tcp_thread.join()
    #     print("heartbeat checker thread joined, shutting down")

    def handlemessage(self, message_dict):
        """Handle all messages."""
        if message_dict["message_type"] == "register":
            with self.lock:
                self.worker_dict[message_dict["worker_port"]] = {
                    'host': message_dict["worker_host"],
                    'status': 'Ready',
                    'last_heartbeat': 0,
                    'curr_task': ''
                }
                register_ack = {
                    "message_type": "register_ack"
                }
                tcp_client(message_dict["worker_host"],
                           message_dict["worker_port"],
                           register_ack)
                #print("ack sent to worker")

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
            #print(
                #f"""finished message recieved from:
                #{message_dict['worker_port']}""")
            worker_port = message_dict['worker_port']
            self.worker_dict[worker_port]['status'] = "Ready"
            self.finished_tasks += 1
            if self.num_tasks == self.finished_tasks:
                print("finished!")
                self.finished = True

    def reassign_tasks(self, worker_port):
        """Reassign tasks that were assigned to a dead worker"""
        LOGGER.info(f"Reassigning tasks for worker {worker_port}")
        for task in list(self.task_queue):
            if task.get('worker_port') == worker_port:
                task.pop('worker_port', None)
                self.task_queue.appendleft(task)
    def assign_tasks(self):
        """Assign tasks to workers."""
        while not self.signals["shutdown"] and not self.finished:
            if self.task_queue:
                #print(len(self.task_queue))
                assigned = False
                for worker_port, worker_info in self.worker_dict.items():
                    job = self.task_queue.popleft()
                    #print("looking for worker")
                    if worker_info['status'] == 'Ready':
                        worker_host = worker_info['host']
                        success = tcp_client(worker_host, worker_port, job)
                        if success:
                            print("worker assigned to task")
                            worker_info['status'] = 'Busy'
                            worker_info['task'] = job
                            assigned = True
                            worker_info['curr_task'] = job
                            #self.num_tasks += 1
                        else:
                            worker_info['status'] = 'Dead'
                            print("worker is dead")
                    if assigned is False:
                        print("coudn't assign task; reassigning")
                        self.task_queue.appendleft(job)
                # wait for worker to become available
            # print(f"total tasks: {self.num_tasks} finished tasks: {self.finished_tasks}" )
            # if self.num_tasks == self.finished_tasks:
            #     self.make_reduce_tasks(job)
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
            #print("input files", input_files)
            partitions = [[] for _ in range(job["num_mappers"])]

            for i, file in enumerate(input_files):
                #print("entering partition logic", i, file)
                task_id = i % job["num_mappers"]
                #print("task id", task_id)
                full_path = os.path.join(job["input_directory"], file)
                partitions[task_id].append(full_path)

            # create message_dict for each task_id and add to task_queue
            for taskid, files in enumerate(partitions):
                #print(f"Task {taskid}: {files}")
                message_dict = {
                    "message_type": "new_map_task",
                    "task_id": taskid,
                    "input_paths": files,
                    "executable": job["mapper_executable"],
                    "output_directory": tmpdir,
                    "num_partitions": job["num_reducers"]
                }
                self.num_tasks += 1
                self.task_queue.append(message_dict)
            print("num map tasks", self.num_tasks)
            self.assign_tasks()
            print("finished, entering reduce logic")
            self.make_reduce_tasks(job, tmpdir)

        # change? until job is completed?
        while not self.signals["shutdown"] and not self.finished_all:
            time.sleep(0.1)
            # DO MAP STAGE WORK THEN SET FINISHED TO TRUE
        LOGGER.info("Cleaned up tmpdir %s", tmpdir)

    def make_reduce_tasks(self,job, tmpdir):
        """Reduce tasks."""
        print("entering reduce tasks")
        self.num_tasks = 0
        self.finished_tasks = 0
        #print("reset num_tasks and finished tasks", self.num_tasks)
        reduce_tasks = [[] for _ in range(job["num_reducers"])]
        input_files = sorted(os.listdir(tmpdir))

        for file in input_files:
            #print(f"file from map task: {file}")
            part_num = int(file.split("part")[1])
            #print(f"partition numbers: {part_num}")
            reduce_tasks[part_num].append(os.path.join(tmpdir, file))

        for i , file in enumerate(reduce_tasks):
            #print(f"part_num: {i}, file {file}")
            reduce_task = {
                "message_type": "new_reduce_task",
                "task_id": i,
                "executable": job["reducer_executable"],
                "input_paths": file,
                "output_directory": job["output_directory"]
            }
            self.num_tasks += 1
            self.task_queue.append(reduce_task)
        self.finished = False
        #print("num reduce tasks", self.num_tasks)
        self.assign_tasks()
        print("finished everything yay")
        self.finished_all = True


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
