"""Job class."""


# use deque data structure
class Job:
    """Job class."""

    def __init__(self, message_dict):
        """Assign job message."""
        if message_dict["message_type"] == "new_manager_job":
            self.input_dir = message_dict["input_directory"]
            self.mapper_exec = message_dict["mapper_executable"]
            self.num_mappers = message_dict["num_mappers"]
            self.num_reducers = message_dict["num_reducers"]
            if message_dict["output_directory"] != '':
                del message_dict["output_directory"]
            print("testing message_dict output", message_dict)
        # create temp dir
        # assign taskid

    def next_task(self):
        """Return the next pending task to be assigned to a Worker."""

    def task_reset(self, task):
        """Re-enqueue a pending task, e.g., when a Worker is marked dead."""

    def task_finished(self, task):
        """Mark a pending task as completed."""

    def execute_job(self):
        """Do job."""
        print("execute")
        # partion files
        # assign task
