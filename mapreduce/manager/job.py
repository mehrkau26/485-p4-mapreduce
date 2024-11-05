
# use deque data structure
class Job:
    def __init__(self, message_dict):
        if message_dict["message_type"] == "new_manager_job":
            self.input_dir = message_dict["input_directory"] #string
            self.mapper_exec = message_dict["mapper_executable"] #string
            self.num_mappers = message_dict["num_mappers"] #int
            self.num_reducers = message_dict["num_reducers"] #int
            if message_dict["output_directory"] != '':
                del message_dict["output_directory"]
            print("testing message_dict output", message_dict)
       #create temp dir
       #assign taskid
 
    def next_task(self):
        """Return the next pending task to be assigned to a Worker."""

    def task_reset(self, task):
        """Re-enqueue a pending task, e.g., when a Worker is marked dead."""

    def task_finished(self, task):
        """Mark a pending task as completed."""

    def execute_job(self):
        print("execute")
        #partion files
        #assign task