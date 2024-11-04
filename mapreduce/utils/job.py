
# use deque data structure
class Job:
    def __init__(self, job_queue):
        for item in job_queue:
            print("JOB!!!!", item)

    def next_task(self):
        """Return the next pending task to be assigned to a Worker."""

    def task_reset(self, task):
        """Re-enqueue a pending task, e.g., when a Worker is marked dead."""

    def task_finished(self, task):
        """Mark a pending task as completed."""
    



    # Add constructor and other methods