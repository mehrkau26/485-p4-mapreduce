

# use deque data structure
class Job:
    def next_task(self):
        """Return the next pending task to be assigned to a Worker."""

    def task_reset(self, task):
        """Re-enqueue a pending task, e.g., when a Worker is marked dead."""

    def task_finished(self, task):
        """Mark a pending task as completed."""

    # Add constructor and other methods