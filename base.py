import threading  # Ensure threading is imported
import logging
from typing import Final

class Worker:
    GROUP: Final = "worker"

    def __init__(self, crash=False, slow=False):
        self.crash = crash
        self.slow = slow
        self.thread = None

    def run(self, **kwargs):
        raise NotImplementedError("This method should be overridden by subclasses")

    def create_and_run(self, **kwargs):
        self.thread = threading.Thread(target=self.run, kwargs=kwargs, name="run")
        self.thread.start()
        logging.info(f"Worker thread {self.thread.name} started.")
