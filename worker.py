import logging
import sys
import time
from typing import Any

from base import Worker
from config import config
from mrds import MyRedis
import pandas as pd


class WcWorker(Worker):
  def run(self, **kwargs: Any) -> None:
    rds: MyRedis = kwargs['rds']

    while True:
      # Read a file name from the Redis stream
      logging.info("Attempting to read a file name from the Redis stream")
      response = rds.read_file_name()
      if not response:
        logging.info("No more files to process. Exiting.")
        break

      stream, messages = response[0]
      message_id, message_data = messages[0]
      file_name = message_data[config["FNAME"]]

      try:
        logging.info(f"Processing file: {file_name}")
        # Process the file
        df = pd.read_csv(file_name, encoding='ISO-8859-1', lineterminator='\n')
        df["text"] = df["text"].astype(str)
        word_counts = {}
        for text in df["text"]:
          if text == '\n':
            continue
          for word in text.split(" "):
            if word not in word_counts:
              word_counts[word] = 0
            word_counts[word] += 1

        # Update word counts in Redis
        for word, count in word_counts.items():
          logging.info(f"Incrementing word count: {word} by {count}")
          rds.increment_word_count(word, count)

        # Acknowledge the message
        rds.acknowledge_message(message_id)
        logging.info(f"Acknowledged message: {message_id}")

      except Exception as e:
        logging.error(f"Error processing file {file_name}: {e}")

      if self.crash:
        logging.critical(f"CRASHING!")
        sys.exit()

      if self.slow:
        logging.critical(f"Sleeping!")
        time.sleep(1)

    logging.info("Exiting")