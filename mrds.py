from __future__ import annotations

import logging
from typing import Optional, Final

from redis.client import Redis

from base import Worker
from config import config


from redis.exceptions import ResponseError

class MyRedis:
  def __init__(self):
    self.rds: Final = Redis(host='localhost', port=6379, password=None,
                       db=0, decode_responses=False)
    self.rds.flushall()
    self.create_consumer_group()

  def create_consumer_group(self):
    try:
      self.rds.xgroup_create(config["IN"], Worker.GROUP, id="0", mkstream=True)
    except ResponseError as e:
      if "BUSYGROUP Consumer Group name already exists" in str(e):
        logging.info("Consumer group already exists")
      else:
        raise e

  def add_file(self, fname: str):
    self.rds.xadd(config["IN"], {config["FNAME"]: fname})

  def top(self, n: int) -> list[tuple[bytes, float]]:
    return self.rds.zrevrangebyscore(config["COUNT"], '+inf', '-inf', 0, n,
                                     withscores=True)

  def is_pending(self) -> bool:
    pending = self.rds.xpending(config["IN"], Worker.GROUP)
    return pending['pending'] > 0

  def restart(self, downtime: int):
    self.rds.shutdown(save=True)
    time.sleep(downtime)
    self.rds = Redis(host='localhost', port=6379, password=None, db=0, decode_responses=False)
    self.create_consumer_group()

  def read_file_name(self):
    try:
      return self.rds.xreadgroup(Worker.GROUP, 'consumer', {config["IN"]: '>'}, count=1)
    except ResponseError as e:
      if "NOGROUP" in str(e):
        self.create_consumer_group()
        return self.rds.xreadgroup(Worker.GROUP, 'consumer', {config["IN"]: '>'}, count=1)
      else:
        raise e

  def increment_word_count(self, word: str, count: int):
    self.rds.zincrby(config["COUNT"], count, word)

  def acknowledge_message(self, message_id: str):
    self.rds.xack(config["IN"], Worker.GROUP, message_id)