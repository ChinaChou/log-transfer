#coding:utf-8
import json
import os
import shutil
import datetime
import logging
import re
from kafka import KafkaConsumer


envs = os.environ
logger = logging.Logger("fxd")
formatter = logging.Formatter(fmt='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.WARN)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

class LogStore():
    service_log_filename = envs.get("LOGFILE_NAME","service.log,catalina.out").split(",")
    log_base_dir = os.path.dirname(__file__) + "/logs"
    kafka_server = envs.get("KAFKA_SERVER")
    log_topic = envs.get("LOG_TOPIC")
    watched_log_files = set()
    opened_log_files = {}

    def __init__(self):
        self._scan_base_log_dir()
        self._open_watched_log_file()

    def _scan_base_log_dir(self):
        fg = os.fwalk(self.log_base_dir)
        for t in fg:
            parent_dir = t[0]
            files = t[2]
            for service_log_filename in self.service_log_filename:
                if service_log_filename in files:
                    self.watched_log_files.add(parent_dir + "/" + service_log_filename)

    def _open_watched_log_file(self):
        for f in self.watched_log_files:
            if f not in self.opened_log_files:
                fobj = open(f,'a')
                self.opened_log_files[f] = fobj

    def create_file(self,file_path):
        parent_dir = file_path.rstrip(self.service_log_filename)
        logger.info(parent_dir)
        if not os.path.isdir(parent_dir):
            os.makedirs(parent_dir)
        if not os.path.isfile(file_path):
            fobj = open(file_path,'a')
            self.opened_log_files[file_path] = fobj
        if not file_path in self.watched_log_files:
            self.watched_log_files.add(file_path)

    def modify_file(self,file_path,contents):
        logger.info('opened_log_files = {0}'.format(self.opened_log_files))
        if not os.path.isfile(file_path):
            logger.warn("file {0} does not exist, so create it".format(file_path))
            fobj = open(file_path,'a')
            self.opened_log_files[file_path] = fobj
            self.watched_log_files.add(file_path)
        self.opened_log_files[file_path].write(contents)
        self.opened_log_files[file_path].flush()

    def move_file(self,src_file,dst_file):
        if src_file in self.watched_log_files:
            logger.info("Move {0} to {1}".format(src_file,dst_file))
            shutil.move(src_file,src_file + "-{0}".format(datetime.datetime.now().strftime('%Y%m%d_%H%M%S')))
            self.watched_log_files.remove(src_file)
            self.opened_log_files.pop(src_file)
        else:
            logger.warn("file {0} does not in watched files".format(src_file))

    def delete_file(self,file_path):
        if file_path in self.watched_log_files:
            self.watched_log_files.remove(file_path)
            self.opened_log_files.pop(file_path)
            #Do not delete file really
            #os.remove(file_path)

    def consume(self):
        consumer = KafkaConsumer(
            self.log_topic,
            group_id="service-log-consumer",
            bootstrap_servers=["{0}:9092".format(self.kafka_server)],
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode()),
        )
        for msg in consumer:
            try:
                hostname = msg.value["hostname"]
                action = msg.value["action"]
                src = msg.value["src"]
                dst = msg.value["dst"]
                contents = msg.value["contents"]
            except Exception as e:
                logger.error("Message format is invalided" + str(e))
            else:
                if src:
                    src_file = self.log_base_dir + "/" + hostname + "/" + src.split("/opt/logs/")[-1]
                    logger.info('src_file = {0}'.format(src_file))
                if dst:
                    dst_file = self.log_base_dir + "/" + hostname + "/" + dst.split("/opt/logs/")[-1]
                if action == "create":
                    logger.info("Create {0} event".format(src_file))
                    self.create_file(src_file)
                elif  action == "modify":
                    logger.info("Modify {0} event".format(src_file))
                    self.modify_file(src_file,contents)
                elif action == "move":
                    logger.info("Move {0} to {1} event".format(src_file,dst_file))
                    self.move_file(src_file,dst_file)
                elif action == "delete":
                    logger.info("delete event happened")
                    self.delete_file(src_file)
                else:
                    logger.error("No action matched {0}".format(action))

if __name__ == "__main__":
    ls = LogStore()
    ls.consume()