#coding:utf-8
import time
import os
import pickle
import json
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from kafka import KafkaProducer
from kafka.errors import KafkaError


log_base_dir = "/data/logs"
envs = os.environ
logger = logging.Logger("fxd")
formatter = logging.Formatter(fmt='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.WARN)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

class MyFileHandler(FileSystemEventHandler):
    fxd_service_log_filename = envs.get("LOGFILE_NAME","service.log,catalina.out").split(",")
    register_file = os.path.dirname(__file__)+"/registry.data"
    host_name = envs.get("HOST_NAME")
    log_topic = envs.get("LOG_TOPIC")
    kafka_server = envs.get("KAFKA_SERVER")
    watched_log_files = []
    opened_files = {}
    read_lines = 70

    def __init__(self, *args, **kwargs):
        self._create_register_file()
        self._scan_log_dir()
        self._conn_kafka()
        self._open_watched_files()
        super(MyFileHandler, self).__init__(*args, **kwargs)

    def _create_register_file(self):
        if not os.path.isfile(self.register_file):
            with open(self.register_file,'wb') as f:
                pickle.dump({},f)

    def _scan_log_dir(self,log_dir=log_base_dir):
        fg = os.fwalk(log_dir)
        for t in fg:
            base_dir = t[0]
            files = t[2]
            for service_log_filename in self.fxd_service_log_filename:
                if service_log_filename in files:
                    self.watched_log_files.append("{0}/{1}".format(base_dir,service_log_filename))
        fg.close()

    def _conn_kafka(self):
        self.producer = KafkaProducer(
            bootstrap_servers=["{0}:9092".format(self.kafka_server)],
	    max_request_size=20971520,
            value_serializer=lambda v:json.dumps(v,ensure_ascii=False).encode(),
        )

    def _open_watched_files(self):
        #open scaned file
        for f in self.watched_log_files:
            data = {
                "hostname":self.host_name,
                "action":"create",
                "src":f,
                "dst":"",
                "cur_post":0,
                "contents":""
            }
            fobj = open(f, "rb")
            if os.path.getsize(self.register_file) > 0:
                with open(self.register_file,'rb') as f2:
                    register_status = pickle.load(f2)
                if f in register_status:
                    fobj.seek(register_status[f],0)
                else:
                    logger.info("Found unwatched file {0}".format(f))
                    self.sendMsg(data)
            else:
                logger.info("Found unwatched file {0}".format(f))
                self.sendMsg(data)
            self.opened_files[f] = fobj

    def sendMsg(self,data):
        future = self.producer.send(self.log_topic,data)
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError as e:
            logger.error("Failed to sync-send data to kafka in 10 seconds " + str(e))
            return None
        else:
            return True

    def _save_position(self,src_path,current_position):
        with open(self.register_file,'r+b') as f:
            ret = pickle.load(f)
            ret[src_path] = current_position
            f.seek(0)
            pickle.dump(ret,f)

    def _delete_position(self,src_path):
        with open(self.register_file,'r+b') as f:
            register_status = pickle.load(f)
            register_status.pop(src_path)
            f.seek(0)
            pickle.dump(register_status,f)

    def on_created(self,event):
        self._scan_log_dir()
        if event.src_path in self.watched_log_files:
            fobj = open(event.src_path,'rb')
            self.opened_files[event.src_path] = fobj
            data = {
                "hostname":self.host_name,
                "action":"create",
                "src":event.src_path,
                "dst":"",
                "cur_post":0,
                "contents":""
            }
            ret = self.sendMsg(data)
            if ret:
                logger.info("Create a new file {0}".format(event.src_path))

    def on_deleted(self,event):
        if event.src_path in self.watched_log_files:
            fobj = self.opened_files.get(event.src_path)
            if fobj:
                fobj.close()
            self.opened_files.pop(event.src_path)
            self._delete_position(event.src_path)
            data = {
                "hostname":self.host_name,
                "action":"delete",
                "src":event.src_path,
                "dst":"",
                "cur_post":-1,
                "contents":""
            }
            ret = self.sendMsg(data)
            if ret:
                logger.info("Delete file {0}".format(event.src_path))

    def on_modified(self,event):
        '''
        Get the modified content and send them to kafka
        save current cursor position
        '''
        if event.src_path in self.watched_log_files:
            logger.info("Modify file {0}".format(event.src_path))
            fobj = self.opened_files.get(event.src_path)
            if fobj:
                contents = ''
                for n in range(0,self.read_lines):
                    ret = fobj.readline().decode()
                    if not ret:
                        break
                    contents += ret
                current_cursor_pos = fobj.tell()
                if contents:
                    data = {
                        "hostname":self.host_name,
                        "action":"modify",
                        "src":event.src_path,
                        "dst":"",
                        "cur_pos": current_cursor_pos,
                        "contents": contents
                    }
                    ret = self.sendMsg(data)
                    if ret:
                        self._save_position(event.src_path,current_cursor_pos)            

    def on_moved(self,event):
        if event.src_path in self.watched_log_files:
            fobj = self.opened_files.get(event.src_path)
            data = {
                "hostname":self.host_name,
                "action":"move",
                "src":event.src_path,
                "dst":event.dest_path,
                "cur_post":-2,
                "contents":""
            }
            ret = self.sendMsg(data)
            if ret:
                if fobj:
                    fobj.close()
                self.opened_files.pop(event.src_path)
                self._delete_position(event.src_path)


if __name__ == '__main__':
    observer = Observer()
    my_file_handler = MyFileHandler()
    observer.schedule(my_file_handler,log_base_dir,recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()