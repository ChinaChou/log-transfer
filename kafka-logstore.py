#!/usr/bin/python
#coding:utf-8
import json,pickle,sys,os,datetime,copy
from kafka import KafkaConsumer

reload(sys)
sys.setdefaultencoding("utf-8")

class LogStore:
    register_file = "/root/register.dat"
    def __init__(self,kafka_server_list,topic):
        self.kafka_server_list = kafka_server_list
        self.topic = topic
        self.consumer = self.connect_kafka()
        self.load_info()
        self.opened_file = {}
        
    def load_info(self):
        '''
        1、确保register_file存在；
        2、将其中的内容读取到self.state_file字典中；
        '''
        if not os.path.isfile(self.register_file):
            base_dir,_ = self.register_file.rsplit('/',1)
            if not os.path.isdir(base_dir):
                os.makedirs(base_dir)
            with open(self.register_file,'w') as f:
                pass
        with open(self.register_file,'r') as f:
            try:
                self.state_file = pickle.load(f)
            except Exception,e:
                self.state_file = {}

    def save_info(self):
        with open(self.register_file,'w') as f:
            pickle.dump(self.state_file,f)
    
    def open_logfile(self,file_path):
        f = open(file_path,'a')
        self.opened_file[file_path] = f

    def close_logfile(self,file_path):
        self.opened_file[file_path].close()
        self.opened_file.pop(file_path)
        self.state_file.pop(file_path)
        base_dir,file_name = file_path.rsplit('/',1)
        os.rename(file_path,"{0}/{1}_{2}".format(base_dir,file_name,datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")))

    def write_content(self,file_path,file_content,file_offset):
        self.opened_file[file_path].write(file_content+"\n")
        self.opened_file[file_path].flush()
        self.state_file[file_path] = file_offset

    def save_content(self,file_path,file_offset,file_content):
        if not os.path.isfile(file_path):
            base_dir,_ = file_path.rsplit('/',1)
            if not os.path.isdir(base_dir):
                os.makedirs(base_dir)
            self.open_logfile(file_path)
        if file_path not in self.opened_file:
            self.open_logfile(file_path)
            #self.opened_file[file_path].write(file_content+"\n")
            #self.state_file[file_path] = file_offset
            self.write_content(file_path,file_content,file_offset)
        else:
            if file_offset >= self.state_file.get(file_path,0):
                #self.opened_file[file_path].write(file_content+"\n")
                self.write_content(file_path,file_content,file_offset)
            else:
                '此处日志滚动了'
                self.close_logfile(file_path)
                self.open_logfile(file_path)
                #self.opened_file[file_path].write(file_content+"\n")
                #self.state_file[file_path] = file_offset        
                self.write_content(file_path,file_content,file_offset)

    def connect_kafka(self):
        try:
            consumer = KafkaConsumer(self.topic,bootstrap_servers=self.kafka_server_list,value_deserializer=lambda v:json.loads(v,encoding="utf-8"))
        except Exception,e:
            print(e)
        else:
            return consumer

    def consume_msg(self):
        if self.consumer:
            count = 0
            for msg in self.consumer:
                content = msg.value
                file_path = content.get('source')
                file_offset = content.get('offset')
                file_content = content.get('message')
                print("#"*80)
                print(file_content)
                self.save_content(file_path,file_offset,file_content)
                print("#"*80)
                count += 1
                if count >= 1000:
                    self.save_info()
                    count = 0


if __name__ == '__main__':
    store = LogStore(['192.168.8.146:9092'],'log_topic')
    store.consume_msg()
