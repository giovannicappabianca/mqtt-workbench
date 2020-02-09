import paho.mqtt.client as mqtt 
import json 
import logging, time, threading
import sched, datetime

class MqttClient(object):
    def __init__(self):
        # read from json file broker and client id
        self._set_parameters()
        #building logger
        self.logger = self.__manageLogging()
        #building client
        self.client= mqtt.Client(self.clientId)
        self.client.connected_flag = False
        self.client.bad_connection_flag=False
        # initialize callback
        self.client.on_connect=self.on_connect
        self.client.on_message=self.on_message     
        self.client.on_subscribe=self.on_subscribe
        self.client.on_disconnect = self.on_disconnect
        self.client.on_log = self.on_log
        #scheduling auto update
        self.scheduler = sched.scheduler(time.time, time.sleep)
        self._setup(self.interval, self._set_parameters)
        self.x = threading.Thread(target=self._run_schedule(), args=(1,), daemon=True)
        self.x.start()
        
    def __manageLogging(self):
        logger = logging.getLogger('mqtt_connection')
        logger.root.setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
        # create file handler which logs messages
        fh = logging.FileHandler('mqtt_connection.log')
        fh.setLevel(logging.DEBUG)
        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(fh)
        logger.addHandler(ch)
        return logger

    def on_connect(self, client, userdata, flags, rc):
        switch = {
            0: "Connection successful",
            1: "Connection refused – incorrect protocol version",
            2: "Connection refused – invalid client identifier",
            3: "Connection refused – server unavailable",
            4: "Connection refused – bad username or password",
            5: "Connection refused – not authorised",
        }
        self.logger.info(switch.get(rc, "Connection refused - Generic error on connection"))
        if rc == 0:
            client.connected_flag=True
        else:
            client.bad_connection_flag = True

    def on_publish(self, client, userdata, mid):
        self.logger.info(mid)
    
    def on_subscribe(self, client, userdata, mid, granted_qos):  # subscribe to mqtt broker
        self.logger.info("Subscribed %s", mid)

    def on_message(self, client, userdata, message):  # get message from mqtt broker 
        self.logger.info("New message received: %s", str(message.payload.decode("utf-8"))) 
        print("message topic=",message.topic)
        print("message qos=",message.qos)
        print("message retain flag=",message.retain)

    def on_disconnect(self, client, userdata, rc):  # disconnect to mqtt broker function
        self.logger.info(("Client disconnected OK"))    

    def connect(self):
        
        self.logger.info("Connecting to broker %s",self.broker)
    
        self.client.loop_start()
        self.client.connect(self.broker)    
        while not self.client.connected_flag and not self.client.bad_connection_flag: 
            self.logger.info("Trying to connect")
            time.sleep(1)
        if self.client.bad_connection_flag:
            self.client.loop_stop()

    def publish(self, topic, payload, qos, retain):
        self.client.publish(topic, payload, qos, retain)
        self.logger.info("published %s on topic %s with QoS %d. Retain %r", topic, payload, qos, retain)

    def subscribe(self, topic, QoS):
        self.client.subscribe(topic, QoS)

    def getClient(self):
        return self.client
    
    def on_log(self, client, userdata, level, buf):
        self.logger.debug("mqtt client log: %s",buf)                                     
                  
    def _setup(self, interval, action, actionargs=()):                             
        action(*actionargs)                                                       
        self.scheduler.enter(interval, 1, self._setup,                             
                        (interval, action, actionargs))                           
    def _run_schedule(self):
        self.scheduler.run()

    def _set_parameters(self):
      with open('config.json') as f:
            data = json.load(f)
            self.broker = data["broker"]
            self.clientId = data["clientId"]
            self.interval = data["autoupdateInterval"]



#Application Sample
myClient = MqttClient()
myClient.connect()
myClient.getClient().loop_start()
myClient.subscribe("home/light", 0)
myClient.publish("home/light", "OFF", 0, False)
time.sleep(24) # wait
myClient.connect()
myClient.publish("home/light", "ON", 0, False)
time.sleep(5) # wait
myClient.getClient().loop_stop() #stop the loop
