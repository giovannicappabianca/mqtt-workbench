import paho.mqtt.client as mqtt 
import json 
import logging, time

class MqttClient(object):
    def __init__(self):
        # read from json file broker and client id
        with open('config.json') as f:
            data = json.load(f)
            self.broker = data["broker"]
            self.clientId = data["clientId"]
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
        self.logger.info("Subscribed %s", userdata)

    def on_message(self, client, userdata, message):  # get message from mqtt broker 
        self.logger.info(("New message received: %s", str(message.payload.decode("utf-8"))))

    def on_disconnect(self, client, userdata, rc):  # disconnect to mqtt broker function
        self.logger.info(("Client disconnected OK"))    

    def connect(self):
        
        self.logger.info("Connecting to broker %s",self.broker)
    
        self.client.loop_start()
        self.client.connect(self.broker)    
        while not self.client.connected_flag and not client.bad_connection_flag: 
            self.logger.info("Trying to connect")
            time.sleep(1)
        if self.client.bad_connection_flag:
            self.client.loop_stop()

        


myClient = MqttClient()
myClient.connect()

