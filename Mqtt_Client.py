import paho.mqtt.client as mqtt 
import json 
import logging

class MqttClient(object):
    def __init__(self):
        # read from json file broker and client id
        with open('config.json') as f:
            data = json.load(f)
            self.broker = data["broker"]
            self.clientId = data["clientId"]
        #building logger
        self.logger = self.__manageLogging()
        
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

    def connect(self):
        client= mqtt.Client(self.clientId)
        self.logger.info("Connecting to broker %s",self.broker)
        client.connect(self.broker)


myClient = MqttClient()
myClient.connect()

