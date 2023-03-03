# import json

import paho.mqtt.client as mqtt
import threading
import time
from db_func import *
# from nmcli_func import*


class paho(threading.Thread):
    def __init__(self, interval=5, mqtt_server='localhost', mqtt_user='demo', mqtt_pass='demo123', mqtt_port=1883):
        threading.Thread.__init__(self, daemon=True)
        print("[MQTT] Initializing")
        # self.stopped = threading.Event()
        self.interval = interval
        self.client = mqtt.Client()
        self.active = False
        self.mqtt_status = 0
        # self.mode = mode
        self.set_address = mqtt_server
        self.mqtt_user = mqtt_user
        self.mqtt_pass = mqtt_pass
        self.mqtt_port = mqtt_port
        self.db = MySql()
        time.sleep(5)

    def close(self):
        self.active = False
        # self.stopped.clear()
        print("[MQTT] Connection closed")

    def parsing(self, raw_string, hmi_id):
        try:
            # self.db.db_connect()
            print('[PROGRAM] Parsing')
            data = str(raw_string).split(' ')[0]
            stable = 1 if str(data).split(',')[0] == 'ST' else 0
            weight = round(float(data.split(',')[1]), 3)
            # hmi = self.get_th(hmi_id)
            query = 'select hmi.*, sku.th_H, sku.th_L from hmi left join sku on hmi.sku_id = sku.id where hmi.id = %s' % hmi_id
            hmi = self.db.db_fetchone(query)
            th_H = hmi['th_H']
            th_L = hmi['th_L']
            status = 'UNDER'
            print(hmi)
            if weight >= th_L and weight <= th_H:
                status = 'PASS'
            elif weight < th_L:
                status = 'UNDER'
            elif weight > th_H:
                status = 'OVER'
            if stable:
                query = "update hmi set weight = %s, stable = %s, status='%s' where id =%s" % (
                    weight, stable, status, hmi_id)
                self.db.db_query(query)
            else:
                query = "update hmi set weight = %s, stable = %s, status='%s', sending = 0 where id =%s" % (
                    weight, stable, status, hmi_id)
                self.db.db_query(query)
            print('[MYSQL] Query : %s' % query)
        except KeyboardInterrupt:
            print("[PROGRAM] Forced Close")
            exit()
        except Exception as e:
            print("[MYSQL] Error reading data from MySQL table")
            print(e)
        # finally:
        #     # if connection.is_connected():
        #     # self.db.db_close()
        #     print("[MYSQL] connection is closed")

    def update_timeout(self, timeout, hmi_id):
        try:
            # self.db.db_connect()
            query = "update hmi set timeout = %s where id =%s" % (
                timeout, hmi_id)
            self.db.db_query(query)
        except KeyboardInterrupt:
            print("[PROGRAM] Forced Close")
            exit()
        except Exception as e:
            print("[MYSQL] Error reading data from MySQL table")
            print(e)
        # finally:
        #     # if connection.is_connected():
        #     # self.db.db_close()
        #     print("[MYSQL] connection is closed")

    def is_json(self, json_str):
        try:
            json_object = json.loads(json_str)
        except ValueError:
            return False
        return True

    def on_connect(self, client, userdata, flags, rc):
        print("[MQTT] Connected with result code", rc)
        # client.subscribe(self.device_id+"/#")
        client.subscribe("hmi/#")

    def on_message(self, client, userdata, msg):
        print("[MQTT] Message received: " + str(msg.topic) +
              " : " + str(msg.payload.decode("utf-8")))
        for i in range(5):
            if (str(msg.topic) == "hmi/%s/weight" % (i+1)):
                message = str(msg.payload.decode("utf-8"))
                self.parsing(message, i+1)
            elif (str(msg.topic) == "hmi/%s/timeout" % (i+1)):
                message = str(msg.payload.decode("utf-8"))
                self.update_timeout(message, i+1)

    def on_subscribe(self, mqttc, obj, mid, granted_qos):
        print("[MQTT] Subscribed: " + str(mid) + " " + str(obj))

    def on_publish(self, mqttc, obj, mid):
        print("[MQTT] Message published -> mid: " + str(mid))

    def run(self):
        self.active = True
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe
        self.client.on_publish = self.on_publish
        print("[MQTT] Connecting...")
        while self.active:
            self.db.db_connect()
            if self.db.db_status():
                try:
                    nms_address = self.set_address
                    mqtt_user = self.mqtt_user
                    mqtt_pass = self.mqtt_pass
                    mqtt_port = self.mqtt_port
                    self.client.username_pw_set(
                        username=mqtt_user, password=mqtt_pass)
                    connected = self.client.connect(nms_address, mqtt_port)
                    print(connected)
                    if connected == 0:
                        self.rc = 0
                        while self.rc == 0:
                            self.rc = self.client.loop()
                            self.mqtt_status = 1
                            if nms_address != self.set_address or mqtt_user != self.mqtt_user or mqtt_pass != self.mqtt_pass or mqtt_port != self.mqtt_port:
                                self.client.disconnect()
                                print("[MQTT] Server disconnected")
                                self.mqtt_status = 0
                                time.sleep(1)
                        if self.rc != 0:
                            print("[MQTT] Server disconnected")
                            self.mqtt_status = 0
                            time.sleep(1)
                except Exception as e:
                    print("[MQTT] Retrying to connect...")
                    print(e)
                    self.mqtt_status = 0
                    time.sleep(1)
                    continue
            else:
                print("[MYSQL] Retrying to connect...")
                self.db.db_reconnect()
                time.sleep(1)
            time.sleep(1/1000)
