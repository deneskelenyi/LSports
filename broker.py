import time

import paho.mqtt.client as mqtt

import config as app_config


broker = app_config.mqtt_host()


def on_disconnect(client, userdata, rc):
	client.connected_flag = False
	try:
		time.sleep(1)
		client.disconnect()
		client.connect(broker, app_config.mqtt_port(), 3)
		client.loop_start()
	except Exception:
		print("MQTT reconnect failed")


def on_message(client, userdata, message):
	return None


def on_connect(client, userdata, flags, rc):
	client.connected_flag = True
	if rc == 0:
		client.subscribe("lines_command", 2)
	else:
		print("connection fail")


def on_subscribe(mosq, obj, mid, granted_qos):
	pass


def init_client(id):
	try:
		client = mqtt.Client(client_id=id, clean_session=True, userdata=None, transport='tcp')
		client.connected_flag = False
		client.on_connect = on_connect
		client.on_message = on_message
		client.on_disconnect = on_disconnect
		client.connect(broker, app_config.mqtt_port())
		client.loop_start()
		client.publish("debug", id + " started")
	except Exception:
		client = None
	return client
