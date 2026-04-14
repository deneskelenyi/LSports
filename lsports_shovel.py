#!/usr/bin/python3
import os,sys,psutil
import pika
import ssl
import json, pymemcache
from colorama import Fore, Back, Style
from queue import Queue
import time
import threading
import queue
from concurrent.futures import ThreadPoolExecutor
import random
from datetime import timedelta, date, datetime
import requests
import multiprocessing as mp
import broker as mqb
import config as app_config

app_config.load_env()

manager = mp.Manager()

chandict=manager.dict()
mq_client=mqb.init_client("lsports_shovel")

#q = Queue(maxsize = 999999999)
q = mp.Queue(1000000)
q2 = mp.Queue(1000000)

threads = []
lcon2 = None
lchan2 = None
lcon1 = None
lchan1 = None

rcon = None
rchan = None
reconCount = 0
reconTotalCount = 0
channels = {}

def enablePackage():
    headers = app_config.lsports_headers()
    url = app_config.lsports_api_base_url()
    urlParams = app_config.lsports_api_query()
    reqUrl = url+'EnablePackage'+urlParams
    requests.get(reqUrl, headers=headers).close()

def amq_connect(hostName):
    params = app_config.local_rabbitmq_params(heartbeat=20)
    params.host = hostName
    con = pika.BlockingConnection(params)
    return con

primary_host, secondary_host = app_config.shovel_publish_hosts()
ch1 = amq_connect(primary_host).channel()
ch2 = amq_connect(secondary_host).channel()

def on_command_message(ch, method, properties, body):
    pass
    
def on_discard_message(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)

def on_message(ch, method, properties, body):
    global channels, q,q2,ch1,ch2,chandict
    
    # The shovel only relays raw feed messages into the local queue.
    try:
        q.put_nowait(body.decode("utf-8"))
    except Exception:
        pass
        
    ch.basic_ack(delivery_tag=method.delivery_tag)

def runClient_lsports():
    global rcon, rchan,reconCount, reconTotalCount
    context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    firstRun=True
    while(True):
        time.sleep(5)   
        reconTotalCount += 1     
        reconCount +=1
        if reconCount >5 or firstRun:
            enablePackage()            
            reconCount = 0
            firstRun = False
        try:            
            now = datetime.now()
            dt_sting = now.strftime("%d/%m/%Y %H:%M:%S")
            print(Fore.BLUE,dt_sting,"XXXXXXXXXXXXXXXXXXXXXX Connecting...")			
            rcon = pika.BlockingConnection(app_config.lsports_remote_rabbitmq_params(heartbeat=20))
            print(Fore.LIGHTGREEN_EX,dt_sting,"Connected")
            rchan = rcon.channel()
            rchan.basic_qos(prefetch_count=100)
            rchan.basic_consume(app_config.lsports_queue_name(), on_message)
            print(Fore.GREEN,dt_sting,"Consuming")
            try:
                rchan.start_consuming()
                reconCount = 0
            except KeyboardInterrupt:
                rchan.stop_consuming()
                rcon.close()
                break
        except pika.exceptions.ConnectionClosedByBroker:
            # Uncomment this to make the example not attempt recovery
            # from server-initiated connection closure, including
            # when the node is stopped cleanly
            #
            # break
            continue
        # Do not recover on channel errors
        except pika.exceptions.AMQPChannelError as err:
            print("Caught a channel error: {}, stopping...".format(err))
            break
        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:
            print("Connection was closed, retrying...")
            continue

def publisher(params):
    (hostName,q,idx,target_qsize) = params
    nowTS=0
    try:
        con = amq_connect(hostName)
        ch = con.channel()
        q2 = ch.queue_declare(queue='lsports',passive=True)
    except Exception as ex1:
        pass
    try:
        while True:     
            try:   
                time.sleep(0.0001)
                elapsed=time.time()-nowTS
                if elapsed > 60:
                    nowTS = time.time()

                connected = False
                try:
                    if ch.connection.is_open:                
                        connected = True
                    else:
                        print(Fore.RED, "connection closed to",hostName)
                except Exception as ex:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)
                try:
                    if not connected:
                        print(Fore.RED,"Not connected")
                        con = amq_connect(hostName)
                        ch = con.channel()
                        ch.confirm_delivery()
                        q2 = ch.queue_declare(queue='lsports',passive=True)
                        
                    
                    if not q.empty():
                        tit = q.get()

                        ch.basic_publish('exch_lsports',
                            'lsports',
                            tit,
                            pika.BasicProperties(content_type='text/plain'
                            )                            
                        )
                        target_qsize=q2.method.message_count
                    
                except Exception as ex:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)
            except Exception as ex:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)

                
                #print(".",end='')
    except Exception as e2:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)





def runClient_plannatech_test():
    global rcon, rchan
    context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    #ch.basic_consume(queue="dds-BetSlipRTv4",
    #				on_message_callback=on_message, auto_ack=True)
    while(True):
        time.sleep(1)        
        try:            
            now = datetime.now()
            dt_sting = now.strftime("%d/%m/%Y %H:%M:%S")
            print(Fore.BLUE,dt_sting,"XXXXXXXXXXXXXXXXXXXXXX Connecting plannatech test...")			
            rcon = pika.BlockingConnection(app_config.local_rabbitmq_params(heartbeat=20))

            print(Fore.LIGHTGREEN_EX,dt_sting,"Connected")
            rchan = rcon.channel()
            rchan.basic_qos(prefetch_count=10)
            #channel.basic_consume('_4620_', on_message)
            rchan.basic_consume(queue="garbagetest",on_message_callback=on_message)			
            #channel.basic_consume(queue="_4620_",on_message_callback=on_message_client, auto_ack=True) 
            print(Fore.GREEN,dt_sting,"Consuming")
            try:
                rchan.start_consuming()
                
            except KeyboardInterrupt:
                rchan.stop_consuming()
                rcon.close()
                break
        except pika.exceptions.ConnectionClosedByBroker:
            # Uncomment this to make the example not attempt recovery
            # from server-initiated connection closure, including
            # when the node is stopped cleanly
            #
            # break
            continue
        # Do not recover on channel errors
        except pika.exceptions.AMQPChannelError as err:
            print("Caught a channel error: {}, stopping...".format(err))
            break
        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:
            print("Connection was closed, retrying...")
            continue


def runClient_discard(params):
    (hostName,queueName) = params
    context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    #ch.basic_consume(queue="dds-BetSlipRTv4",
    #				on_message_callback=on_message, auto_ack=True)
    while(True):        
             
        try:                        
            now = datetime.now()
            dt_sting = now.strftime("%d/%m/%Y %H:%M:%S")
            print(Fore.BLUE,dt_sting,"XXXXXXXXXXXXXXXXXXXXXX Connecting...")			
            params = app_config.local_rabbitmq_params(heartbeat=20)
            params.host = hostName
            lcon = pika.BlockingConnection(params)
            print(Fore.LIGHTGREEN_EX,dt_sting,"Connected")
            lchan = lcon.channel()
                    
            lchan.basic_qos(prefetch_count=100)
            lchan.basic_consume(queue="plannatech",on_message_callback=on_discard_message)			
            print(Fore.GREEN,dt_sting,"Consuming discard channel")
            try:
                lchan.start_consuming()
                
            except KeyboardInterrupt:
                lchan.stop_consuming()
                lcon.close()
                break
        except pika.exceptions.ConnectionClosedByBroker:
            # Uncomment this to make the example not attempt recovery
            # from server-initiated connection closure, including
            # when the node is stopped cleanly
            #
            # break
            continue
        # Do not recover on channel errors
        except pika.exceptions.AMQPChannelError as err:
            print("Caught a channel error: {}, stopping...".format(err))
            break
        # Recover on all other connection errors
        except pika.exceptions.AMQPConnectionError:
            print("Connection was closed, retrying...")
            continue
        time.sleep(100)     

target_qsize = 0


if sys.argv[1] == 'shovel':
    
    hostName1, hostName2 = app_config.shovel_publish_hosts()
    
    t = threading.Thread(target=publisher,args=((hostName1,q,1,target_qsize),)) #, args=(json.loads(body))
    t.name = "publisher 1"
    t.start()
    threads.append(t)
    time.sleep(1)
    
    t = threading.Thread(target=publisher,args=((hostName1,q,2,target_qsize),)) #, args=(json.loads(body))
    t.name = "publisher 2"
    t.start()
    threads.append(t)
    time.sleep(1)
    time.sleep(2)
    
    t = threading.Thread(target=runClient_lsports) #, args=(json.loads(body))
    t.name = 'runClient_lsports'
    t.start()
    threads.append(t)

    
    nowTS = 0
    while True:
        time.sleep(0.01)
        try:
            elapsedTS=time.time()-nowTS	
            if elapsedTS>5:
            #print(elapsedTS)
                stat = {}
                tstat = []
                nowTS = round(time.time())
                for t in threads:            
                    #print(t.name,t.isAlive())                    
                    thisThread = {}
                    thisThread["name"] = t.name 
                    thisThread["status"] = t.is_alive()
                    thisThread["lastSeen"] = round(time.time())
                    tstat.append(thisThread)
                stat["tstat"] = tstat
                stat["qsize"] = q.qsize()
                stat["target_qsize"] = target_qsize
                stat["reconCount"] = reconCount
                stat["reconTotalCount"] = reconTotalCount
                #print(json.dumps(tstat))
                mq_client.publish('shovel/lsports',json.dumps(stat,indent=2),retain=True)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)


if sys.argv[1] == 'discard':
    t = threading.Thread(target=runClient_discard,args=(('localhost',sys.argv[2]),)) #, args=(json.loads(body))
    t.start()
    threads.append(t)
