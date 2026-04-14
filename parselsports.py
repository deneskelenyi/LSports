#!/usr/bin/python3

from dataclasses import dataclass
import sqlite3

from wsgiref.headers import tspecials
import dateutil.parser
from datetime import timedelta, date, datetime
import time
import json
import os
import sys
import psutil
import re
import errno
import mysql.connector
from threading import local
from colorama import Fore, Back, Style
import argparse
import urllib.request
import requests
import ssl
#import dbcon
import threading
from requests.structures import CaseInsensitiveDict
import multiprocessing as mp
import pymemcache
import copy
import random
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
import schedule
import config as app_config

app_config.load_env()
mcc = pymemcache.Client(app_config.memcache_address())
import broker as mqb
import pika
import static_shared as statvar

match_status = statvar.match_status
msg_types = statvar.msg_types
bookIDs = statvar.bookIDs

cparams = app_config.mysql_connect_kwargs()

try:
    mydb = app_config.create_mysql_connection()
    curs_prep = mydb.cursor(prepared=True)
    curs_dict = mydb.cursor(dictionary=True)
except Exception as ex:
    print(ex)



host1='http://client.lsports.eu'
package = app_config.lsports_package_id()
headers = app_config.lsports_headers()
url = app_config.lsports_api_base_url()
urlParams = app_config.lsports_api_query()

manager = mp.Manager()
sportDict = {}
mq_client=mqb.init_client("lsports_client")
mq_client.publish('lsports_status',"hello")
msgCount = 0
msgTypeCount = manager.dict()
oddTypeCount = manager.dict()

maxContestId = 0
maxOutrightId = 0 
print("This PID:",os.getpid())
pidFileName = "runner.pid"
if sys.argv[1]=='streamclient':
    pidFileName = "streamclient.pid"
if sys.argv[1]=='stream':
    pidFileName = "stream.pid"
if sys.argv[1]=='client':
    pidFileName = "client.pid"


with open(pidFileName, 'w', encoding='utf-8') as f:
    f.write(str(os.getpid()))



def findSmallestQueue(qs):
    try:
        smallest=9999999999	
        smallestId=0
        
        for qi in sorted(qs,key=lambda x: random.random()):
            #print(qi,qs[qi].qsize())
            thisSize=qs[qi].qsize()
            if thisSize<smallest:
                smallest=thisSize
                smallestId=qi
    except Exception as ex:
        #print(Fore.RED,"Exception",bookName,bookID,ex)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)    
    #if qid!=smallestId:
    #	#print(qid,smallestId,qs[qid].qsize(),smallest)
    #	pass

    return smallestId

def publishHandler(params):
    (thisQue,isFF,phqid,pstatus_p) = params
    thisId="publisher_"+str(phqid)
    c = None
    try:
        c=connect_amq_local(thisId)
        ch = c.channel()
    except Exception as ex:
        pass
    bigdoc=[]
    nowTS=time.time()
    nowTS_2=time.time()
    last_proctime = 0
    msgCnt=0
    msgCntHold=0
    time.sleep(0.01)
    lastPush=0
    isPushing=0
    lastPushSize=0
    pushTS=0
    elapsedTS = 0
    while True:
        elapsedTS=time.time()-nowTS    
        elapsedTS_2=time.time()-nowTS_2	
        if elapsedTS>5:
            #print(elapsedTS)
            msgPersec=(msgCnt-msgCntHold)/elapsedTS
            msgCntHold = msgCnt
            nowTS=time.time()
            d_msg={
                            
                "msgCnt":msgCnt,
                "msgPersec":round(msgPersec,2),
                "queue size":thisQue.qsize(),				
                "process id":phqid,				
                "last proctime":round(last_proctime),
                "bigdoc pending":len(bigdoc),	
                "lastPush ago":round(lastPush),
                "push in": round(elapsedTS_2),
                "lastPushSize":lastPushSize,
                "isPushing":isPushing,
                "now":time.time(),
                "pushTS":pushTS
            }
            pstatus_p[thisId]=d_msg
        if not thisQue.empty():                                
            try:
                qitem=thisQue.get()
                msgCnt+=1
                #print(Fore.BLUE,".",end='')
                try:
                    while not ch.connection.is_open:
                        try:
                            print("reconnecting",thisId)
                            c=connect_amq_local(thisId)
                            ch = c.channel()
                            time.sleep(1)
                        except Exception as exrecon:
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            print(exc_type, fname, exc_tb.tb_lineno)
                        #print(type(qitem))
                    ch.basic_publish('',
                        'lines_db_contest',
                        json.dumps(qitem),
                        pika.BasicProperties(content_type='text/plain',
                        delivery_mode=1)
                        )
                except Exception as exPika:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)
                #print(Fore.GREEN,".",end='')
                
                #for item in qitem:
                #    bigdoc.append(item)
            except:                
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
        else:
            time.sleep(0.0001)


def publishHandler_(params):
    (thisQue,isFF,phqid,pstatus_p) = params
    '''if isFF:
        thisId="publisher_ff_"+str(phqid)
    else:'''
    
    thisId="publisher_"+str(phqid)


    #mq_client_ph=mqb.init_client(clientName)
    bigdoc=[]
    nowTS=time.time()
    nowTS_2=time.time()
    last_proctime = 0
    msgCnt=0
    msgCntHold=0
    time.sleep(0.01)
    lastPush=0
    isPushing=0
    lastPushSize=0
    pushTS=0
    while True:		
        time.sleep(0.001)
        tid=0
        elapsedTS=time.time()-nowTS    
        elapsedTS_2=time.time()-nowTS_2	
        if elapsedTS>5:
            #print(elapsedTS)
            msgPersec=(msgCnt-msgCntHold)/elapsedTS
            msgCntHold = msgCnt
            nowTS=time.time()
            d_msg={
                            
                "msgCnt":msgCnt,
                "msgPersec":round(msgPersec,2),
                "queue size":thisQue.qsize(),				
                "process id":phqid,				
                "last proctime":round(last_proctime),
                "bigdoc pending":len(bigdoc),	
                "lastPush ago":round(lastPush),
                "push in": round(elapsedTS_2),
                "lastPushSize":lastPushSize,
                "isPushing":isPushing,
                "now":time.time(),
                "pushTS":pushTS
            }
            pstatus_p[thisId]=d_msg
            #print(d_msg)
            '''try:
                mq_client_ph.publish('plannatech_status',json.dumps(d_msg))
            except Exception as ex_mq:
                print(ex_mq)
                '''

        #time.sleep(0.001)
        emptyCount = 0
        doCatchup = False

        if(thisQue.qsize()>30):
            # When the queue spikes, batch more aggressively to catch up.
            doCatchup = True
            while not thisQue.empty() and emptyCount<100:
                time.sleep(0.0001)
                emptyCount += 1
                try:
                    qitem=thisQue.get()
                    msgCnt+=1
                    for item in qitem:
                        bigdoc.append(item)
                except:                
                    pass
        else:
            if not thisQue.empty():                                
                try:
                    qitem=thisQue.get()
                    msgCnt+=1
                    for item in qitem:
                        bigdoc.append(item)
                except:                
                    pass

        #else:
        #    #print(thisQue.qsize())
        #    pass

        '''print("len",len(bigdoc),(len(bigdoc)>200))
        print("elapsed",elapsedTS,(elapsedTS>30))
        print
        print("wtf",(len(bigdoc)>200 or elapsedTS_2>30))'''
        if(elapsedTS_2 > 30 and len(bigdoc)==0):
            elapsedTS_2=0
            nowTS_2=time.time()
        if (len(bigdoc)>100 or (elapsedTS_2>15 and len(bigdoc)>0)) or doCatchup:
            lastPush=elapsedTS_2
            nowTS_2=time.time()
            bigdoc_copy=bigdoc
            lastPushSize=len(bigdoc_copy)            
            #print(len(bigdoc_copy))
            pushTS=time.time()
            isPushing=1
            #d_msg["pushStart"]=
            d_msg["bigdoc pending"]=len(bigdoc)
            d_msg["isPushing"]=isPushing
            d_msg["pushTS"]=pushTS
            pstatus_p[thisId]=d_msg
            '''if(thisQue.qsize()>50):
                print("Catchup ")
                p3=mp.Process(target=publishData, args=(bigdoc_copy,False,0,isFF,True))
                p3.start()
            else:
                publishData(bigdoc_copy,False,0,isFF,False)			'''
            if doCatchup:
                p3=mp.Process(target=publishData, args=(bigdoc_copy,False,0,isFF,True))
                p3.start()
            else:             
                publishData(bigdoc_copy,False,0,isFF,False)
            isPushing=0
            d_msg["isPushing"]=isPushing			
            pstatus_p[thisId]=d_msg
            bigdoc=[]

            last_proctime=time.time()-pushTS

            #print("bigdocQ",thisQue.qsize(),"bigdoc",len(bigdoc),"bigdoc_copy",len(bigdoc_copy))
        #print("bigdocQ",bigdocQ.qsize(),"bigdoc",len(bigdoc))




def processScores(j,db_p,curs_p,cursd_p):
    #print(j)
    if 'Body' in j:
        body = j["Body"]
        if 'Events' in body:
            base = body["Events"]
        else:
            base = body
    else:
        print(Fore.RED,"invalid score",j)
        return None
    cnt = 0
    longest = 0
    maxrsultslen=0
    maxscorelen=0
    for e in base:
        cnt += 1
        thisLen = len(json.dumps(e))
        if thisLen > longest:
            longest = thisLen

        if cnt > 5:
            #continue
            pass
        #print(Fore.LIGHTBLACK_EX,"-------------------")
        #print(e)
        provider_id = e["FixtureId"]
        try:
            if 'Livescore' in e:
                if e["Livescore"] is None:
                    print("Livescore payload missing")
                    print(e)
                    return
                ls = e["Livescore"]
                s = ls["Scoreboard"]    
                status=""
                if 'Status' in s:
                    status = s["Status"]
                currentPeriod = s["CurrentPeriod"]
                ls_time = s["Time"]
                if 'Results' in s:
                
                    results = s["Results"]
                    thisLen = len(results)
                    #if thisLen > maxrsultslen:
                        #maxrsultslen = thisLen
                        #print("-------------------")
                        #print(json.dumps(results))
                    score_1 = -1
                    score_2 = -1
                    for r in results:
                        
                        if r["Position"] == "1":
                            score_1 = r["Value"];
                        if r["Position"] == "2":
                            score_2 = r["Value"];
                        scorelen = len(r["Value"])
                        if scorelen > maxscorelen:
                            maxscorelen = scorelen
                            #print(r["Value"])
                        #if not isinstance(r["Value"],int):                            
                        #    print("Not int",r["Value"],type(r["Value"]))                            
                    if len(results) > 2:                        
                        print(Fore.MAGENTA,"result len >2 ",results)
                    else:
                        pass
                        #insert_tuple=(provider_id,score_1,score_2,status,currentPeriod,'bla')
                    try:    
                        insert_tuple=(provider_id,score_1,score_2,status,currentPeriod,json.dumps(e))                        
                        insert_local_results(insert_tuple,curs_p,cursd_p,db_p,False)
                    except Exception as ex:
                        pass
                else:
                    print(Fore.RED,"No Results")    
            else:
                print(Fore.RED,"No livescore")
        except Exception as bla:
            #print(Fore.RED,e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
    #print("longest",longest,"maxrsultslen",maxrsultslen,"maxscorelen",maxscorelen,"processed",cnt)

def qreader(params):
    (thisQue,qid,pstatus_p,s_db,s_curs,s_cursd,msgTypeCount,oddTypeCount) = params
    thisId='reader_'+str(qid)
    msgCnt = 0
    last_proctime = 0 
    bigdoc = {}
    lastPush = 0
    isPushing = 0
    lastPushSize = 0
    pushTS = 0
    nowTS = time.time()
    nowTS_2 = time.time()
    msgCntHold = 0
    t_db = None
    cparams = app_config.mysql_connect_kwargs()

    '''try :
        t_db = mysql.connector.connect(**cparams)
        t_curs_p = t_db.cursor(prepared=True)
        t_curs_d = t_db.cursor(dictionary=True)
    except Exception as exdb:
        print(exdb)
    '''
    isConnected = False
    while True:
        
        try:
            
            time.sleep(0.001)
            if isinstance(t_db,mysql.connector.connection_cext.CMySQLConnection):
                if t_db.is_connected():
                    isConnected = True
                else:
                    print('DB connection null')
                    isConnected = False
            if not isConnected:
                retryCount = 0
                while not isConnected:
                    retryCount += 1
                    print(qid," retry loop ",retryCount)
                    time.sleep(1)
                    try:
                        t_db = mysql.connector.connect(**cparams)
                        t_curs_p = t_db.cursor(prepared=True)
                        t_curs_d = t_db.cursor(dictionary=True)
                        isConnected = True
                        print(Fore.GREEN)
                    except Exception as ex2:
                        print(ex2)
                else:
                    isConnected = True
            

            elapsedTS=time.time()-nowTS    
            elapsedTS_2=time.time()-nowTS_2	
            if elapsedTS>5:
                #print(elapsedTS)
                msgPersec=(msgCnt-msgCntHold)/elapsedTS
                msgCntHold = msgCnt
                nowTS=time.time()
                d_msg={
                                
                    "msgCnt":msgCnt,
                    "msgPersec":round(msgPersec,2),
                    "queue size":thisQue.qsize(),				
                    "process id":qid,				
                    "last proctime":round(last_proctime),
                    "bigdoc pending":len(bigdoc),	
                    "lastPush ago":round(lastPush),
                    "push in": round(elapsedTS_2),
                    "lastPushSize":lastPushSize,
                    "isPushing":isPushing,
                    "now":time.time(),
                    "pushTS":pushTS
                }
                pstatus_p[thisId]=d_msg
        

            if not thisQue.empty():
                tit=thisQue.get()
                msgCnt += 1
                try:
                    processStreamMessage(tit,t_db,t_curs_p,t_curs_d,msgTypeCount,oddTypeCount)
                    pass
                except Exception as ex1:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(exc_type, fname, exc_tb.tb_lineno)
                #print(Fore.BLUE,"\n----------------------\n",tit)
        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)




'''def insert_local_db(insert_tuple):
    # cur = con.cursor()
    try:
        #print(Fore.MAGENTA,insert_tuple)
        sql_ins = 
        insert ignore into ls_contest 
        (sport,league,home,away,run_date,provider_id,last_updated) 
        values(?,?,?,?,?,?,?) 
        on duplicate key update run_date=values(run_date), last_updated=values(last_updated);
        
        curs_prep.execute(sql_ins,insert_tuple)
        mydb.commit()
        
    except Exception as ex:
        print(Fore.RED,"insert error:",sql_ins, insert_tuple,ex)
    return None
'''

def insert_local_results(insert_tuple,curs,cursd,db,doReturn):
    # cur = con.cursor()
    try:
        #print(Fore.MAGENTA,insert_tuple)
        sql_ins = '''
        insert  into ls_status 
        (provider_id,score_1,score_2,status,currentperiod,json_data) 
        values(?,?,?,?,?,?) 
        on duplicate key update 
        score_1=values(score_1),
        score_2=values(score_2),
        status=values(status),
        currentperiod=values(currentperiod),
        json_data=values(json_data)
        ;
        '''
        curs.execute(sql_ins,insert_tuple)
        #print(insert_tuple[5])
        db.commit()
        #print("insert id",curs_prep.lastrowid)    
        if curs_prep.lastrowid == 0:
            pass
        else:
            return curs_prep.lastrowid
        if doReturn:
            contest = getContest(insert_tuple[5],cursd,db)
            #print(Fore.BLUE,"contest: insert_local_db",contest)
            return contest
    except Exception as ex:
        print(Fore.RED,"insert error:",sql_ins, insert_tuple,ex)
        return -1
    return None

def insert_extra_markets(extraMarkets,curs,cursd,db):
    #print(extraMarkets)
    
    try:
        #print(Fore.MAGENTA,insert_tuple)
        sql_ins = '''
        insert ignore into ls_other_bets

        (type,title,sbid,provider_id) 
        values(?,?,?,?) ;
        
        '''
        curs.executemany(sql_ins,extraMarkets)
        #print(insert_tuple[5])
        db.commit()
        #print("insert id",curs_prep.lastrowid)    
        '''if curs_prep.lastrowid == 0:
            pass
        else:
            return curs_prep.lastrowid
        if doReturn:
            contest = getContest(insert_tuple[5],cursd,db)
            #print(Fore.BLUE,"contest: insert_local_db",contest)
            return contest'''
    except Exception as ex:
        print(Fore.RED,"insert error:",sql_ins, extraMarkets,ex)
        return -1
    return None

def insert_local_outright_markets(insert_tuple,curs,cursd,db):
    try:
        sql_ins = '''
        INSERT IGNORE INTO ls_outright_markets
        (
        `provider_id`,
        `market_name`,
        `market_id`
        )
        VALUES
        (?,?,?);
        '''
        curs.execute(sql_ins,insert_tuple)
        #print(insert_tuple[5])
        db.commit()
        #print("insert id",curs_prep.lastrowid)    
        '''if curs_prep.lastrowid == 0:
            pass
        else:
            return curs_prep.lastrowid
        if doReturn:
            contest = getContest(insert_tuple[5],cursd,db)
            #print(Fore.BLUE,"contest: insert_local_db",contest)
            return contest'''
    except Exception as ex:
        print(Fore.RED,"insert error:",sql_ins, insert_tuple,ex)
        return -1
    return None
        



def insert_local_outright_bets(insert_tuple,curs,cursd,db):
    try:
        sql_ins = '''
        INSERT IGNORE INTO `linesdb`.`ls_outright_markets_contestants`
        (
        `provider_id`,
        `market_id`,
        `b_id`,
        `b_name`,
        `b_status`,
        `line`,
        `baseline`,
        `price`,
        `startprice`,
        `participant_id`,
        `last_update`,
        `sbid`)
        VALUES
        (?,?,?,?, ?,?,?,?, ?,?,?,?)
        on duplicate key update 
        b_status=values(b_status),
        line=values(line),
        baseline=values(baseline),
        price=values(price),
        startprice=values(startprice),
        last_update=values(last_update)        
        ;

        '''
        curs.executemany(sql_ins,insert_tuple)
        #print(insert_tuple[5])
        db.commit()
        #print("insert id",curs_prep.lastrowid)    
        '''if curs_prep.lastrowid == 0:
            pass
        else:
            return curs_prep.lastrowid
        if doReturn:
            contest = getContest(insert_tuple[5],cursd,db)
            #print(Fore.BLUE,"contest: insert_local_db",contest)
            return contest'''
    except Exception as ex:
        print(Fore.RED,"insert error:",sql_ins, insert_tuple,ex)
        return -1
    return None
        

def insert_local_outright(insert_tuple,curs,cursd,db,doReturn):
    # cur = con.cursor()
    try:
        #print(Fore.MAGENTA,insert_tuple)
        sql_ins = '''
        insert ignore into ls_outright
        (ls_id, ls_name, ls_type, comp_id, comp_type, comp_name,sport,location,run_date,provider_id,last_updated,status) 
        values(?,?,?,?,?,?,?,?,?,?,?,?) 
        on duplicate key update run_date=values(run_date), last_updated=values(last_updated),status=values(status);
        '''
        curs.execute(sql_ins,insert_tuple)
        #print(insert_tuple[5])
        db.commit()
        #print("insert id",curs_prep.lastrowid)    
        '''if curs_prep.lastrowid == 0:
            pass
        else:
            return curs_prep.lastrowid
        if doReturn:
            contest = getContest(insert_tuple[5],cursd,db)
            #print(Fore.BLUE,"contest: insert_local_db",contest)
            return contest'''
    except Exception as ex:
        print(Fore.RED,"insert error:",sql_ins, insert_tuple,ex)
        return -1
    return None


def insert_local_db_2(insert_tuple,curs,cursd,db,doReturn):
    # cur = con.cursor()
    try:
        #print(Fore.MAGENTA,insert_tuple)
        sql_ins = '''
        insert ignore into ls_contest 
        (sport,league,home,away,run_date,provider_id,last_updated,status,sport_id,location_id,league_id) 
        values(?,?,?,?,?,?,?,?,?,?,?) 
        on duplicate key update 
        run_date=values(run_date), last_updated=values(last_updated),status=values(status)
        ,sport_id=values(sport_id), location_id=values(location_id), league_id=values(league_id)
        ;
        '''
        curs.execute(sql_ins,insert_tuple)
        #print(insert_tuple[5])
        db.commit()
        #print("insert id",curs_prep.lastrowid)    
        if curs_prep.lastrowid == 0:
            pass
        else:
            return curs_prep.lastrowid
        if doReturn:
            contest = getContest(insert_tuple[5],cursd,db)
            #print(Fore.BLUE,"contest: insert_local_db",contest)
            return contest
    except Exception as ex:
        print(Fore.RED,"insert error:",sql_ins, insert_tuple,ex)
        return -1
    return None
def process_league_chunk(i,s_curs,s_cursd,s_db):

    ls_id = i["Id"]
    ls_name = i["Name"]
    ls_type = i["Type"]
    #print(ls_id,ls_name)
    ## this thing here not in Fixtures        
    for comp in i["Competitions"]:
        comp_type = comp["Type"]
        comp_name = comp["Name"]
        comp_id = comp["Id"]
        #print("-------------------")
        #print()
        for e in comp["Events"]:
            
            #print("-------------------")
            #print(e["OutrightFixture"])
            ## instead of fixture !!!!
            of = e["OutrightLeague"]
            status = of["Status"]
            LastUpdate = re.sub("T"," ",of["LastUpdate"][:19])
            sportName = of["Sport"]["Name"]
            if sportName in ("Horse Racing","Greyhounds","Trotting"):
                #print("Horse/Grayhound/Trotting skip")
                continue
            locationName = of["Location"]["Name"]
            #run_date = re.sub("T"," ",of["StartDate"])
            run_date = "0000-00-00 00:00:00"
            provider_id = e["FixtureId"]
            
            

            insert_tuple=(
                ls_id,
                ls_name,
                ls_type,
                comp_id,
                comp_type,
                comp_name,
                sportName,
                locationName,                
                run_date,
                provider_id,
                LastUpdate,
                status
            )
            insert_local_outright(insert_tuple,s_curs,s_cursd,s_db,False)

def insert_outright_participants(participants,ls_id,provider_id,s_curs,s_cursd,s_db):
    sql = '''insert ignore into ls_outrights_participants 
        (participant_id, name, position, isactive, ls_id, provider_id)
        values (?,?,?,?,?,?) 
        on duplicate key update
        name=values(name), position=values(position), isactive=values(isactive);
        '''
        
    pcount = 0
    tuple_many = []
    for p in participants:
        participant_id = p["Id"]
        name = p["Name"]
        position = p["Position"]
        isactive = p["IsActive"]
        insert_tuple = (participant_id,name,position,isactive,ls_id,provider_id,)
        tuple_many.append(insert_tuple)
        pcount += 1
    
    s_curs.executemany(sql,tuple_many)
    s_db.commit()


def process_fixture_chunk(i,s_curs,s_cursd,s_db):
    ls_id = i["Id"]
    ls_name = i["Name"]
    ls_type = i["Type"]
    for e in i["Events"]:        
        #print("-------------------")
        #print(e["OutrightFixture"])
        of = e["OutrightFixture"]
        LastUpdate = re.sub("T"," ",of["LastUpdate"][:19])
        sportName = of["Sport"]["Name"]
        status = of["Status"]
        if sportName in ("Horse Racing","Greyhounds","Trotting"):
            #print("Horse/Grayhound/Trotting skip")
            continue
        locationName = of["Location"]["Name"]
        run_date = re.sub("T"," ",of["StartDate"])
        provider_id = e["FixtureId"]
        
        comp_type = 0
        comp_name = ""
        comp_id = ""

        insert_tuple=(
            ls_id,
            ls_name,
            ls_type,
            comp_id,
            comp_type,
            comp_name,
            sportName,
            locationName,                
            run_date,
            provider_id,
            LastUpdate,
            status
        )
        #print(insert_tuple)
        insert_local_outright(insert_tuple,s_curs,s_cursd,s_db,False)
        try:
            participants = of["Participants"]
            insert_outright_participants(participants,ls_id,provider_id,s_curs,s_cursd,s_db)

        except Exception as ex:
            print("participant insert failed",ex)


def process_outright_fixture(j,s_curs,s_cursd,s_db):
    try:
        #rint(j)
        if j["Header"]["Type"] == 38:
            # League snapshots can arrive either directly in Body or wrapped in Competition.
            base = j["Body"]            
            if "Competition" in j["Body"]:
                base = j["Body"]["Competition"]
            
            if isinstance(base,list):
                for i in base:
                    process_league_chunk(i,s_curs,s_cursd,s_db)
            else:
                process_league_chunk(base,s_curs,s_cursd,s_db)

                
        elif j["Header"]["Type"] == 37:
            # Fixture snapshots use the same wrapper pattern but include participants and dates.
            base = j["Body"]
            if "Competition" in j["Body"]:
                base = j["Body"]["Competition"]
            
            if isinstance(base,list):
                for i in base:
                    process_fixture_chunk(i,s_curs,s_cursd,s_db)
            else:
                process_fixture_chunk(base,s_curs,s_cursd,s_db)
            
        else:     
            print(j["Header"])
    except Exception as ex:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(j)




def getContent(url,fileName):
    #print(url,headers)
    file_age=-1	
    fileName = re.sub(" ","_",fileName)
    r = requests.get(url, headers=headers)#,data=json.dumps(params))
    
    #print(r.headers)
    #print("-----------------")
    fc = r.content
    #print(fc)
    j = json.loads(fc)
    #print(j["Body"])
    f=open("data/"+fileName+".json","wb")
    f.write(fc)
    f.close()
    r.close()
    return fc

def getContentCached(url,fileName,maxAge):
    #print(url,headers)
    file_age=-1	
    fileName = re.sub(" ","_",fileName)
    fileName = "data/"+fileName+".json"
    try:        
        x=os.stat(fileName)
        file_age=(time.time()-x.st_mtime) 
        #print("The age of the given file",fileName," is: ",file_age)
    except:
        #print(Fore.RED,"no ",fileName,"found!!!")
        pass
    if file_age>(maxAge) or file_age==-1:
        r = requests.get(url, headers=headers)#,data=json.dumps(params))
        fc = r.content
        f=open(fileName,"wb")
        f.write(fc)
        f.close()
        r.close()
        return fc
    else:
        #print(Fore.GREEN,"File found, using cache")
        f=open(fileName)
        fc=f.read()
        f.close()
        return fc



def controlPackage(op):
    ret = getContent(url+op+urlParams,op)

def getContentTest():
    
    c = getContentCached(url+'GetSports'+urlParams,'Sports',60)
    print(len(json.loads(c)))

def getOutrightLeagues(TS):
    ret = getContentCached(url+'GetOutrightLeagues'+urlParams,'OutrightLeagues',0)
    return ret
def getOutrightFixtures(TS):
    ret = getContentCached(url+'GetOutrightFixtures'+urlParams,'OutrightFixtures',0)
    return ret


def getEvents(Id):
    ret = getContent(url+'GetEvents'+urlParams+'&sports='+str(Id),'_GetEvents_'+str(Id))
    return ret



def getFixtures(Id):
    #print("Fixture ID:",Id)
    ret = getContentCached(url+'GetFixtures'+urlParams+'&sports='+str(Id),'_GetFixtures_'+str(Id),600)
    return ret

def getFixturesByTS(ts):
    thisURL= url+'GetFixtures'+urlParams+'&timestamp='+str(ts)
    ret = getContentCached(thisURL,'Fixtures_TS',0)
    return ret


def getFixturesById(idSTR):
    thisURL= url+'GetFixtures'+urlParams+'&fixtures='+idSTR
    ret = getContentCached(thisURL,'_Missing_Fixtures_'+str(idSTR),3600)
    return ret

def handleMissingSingle(provider_id,curs,cursd,db): 
    res = {}
    try:
        fixtures = getFixturesById(str(provider_id))
        res = insertFixtures(fixtures,curs,cursd,db,True,False)        
        return res        
    except Exception as ex:
        print(Fore.RED,"handleMissingSingle failed",ex)
        return res


def handleMissing(md,curs,cursd,db): 
    try:
        if len(md)==0 :
            return None    
        idSTR =""
        cnt = 0
        md2 = copy.deepcopy(md)
        for id in md2:
            cnt +=1
            if cnt>1:
                idSTR += ","
            idSTR += str(id)        
        md.clear()
        fixtures = getFixturesById(idSTR)
        insertFixtures(fixtures,curs_prep,curs_dict,mydb,False,False)
    except Exception as ex:
        print(Fore.RED,"handleMissing failed",ex)

def getTS(fileName):
    try:
        f=open(fileName)
        fc=f.read()
        f.close()
        j = json.loads(fc)
        ts = j["Header"]["ServerTimestamp"]
    except Exception as ex:
        return round(time.time())-3600
    return ts


def getFixtureLastTimeStamp():
    sports = json.loads(getSports())
    tsMin = 9999999999999
    for sport in sports["Body"]:
        try: 
            #print(sport)
            fileName = "data/_GetFixtures_"+str(sport["Id"])+".json"
            #print(fileName)
            ts = getTS(fileName)
            if ts < tsMin and ts > -1:
                #print(Fore.GREEN,"new tsmin:",ts,tsMin,(ts-tsMin))
                tsMin = ts
            else:
                #print(Fore.RED,"no new",ts,tsMin,(ts-tsMin))
                pass
                
            #print("checking by TS file")            

        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
    try: 
        ts = getTS("data/Fixtures_TS.json")    
        if ts > tsMin:
            tsMin = ts
        
        print(Fore.CYAN,"Fixtures by ts ts",ts,tsMin,(ts-tsMin))
        now = round(time.time())
        print(Fore.GREEN,"tsMin is ",(now-tsMin),"s old")
    except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            
    return tsMin


def insertFixtures_stream(j,curs,cursd,db,doReturn,checkDupe):
    cnt = 0 
    ret = {}    
    #print(len(contestDict))
    updatedCnt = 0
    dupeCnt = 0
    insertedCnt = 0 
    sportName = ""
    for i in j["Body"]["Events"]:
        cnt += 1 
        #if cnt > 99999999:
        #    continue
        #print(Fore.BLUE,i)
        #if cnt > 10:
        #    continue

        try: 
            
            ft = i["Fixture"]
            LastUpdate = re.sub("T"," ",ft["LastUpdate"][:19])
            sport = ft["Sport"]
            status = ft["Status"]
            league = ft["League"]
            try:
                location = ft["Location"]
            except Exception as err1:
                pass
            sportId = -1 
            locationId = -1
            leagueId = -1
            try:
                sportId = sport["Id"]
            except Exception as err1:
                pass
            try:
                locationId = location["Id"]
            except Exception as err1:
                pass
            try: 
                leagueId = league["Id"]
            except Exception as err1:
                pass

            teams = {}
            
            for team in ft["Participants"]:
                #print("-------------------")
                #print(teams)
                tp=team["Position"]
                teams[tp]=team
            provider_id = i["FixtureId"]
            sportName = sport["Name"]
            run_date = re.sub("T"," ",ft["StartDate"])
            insert_tuple=(
                sportName,
                league["Name"],
                teams["1"]["Name"],
                teams["2"]["Name"],
                run_date,
                provider_id,
                LastUpdate,
                status,
                sportId,
                locationId,
                leagueId
            )
            run_insert = True
            if(checkDupe):

                if provider_id not in contestDict:                    
                    insertedCnt += 1
                    run_insert = True
                    
                    
                else:
                    contest = contestDict[provider_id]                    
                    c_run_date=contest["run_date"]                    
                    if run_date != c_run_date:
                        updatedCnt += 1
                        run_insert = True
                        '''print("-----------------------------")
                        print(Fore.LIGHTBLACK_EX,contest)
                        print(Fore.LIGHTGREEN_EX,insert_tuple)
                        print(type(c_run_date),c_run_date)
                        print("-----------------------------")'''
                    else:
                        dupeCnt += 1
                        run_insert = False

                    
            


            #print(Fore.YELLOW,insert_tuple)
            if doReturn:
                contest = insert_local_db_2(insert_tuple,curs,cursd,db,doReturn)
                #print(Fore.YELLOW,"Contest in insertFixtures()",contest)
                ret[provider_id] = contest
            else:
                if run_insert:
                    insert_local_db_2(insert_tuple,curs,cursd,db,doReturn)
                else:
                    #print("skip")
                    pass
        except Exception as ex:                        
            print(ex,j)
            #print(Fore.RED,insert_tuple)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            #print(teams)
            #print(ft)
            

    #print(Fore.BLUE, sportName,"processed:",cnt,"inserted",insertedCnt,"items, dupes: ",dupeCnt," items","updated:",updatedCnt )
    return ret



def insertFixtures(fixtures,curs,cursd,db,doReturn,checkDupe):
    #f = open('GetFixtures.json')
    #print(type(fixtures))
    if isinstance(fixtures,bytes):
        j = json.loads(fixtures)
    else:        
        #print(type(fixtures))
        j = fixtures        
    #print(json.dumps(j,indent=5))
    cnt = 0 
    ret = {}    
    #print(len(contestDict))
    updatedCnt = 0
    dupeCnt = 0
    insertedCnt = 0 
    sportName = ""
    #print(len(j["Body"]))
    for i in j["Body"]:
        cnt += 1 
        #if cnt > 99999999:
        #    continue
        #print(Fore.BLUE,i)
        #if cnt > 10:
        #    continue

        try: 
            
            ft = i["Fixture"]
            LastUpdate = re.sub("T"," ",ft["LastUpdate"][:19])
            sport = ft["Sport"]
            league = ft["League"]
            try:
                location = ft["Location"]
            except Exception as err1:
                pass
            
            teams = {}
            
            for team in ft["Participants"]:
                #print("-------------------")
                #print(teams)
                tp=team["Position"]
                teams[tp]=team
            provider_id = i["FixtureId"]
            sportName = sport["Name"]
            
            sportId = -1 
            locationId = -1
            leagueId = -1
            try:
                sportId = sport["Id"]
            except Exception as err1:
                pass
            try:
                locationId = location["Id"]
            except Exception as err1:
                pass
            try: 
                leagueId = league["Id"]
            except Exception as err1:
                pass
            

            status = ft["Status"]
            run_date = re.sub("T"," ",ft["StartDate"])
            insert_tuple=(
                sportName,
                league["Name"],
                teams["1"]["Name"],
                teams["2"]["Name"],
                run_date,
                provider_id,
                LastUpdate,
                status,
                sportId,
                locationId,
                leagueId
            )
            run_insert = True
            if(checkDupe):

                if provider_id not in contestDict:                    
                    insertedCnt += 1
                    run_insert = True
                    
                    
                else:
                    contest = contestDict[provider_id]                    
                    c_run_date = contest["run_date"]
                    c_status = contest["status"]
                    if run_date != c_run_date or c_status != status:
                        updatedCnt += 1
                        run_insert = True
                        if c_status != status:
                            print("---------STATUS CHANGED--------------------")
                            print(Fore.LIGHTBLACK_EX,contest)
                            #print(Fore.LIGHTGREEN_EX,insert_tuple)
                            #print(type(c_run_date),c_run_date)
                            print("-----------------------------")
                    else:
                        dupeCnt += 1
                        run_insert = False
            else:
                insertedCnt += 1
                    
            


            #print(Fore.YELLOW,insert_tuple)
            if doReturn:
                contest = insert_local_db_2(insert_tuple,curs,cursd,db,doReturn)
                print(Fore.YELLOW,"Contest in insertFixtures()",contest)
                ret[provider_id] = contest
            else:
                if run_insert:
                    insert_local_db_2(insert_tuple,curs,cursd,db,doReturn)
                else:
                    #print("skip")
                    pass
        except Exception as ex:                        
            print(ex,j)
            #print(Fore.RED,insert_tuple)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            #print(teams)
            #print(ft)
            

    print(Fore.BLUE, sportName,"processed:",cnt,"inserted",insertedCnt,"items, dupes: ",dupeCnt," items","updated:",updatedCnt )
    return ret

#get_schedule()



def insert_local_db_bo_contestant(insert_tuple,curs,db):
    # cur = con.cursor()
    try:
        #print(Fore.MAGENTA,insert_tuple)
        sql_ins = '''
        insert  into ls_bo_contestant
        ( provider_id,market_id,market_name,sb_id,sb_name,bets) 
        values(?,?,?,?,?,?) 
        on duplicate key update bets=values(bets);
        '''
        curs.execute(sql_ins,insert_tuple)

        db.commit()
        ret = curs.lastrowid
        #if ret == 0:
        #    print("already there")
            
    except Exception as ex:
        #print(Fore.RED,"insert error:",sql_ins, insert_tuple,ex)
        #print(Fore.MAGENTA,"insert error:",ex)
        return -1
    return ret

def getSports():
    ret = getContentCached(url+'GetSports'+urlParams,'Sports',3600)
    return ret

def getBooks():
    ret = getContent(url+'GetBookmakers'+urlParams,'_Books')
    return ret

def getLocations():
    ret = getContent(url+'GetLocations'+urlParams,'_Locations')
    return ret

def getLeagues():
    ret = getContent(url+'GetLeagues'+urlParams,'_Leagues')
    return ret



'''def get_fixture_markets():
    f = open('GetFixtureMarkets_basketball.json')
    f = open('GetFixtureMarkets_tennis.json')
    f = open('GetFixtureMarkets_american_football.json')
    j = json.loads(f.read())
    #print(json.dumps(j,indent=5))
    cnt = 0 

    for i in j["Body"]:
        
        cnt += 1 
        
        #print(Fore.BLUE,i)
        #print(Fore.YELLOW,"------------------------------------------------")
        #( provider_id,market_id,market_name,sb_id,sb_name,bets) 
        provider_id = i["FixtureId"]
        for m in i["Markets"]:
            print(Fore.MAGENTA,m)
            market_id = m["Id"]
            market_name = m["Name"]
            for p in m["Providers"]:
            
                sb_id = p["Id"]
                sb_name = p["Name"]
                bets = '[]'
                if "Bets" in p:
                    bets = p["Bets"]
                insert_tuple=( provider_id,market_id,market_name,sb_id,sb_name,json.dumps(bets)) 
                #print(Fore.CYAN,"------------------------------------------------")
                #print(insert_tuple) 
                try:
                    res = insert_local_db_bo_contestant(insert_tuple)
                    if res == 1:
                        print("Duplicate contestant row detected")
                        print("provider_id=", insert_tuple[0])
                except Exception as ex:
                    print(insert_tuple) 
                    print(ex)

                
            print(Fore.WHITE,"------------------------------------------------")
'''

def getOutRightsStored(maxOrIDparam):
    sql = ''' select * from ls_outright where 1 
     and id>%s; '''
    params = (maxOrIDparam,)
    curs_dict.execute(sql, params)    
    res2 = curs_dict.fetchall()
    mydb.commit()
    return res2


def getContests(maxIDparam):
    sql = '''select id,sport,league,home,away,cast(run_date as char) run_date,provider_id, cast(last_updated as char) as last_updated 
    ,status
    from ls_contest where 1 
    and run_date>date_sub(now(), interval 2 day) and id>%s ; '''
    params = (maxIDparam,)
    curs_dict.execute(sql, params)    
    res2 = curs_dict.fetchall()
    mydb.commit()
    return res2

def getContests_DDS():
    sql = "select * from ls_contest where 1 limit 10; "
    params = ()
    curs_dict.execute(sql, params)
    
    res2 = curs_dict.fetchall()
    mydb.commit()
    return res2


def getContest(provider_id,cursd,db):
    #curs_dict
    sql = "select id,sport,league,home,away,cast(run_date as char) run_date,provider_id, cast(last_updated as char) as last_updated from ls_contest where 1 and provider_id=%s; "

    '''sql = "select id,sport,league,home,away,run_date,provider_id,date_format(last_updated,'%%Y-%%m-%%d %%H:%%i:%%s')as last_updated from ls_contest where 1 and provider_id=%s; ";'''
    params = (provider_id,)
    cursd.execute(sql, params)
    #print("bla")
    res2 = cursd.fetchall()
    #print(sql,res2)
    if len(res2)>0:
        return res2[0]
    return None
    #return res2[0]
    #maxTS=int(res2[0]["maxTS"])

def publishData(data,test,sbid,isFF,debug):
    try:
        #print(test,sbid,isFF)                
        #return None
        #print(data)
        #print(len(sys.argv))
        if len(sys.argv)>2:
            if(sys.argv[2] =='nopub'):
                time.sleep(1);
                return None
        pURL = app_config.publish_url(isFF)
        if(test):
            pURL+="&test=1"
        pURL+="&sb="+str(sbid)
        if debug:
            print(pURL)
            #print(json.dumps(data,indent=2))
            #mq_client.publish('lsports/pubdata',json.dumps(data,indent=2))

        #print(json.dumps(data))
        #print(json.dumps(data,indent=5))
        #requests.post(pURL, data=json.dumps(data))
        r = requests.post(pURL, data=json.dumps(data))
        #print("Process time:  : " + str(time.time() - start))
        r.close()
        #print(r.content)
        #time.sleep(5)
        #mq_client.publish('lsports/pubtime',str(time.time() - start))
        #print("Close time:  : " + str(time.time() - start))
        #print("Status: "+str(r.status_code))
        #time.sleep(10)
    except Exception as e2:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print('error PHP data push')
    return None

def insertMarket(provider_id,markets,curs_prep,curs_dict,mydb):
    stats = {}
    countMarkets = 0
    cnt = 0
    contest = contestDict[provider_id]
    #print("---------------------------")
    #print(Fore.WHITE,markets)
    #print("---------------------------")
    bigdoc = []
    insc = {}
    insc["home"] = contest["home"]
    insc["away"] = contest["away"]
    insc["name"] = contest["home"] +' vs '+ contest["away"]
    insc["sb_contest_id"] = provider_id
    insc["is_consensus_source"] = provider_id
    insc["groupname"] = contest["league"]
    insc["sport"] = contest["sport"]
    insc["start"] = contest["run_date"]
    insc["ls_status"] = contest["status"]
    insc["live"] = 0
    insc["bo"] = []

    #print(Fore.GREEN,insc)
    try:
        for m in markets:
            countMarkets += 1
            stats["countMarkets"] = countMarkets
            market_id = m["Id"]
            market_name = m["Name"]

            for p in m["Providers"]:
                ins = copy.deepcopy(insc)
                sb_id = p["Id"]                
                #print(Fore.BLUE,sb_id,Fore.YELLOW,market_id)
                ins["sbid"] = sb_id+1000
                #print("---------------------------------")
                #print("NEW PROVIDER", sb_id,ins)
                #print("---------------------------------")
                #if not 1071 == sb_id:
                #    continue
                cnt += 1 
                sb_name = p["Name"]
                #bets = 
                #bo = []
                #ins["bo"] = []
                if "Bets" in p:
                    bets = p["Bets"]
                    #print("calling insertBetOffers(",market_id,sb_id,provider_id)
                    ins["bo"]=insertBetOffers(bets,market_id,sb_id,provider_id)
                    #print(Fore.GREEN,json.dumps(ins["bo"],indent=5))
                #print(Fore.LIGHTCYAN_EX,json.dumps(ins,indent=2))   
                bigdoc.append(ins)
                if sb_id == 1:
                        for i in range(1,6):
                            ins_dup = copy.deepcopy(ins)
                            ins_dup["sbid"]=3000+i #mybet
                            #print("---------------------")
                            #print(ins_dup)
                            bigdoc.append(ins_dup)
                pushindex = findSmallestQueue(q)
                q[pushindex].put_nowait(bigdoc)


        #publishData(bigdoc,False,0,False,True)

    except Exception as ex:
        #print(Fore.RED,"Exception",bookName,bookID,ex)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)


def insertOutrightMarkets(j,s_curs,s_cursd,s_db,verbose):
    
    print("BODYTYPE:",type(j["Body"]))
    if isinstance(j["Body"],list):
        base = j["Body"]
    
    elif isinstance(j["Body"],dict):
        base = j["Body"]["Events"]
    else:
        print(Fore.RED,"SOMETHIN IS XXXXXX UP")

    try:    
        for i in base:           
            provider_id = i["FixtureId"]
            if provider_id in contestDict:
                pass
                #print(Fore.GREEN, "FOUND ",provider_id)
                #contest = contestDict[provider_id]
                #print(contest)
            for m in i["Markets"]:
                #countMarkets += 1
                #stats["countMarkets"] = countMarkets
                
                market_id = m["Id"]
                market_name = m["Name"]
                insert_tuple=(provider_id,market_name,market_id)
                insert_local_outright_markets(insert_tuple,s_curs,s_cursd,s_db)
                #print("MARKET",m)
                #print(insert_tuple)
                bookCount = 0
                betCount = 0
                for p in m["Providers"]:
                    #print(p["Name"])
                    '''if "LastUpdate" in p:
                        LastUpdate = p["LastUpdate"]
                        date_time_obj = datetime.strptime(LastUpdate[0:19], '%Y-%m-%dT%H:%M:%S')
                        provider_last_update_ts=date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
                        #print(market_last_ts)'''
                    bookCount += 1
                    if "Bets" in p:
                        betCount +=1
                        insert_bet_tuple_list=[]                    
                        for b in p["Bets"]:
                            b_id = b["Id"]
                            b_name = b["Name"]
                            
                            line = ""
                            if "Line" in b:
                                line = b["Line"]
                            
                            baseline = ""
                            if "BaseLine" in b:
                                baseline = b["BaseLine"]

                            '''settlement = 0
                            if "Settlement" in b:
                                settlement = b["Settlement"]
                            
                            position = 0
                            if "Position" in b:
                                position = b["Position"]
                            '''

                            b_status = b["Status"]
                            
                            participant_id = 0
                            if "ParticipantId" in b:
                                participant_id = b["ParticipantId"]
                            startprice = b["StartPrice"]
                            price = b["Price"]

                            b_LastUpdate = b["LastUpdate"]
                            date_time_obj = datetime.strptime(b_LastUpdate[0:19], '%Y-%m-%dT%H:%M:%S')
                            last_update=date_time_obj.strftime("%Y-%m-%d %H:%M:%S")

                            sbid = p["Id"]+1000                 
                            insert_bet_tuple_list.append((provider_id,market_id, b_id,b_name, b_status, line,baseline,price,startprice,participant_id,last_update,sbid))
                        insert_local_outright_bets(insert_bet_tuple_list,s_curs,s_cursd,s_db)
                        print("provider_id",provider_id,"sbid",sbid,"insert_bet_tuple_list:",len(insert_bet_tuple_list))
                        #print(Fore.LIGHTGREEN_EX,p["Bets"])
                        pass
                    else:
                        #print(Fore.RED,"No bets")
                        #print(p)
                        pass

                    #for b in p["Bets"]:
                    #    print(b["Name"],b["Price"])
                if verbose:
                    print("book:",bookCount,"bet:",betCount,"provider_id",provider_id,"sbid",sbid)
    except Exception as ex:
        #print(Fore.RED,"Exception",bookName,bookID,ex)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)


def connect_amq_local(clientName):
    print("reconnect ... ",clientName)
    try:
        c = pika.BlockingConnection(app_config.local_rabbitmq_params(clientName, heartbeat=5))
        return c
    except Exception as ex:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        
        


def insertMarkets(sports,bookID,bookName,curs,cursd,db,successDict,missingDict,giveupDict,stats,contestDictLocal,TS):
    
    def processj():
        c = None
        try:
            c=connect_amq_local("markets_"+str(bookID))
            time.sleep(1)
            ch = c.channel()
        except Exception as ex:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(Fore.RED,exc_type, fname, exc_tb.tb_lineno)
        cnt = 0 
        countTotal = 0
        countTotalMarkets = 0
        countMarkets = 0
        countTotalMatches = 1
        bigdoc = []

        for i in j["Body"]:   
            countTotalMatches += 1
            for m in i["Markets"]:
                countTotalMarkets += 1 
        
        stats["countTotalMarkets"] = countTotalMarkets
        stats["countTotalMatches"] = countTotalMatches
        stats["time"] = time.time()
        stats["missingDict_len"]=len(missingDict)
        stats["giveupDict_len"]=len(giveupDict)
        stats["bigdoc_len"] = len(bigdoc)
        stats["cached_error"] = 0
        stats["cached_hit"] = 0
        #print("Total:",countTotalMarkets)
        #return None
        countMatches = 0
        
        for i in j["Body"]:                
            countMatches += 1

            stats["bigdoc_len"] = len(bigdoc)
            stats["countMatches"] = countMatches   
            
            #cnt += 1 
            #print(Fore.YELLOW,"cnt:",cnt)
            
            cntm = 0 
            '''for m in i["Markets"]:
                cntm += 1 
                print(Fore.BLUE,"cntm:",cntm)
            '''
            #if cnt > 99999999999:
            #    continue
            #print(Fore.BLUE,i)
            #print(Fore.YELLOW,"------------------------------------------------")
            #( provider_id,market_id,market_name,sb_id,sb_name,bets) 
            provider_id = i["FixtureId"]
            
            if provider_id in missingDict:                    
                #print("Saved one")
                stats["cached_error"] += 1
                continue
            if provider_id in contestDictLocal:
                stats["cached_hit"] += 1 
                contest = contestDictLocal[provider_id]
            else:
                contest = getContest(provider_id,cursd,db)

            if contest is None:
                
                #contestNew = handleMissingSingle(provider_id,curs,cursd,db)
                contestNew = []
                if len(contestNew) == 0:
                    giveupDict[provider_id] = provider_id
                    missingDict[provider_id]=provider_id
                    print(Fore.RED,"Given up on ",provider_id)
                    continue
                else:
                    if provider_id not in contestNew:
                        giveupDict[provider_id] = provider_id
                        missingDict[provider_id]=provider_id
                        print(Fore.RED,"Given up on ",provider_id)
                        continue
                    else: 
                        contest = contestNew[provider_id]
                        successDict[provider_id]=provider_id

            if contest is None:                                        
                if provider_id not in missingDict:
                    giveupDict[provider_id]=provider_id
                    missingDict[provider_id]=provider_id
                    print(Fore.RED,"Given up on ",provider_id)
                    continue
                
            else:
                if provider_id not in successDict:                        
                    #successList.append(provider_id)
                    successDict[provider_id]=provider_id
                if provider_id not in contestDictLocal:
                    contestDictLocal[provider_id] = contest
                    print("Adding contest to local db",provider_id,contest)
                
                #print(Fore.GREEN,contest)

            '''{"id": 154169, "sport": "Tennis", "league": "Masters Women", "home": "Anastasiya Feklistova", "away": "V. Lenskaya-Bogomolova", "run_date": "2022-09-06 08:00:00", "provider_id": 9190746, "last_updated": "2022-09-06 10:04:56"}'''
            insc = {}
            insc["home"] = contest["home"]
            insc["away"] = contest["away"]
            insc["name"] = contest["home"] +' vs '+ contest["away"]
            insc["sb_contest_id"] = provider_id
            insc["is_consensus_source"] = provider_id
            insc["ls_status"] = contest["status"]
            insc["groupname"] = contest["league"]
            insc["sport"] = contest["sport"]
            insc["start"] = contest["run_date"]
            insc["live"] = 0
            insc["bo"] = []
            #cntm = 0 
            for m in i["Markets"]:
                countMarkets += 1
                stats["countMarkets"] = countMarkets
                #if countMarkets%100 == 0:
                #    print("Markets ::: ",contest["sport"],countMarkets,"of",countTotalMarkets)
                
                '''item["type"]=g["Description"]
                #id=str(idGame)+"_"+item["type"]
                id=hashlib.md5((g["HomeTeam"]+g["VisitorTeam"]+item["type"]).encode('utf-8')).hexdigest()
                id=str(g["IdGame"])+"_prop"
                item["id"]=id
                item["oc"]=[]'''

                #cnt += 1 
                #print(Fore.BLUE,"cntm:",cnt)
                #continue
                #print(Fore.MAGENTA,m)
                market_id = m["Id"]
                market_name = m["Name"]
                #print(Fore.BLUE,insc["name"],contest["sport"], market_id,market_name)
                #print("---------------------------------")
                
                for p in m["Providers"]:
                    
                    
                    ins = copy.deepcopy(insc)
                    sb_id = p["Id"]                
                    #print(Fore.BLUE,sb_id,Fore.YELLOW,market_id)
                    ins["sbid"] = sb_id+1000
                    #print("---------------------------------")
                    #print("NEW PROVIDER", sb_id,ins)
                    #print("---------------------------------")
                    #if not 1071 == sb_id:
                    #    continue
                    cnt += 1 
                    sb_name = p["Name"]
                    #bets = 
                    #bo = []
                    #ins["bo"] = []
                    if "Bets" in p:
                        bets = p["Bets"]
                        #print("calling insertBetOffers(",market_id,sb_id,provider_id)
                        ins["bo"]=insertBetOffers(bets,market_id,sb_id,provider_id)
                        #print(Fore.GREEN,json.dumps(ins["bo"],indent=5))
                    #print(ins)
                    bigdoc.append(ins)
                    if sb_id == 1:
                        for i in range(1,6):
                            ins_dup = copy.deepcopy(ins)
                            ins_dup["sbid"]=3000+i #mybet
                            #print("---------------------")
                            #print(ins_dup)
                            bigdoc.append(ins_dup)
                        

                    if len(bigdoc)%50==0 and len(bigdoc)>0:    
                        while not ch.connection.is_open:
                            try:
                                print("reconnecting",str(bookID))
                                c=connect_amq_local(str(bookID))
                                ch = c.channel()
                                time.sleep(1)
                            except Exception as exrecon:
                                exc_type, exc_obj, exc_tb = sys.exc_info()
                                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                print(exc_type, fname, exc_tb.tb_lineno)
                            #print(type(qitem))
                        ch.basic_publish('',
                            'lines_db_contest',
                            json.dumps(bigdoc),
                            pika.BasicProperties(content_type='text/plain',
                            delivery_mode=1)
                        )        
                        #print("bigdoc len:",len(bigdoc))
                        #print(Fore.WHITE,"------------------------------------------------")
                        #print(Fore.MAGENTA,json.dumps(bigdoc,indent=5))
                        #print(Fore.WHITE,"------------------------------------------------")
                        
                        ######publishData(bigdoc,False,0,False,False)
                        
                        #mq_client.publish('lsports/pubtime','x')
                        bigdoc = []                             
                    '''print(Fore.MAGENTA,json.dumps(bigdoc,indent=5))
                    print(Fore.WHITE,"------------------------------------------------")
                    bigdoc =   []'''
                    insert_tuple=( provider_id,market_id,market_name,sb_id,sb_name,json.dumps(bets)) 
                    #print(json.dumps(bets,indent=5))
                    #print(Fore.CYAN,"------------------------------------------------")
                    #print(insert_tuple) 
                    '''try:
                        res = 1
                        #res = insert_local_db_bo_contestant(insert_tuple,curs,db)
                        if res == -1:
                            #print(Fore.RED,"Insert error")
                            #print("provider_id=", provider_id)
                            missingList.append(provider_id)
                            print(Fore.RED,provider_id,sportName,len(missingList))
                        else:
                            #print(Fore.GREEN,res)
                            successList.append(provider_id)
                    except Exception as ex:
                        #print(insert_tuple) 
                        print(ex)
                        '''
                    #print(Fore.LIGHTBLACK_EX,json.dumps(ins))

                if len(bigdoc)%50==0 and len(bigdoc)>0:
                    stats["time"] = round(time.time())
                    stats["pushtime"] = round(time.time())
                    stats["pushing"] = 1                     
                    while not ch.connection.is_open:
                        try:
                            print("reconnecting",str(bookID))
                            c=connect_amq_local(str(bookID))
                            ch = c.channel()
                            time.sleep(1)
                        except Exception as exrecon:
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            print(exc_type, fname, exc_tb.tb_lineno)
                        #print(type(qitem))
                    ch.basic_publish('',
                        'lines_db_contest',
                        json.dumps(bigdoc),
                        pika.BasicProperties(content_type='text/plain',
                        delivery_mode=1)
                        )

                    #####publishData(bigdoc,False,0,False,False)


                    #a = executor.submit(publishData,(bigdoc,False,0,False))
                    stats["time"] = round(time.time())                        
                    stats["pushing"] = 0                        
                    bigdoc = []

            if len(bigdoc)%200==0 and len(bigdoc)>0:            
                stats["time"] = round(time.time())
                stats["pushtime"] = round(time.time())
                stats["pushing"] = 1
                while not ch.connection.is_open:
                    try:
                        print("reconnecting",str(bookID))
                        c=connect_amq_local(str(bookID))
                        ch = c.channel()
                        time.sleep(1)
                    except Exception as exrecon:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        print(exc_type, fname, exc_tb.tb_lineno)
                    #print(type(qitem))
                ch.basic_publish('',
                    'lines_db_contest',
                    json.dumps(bigdoc),
                    pika.BasicProperties(content_type='text/plain',
                    delivery_mode=1)
                    )                                                
                #####publishData(bigdoc,False,0,False,False)                        
                stats["time"] = time.time()                        
                stats["pushing"] = 0                        
                bigdoc = []
        stats["time"] = time.time()
        stats["pushtime"] = time.time()
        stats["pushing"] = 1   
        publishData(bigdoc,False,0,False,False)                        
        stats["time"] = time.time()                        
        stats["pushing"] = 0                        
        bigdoc = []                        
        ##movedfrom end
    if TS>0:
        
        print("TS mode selected",TS)
        stats["bookName"] = bookName
        stats["sport"] = "TS mode"
        j = {}
        try:
            m = getMarketsbyTS(tspecials,bookID,bookName,600)            
            j = json.loads(m)            
            time.sleep(1)
        except:
            print(Fore.LIGHTRED_EX,"load error parse","Timestamp",bookName)
            time.sleep(1)
            m = getMarketsbyTS(TS,bookID,bookName,0)
            j = json.loads(m)
            print(Fore.BLUE,"load OK","Timestamp",bookName,m)
            #continue
        try:
            if "Body" not in j:
                time.sleep(5)
                print(Fore.MAGENTA,"ERROR",j,"RETRYING")
                print(Fore.BLUE,"load OK","Timestamp",bookName)
                m = getMarketsbyTS(TS,bookID,bookName,0)
                j = json.loads(m)
                print(Fore.GREEN,"PARSE OK","Timestamp",bookName)

                print(" run processJ()")
                

        except:
            print(Fore.RED,"load error","Timestamp",bookName,m)
        
        processj()

    else:
        #print(sports)
        for sport in sports["Body"]:        
            #contestDictLocal[time.time()]=time.time()
            
            try:
                sid = sport["Id"]
                sportName = sport["Name"]
                stats["bookName"] = bookName
                if sportName != 'Football':
                    print(Fore.RED,sportName, "not soccer, bailing")
                    #continue
                else :
                    print(Fore.GREEN, sportName)

                stats["sport"] = sportName
                
                #stats["XXXXX"]=len(contestDictLocal)
                print(bookID,bookName,sid,sportName)
                time.sleep(5)
                j = {}
                try:
                    m = getMarkets(sport,bookID,bookName,600)
                    j = json.loads(m)   
                    #print(json.dumps(j,indent=2))         
                except:
                    print(Fore.LIGHTRED_EX,"load error parse",sportName,bookName)
                    time.sleep(1)
                    m = getMarkets(sport,bookID,bookName,0)
                    j = json.loads(m)
                    print(Fore.BLUE,"load OK",sportName,bookName,m)
                    #continue
                try:
                    if "Body" not in j:
                        time.sleep(1)
                        print(Fore.MAGENTA,"ERROR",j,"RETRYING")
                        print(Fore.BLUE,"load OK",sportName,bookName)
                        m = getMarkets(sport,bookID,bookName,0)
                        j = json.loads(m)
                        print(Fore.GREEN,"PARSE OK",sportName,bookName)

                        print(" run processJ()")
                        

                except:
                    print(Fore.RED,"load error",sportName,bookName,m)
                    continue


                #print(json.dumps(j,indent=5))            
                
                

                ## movedfrom 
                processj()
                
                ##movedfrom end
                                                
                
            except Exception as ex:
                print(Fore.RED,"Exception",bookName,bookID,ex)
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
            #while True:
        # end for sports
    #end of not ts but sport iterations    





def insertBetOffers(bets,market_id,sb_id,provider_id):
    bos = {}
    ''' 52 = 12
        2  = total
        3 = hc 
        1 = 1x2'''
    boTypes = {52:'1x2',2:'total',3:'spread',1:"1x2"}
    boType=boTypes[market_id]
    bo = []

    if market_id in(52,1):
        
        item = {}
        item["type"] = "1x2"
        item["id"] = str(provider_id)+"_"+str(market_id)+"_"+boType
        item["oc"] = []
        #item["sbid"] = sb_id
        for bet in bets:
            #print(bet)
            
            ocitem={}
            ocitem["id"]=bet["Id"]
            ocitem["name"]=bet["Name"]
            LastUpdate = bet["LastUpdate"];

            date_time_obj = datetime.strptime(LastUpdate[0:19], '%Y-%m-%dT%H:%M:%S')
            ocitem["lastTS"]=date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
            ocitem["lastTS_ts"]=round(time.mktime(date_time_obj.timetuple()))
            ocitem["odds"]=bet["Price"]
            ocitem["odds_spread"]=0
            item["oc"].append(ocitem)

        bo.append(item)
    else:
        #print("MARKET ID PART",market_id)
        #return bo
        for bet in bets:
            #print("------------------")
            #print(Fore.RED,bet)
            #print("------------------")
            
            modifier = ""
            if "BaseLine" in bet:
                #id=str(idGame)+"_"+item["type"]
                modifier="_"+re.sub('\(.*\)','',bet["BaseLine"]).strip()
                #print("modifier",modifier)

            id = "spread"+modifier 
            
            
            ocitem={}
            ocitem["id"]=bet["Id"]
            ocitem["name"]=bet["Name"]
            LastUpdate = bet["LastUpdate"];

            #print(LastUpdate)
            date_time_obj = datetime.strptime(LastUpdate[0:19], '%Y-%m-%dT%H:%M:%S')
            #print("Now:",LastUpdate,date_time_obj)
            ocitem["lastTS"]=date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
            ocitem["lastTS_ts"]=round(time.mktime(date_time_obj.timetuple()))
            #ocitem["lastTS_ts_check"]=date_time_obj.timetuple()
            
            #ocitem["lastTS_ts_date"]=datetime.utcfromtimestamp().strftime('%Y-%m-%d %H:%M:%S')
            #print("TYPE:",type(bet["Price"]))
            if not isinstance(bet["Price"], str):
                print(Fore.RED,"XXXXXXXXXXXXXXXX",bet["Price"])
            ocitem["odds"]=bet["Price"]
            ocitem["odds_spread"]=0
            try:
                ocitem["odds_spread"]=re.sub('\(.*\)','',bet["Line"]).strip()
            except:
                pass
            #item["oc"].append(ocitem)


            if id in bos:
                #print("found",bos[id])
                item=bos[id]
                
                item["oc"].append(ocitem)
                bos[id]=item
                #print(json.dumps(item,indent=5))
            else:
                item={}		
                item["type"]=boType
                #item["sbid"] = sb_id
                
                #print("NOT found",bos[id])
                item["id"]=str(provider_id)+"_"+str(market_id)+"_"+modifier+"_"+boType                  
                item["oc"]=[]
                
                item["oc"].append(ocitem)
                bos[id]=item
                
                #print("----------------------")
                #print(item)
            
        boIns=[]

        for id in bos:
            #print(Fore.LIGHTBLUE_EX,"------------------"+id+"------------------")
            #print(Fore.YELLOW,json.dumps(bos[id]))
            #print(Fore.LIGHTBLUE_EX,"------------------/"+id+"/------------------")
            bo.append(bos[id])
    
    #print(Fore.GREEN,"------------------bo: ",sb_id,market_id,"------------------")
    #print(bo)    
    #print(Fore.GREEN,"------------------/bo------------------")

    return bo



def getScores(TS):
    #print(TS)
    if TS > 0:
        ret = getContentCached(url+'GetScores'+urlParams+'&timestamp='+str(TS),'GetScores',0)
    else:
        ret = getContentCached(url+'GetScores'+urlParams,'GetScoresFull',3600)        
    return ret

def getMarketsByFixtures(fixtureListCommaStr,maxAge):
    #print('_GetFixtureMarkets_'+str(Id)+'.json')
    fileName = 'Markets_bla'
    print(fileName)
    
    bla = '160,299,274,639,300,2494,2534,2533,2495,463,1401,1429,1578,2709,2653,1559,2531,2699,2698,2697,2696,1755,2700,2701,2706,2708,2528,2522,2523,2524,2526,2527,310'

    ret = getContentCached(url+'GetFixtureMarkets'+urlParams+'&markets='+bla+'&Fixtures='+fixtureListCommaStr,fileName,maxAge)
    return ret

def getMarketsByFixture_singular(fixtureId,maxAge):
    #print('_GetFixtureMarkets_'+str(Id)+'.json')
    fileName = 'Markets_'+str(fixtureId)
    print(fileName)
    
    bla = '160,299,274,639,300,2494,2534,2533,2495,463,1401,1429,1578,2709,2653,1559,2531,2699,2698,2697,2696,1755,2700,2701,2706,2708,2528,2522,2523,2524,2526,2527,310'

    #ret = getContentCached(url+'GetFixtureMarkets'+urlParams+'&markets='+bla+'&Fixtures='+fixtureId,fileName,maxAge)
    ret = getContentCached(url+'GetFixtureMarkets'+urlParams+'&Fixtures='+fixtureId,fileName,maxAge)
    return ret


def getMarkets(sport,bookID,bookName,maxAge):
    #print('_GetFixtureMarkets_'+str(Id)+'.json')
    fileName = "Markets_"+str(bookName)+"_"+str(bookID)+"_"+sport["Name"]+"_"+str(sport["Id"])
    #print(fileName)
    ret = getContentCached(url+'GetFixtureMarkets'+urlParams+'&sports='+str(sport["Id"])+'&markets=1,2,52,3'+'&bookmakers='+str(bookID),fileName,maxAge)
    return ret

def getMarketsbyTS(TS,bookID,bookName,maxAge):
    #print('_GetFixtureMarkets_'+str(Id)+'.json')
    fileName = "Markets_"+str(bookName)+"_"+str(bookID)+"_TS"
    #print(fileName)
    ret = getContentCached(url+'GetFixtureMarkets'+urlParams+'&markets=1,2,52,3'+'&bookmakers='+str(bookID)+'&timestamp='+str(TS),fileName,maxAge)
    return ret


def insertSports(param):
    insert_tuple = (param["Name"],param["Id"])
    sql_ins = '''
        insert  ignore into  ls_sport
        ( name,id ) 
        values(?,?) 
        ;
        '''
    curs_prep.execute(sql_ins,insert_tuple)
    mydb.commit()

def insertBooks(param):
    insert_tuple = (param["Name"],param["Id"])
    sql_ins = '''
        insert  ignore into  ls_book
        ( name,id ) 
        values(?,?) 
        ;
        '''
    curs_prep.execute(sql_ins,insert_tuple)
    mydb.commit()
def getMarketsLastTimeStamp(bookID,bookName):
    ts = -1
    try:
        #fileName = "data/_GetFixtures_"+str(sport["Id"])+".json"
        fileName = "data/Markets_"+str(bookName)+"_"+str(bookID)+"_TS.json"
        print(fileName)
        ts = getTS(fileName)
    except Exception as ex:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
    return ts

def loadOutrights(outrightDict,maxOrIDparam):
    global maxOutrightId
    maxID = maxOrIDparam
    nowTS = time.time()    
    outrights = getOutRightsStored(maxOrIDparam)
    elapsedTS=time.time()-nowTS   
    #print("took",elapsedTS)
    ret = {}
    dupecount = 0
    newcount = 0
    nowTS = time.time()
    for c in outrights:
        provider_id = c["provider_id"]
        thisID = c["id"]
        if thisID>maxID:
            maxID = thisID
        if provider_id in outrightDict:
            dupecount += 1
            #print("+")
            pass
        else:
            ret[provider_id]=c
            newcount +=1
        #print(c) 
    maxOutrightId = maxID
    #print(Fore.LIGHTGREEN_EX,"loaded",len(ret),"dupe",dupecount,"newcount",newcount,"maxOutrightId",maxOutrightId,"maxOrIDparam",maxOrIDparam)
    elapsedTS=time.time()-nowTS
    
    return ret


def loadContests(contestDict,maxIDparam):
    global maxContestId
    maxID = maxIDparam
    nowTS = time.time()    
    contests = getContests(maxIDparam)
    elapsedTS=time.time()-nowTS   
    #print("took",elapsedTS)
    ret = {}
    dupecount = 0
    newcount = 0
    nowTS = time.time()
    for c in contests:
        provider_id = c["provider_id"]
        thisID = c["id"]
        if thisID>maxID:
            maxID = thisID
        if provider_id in contestDict:
            dupecount += 1
            #print("+")
            pass
        else:
            ret[provider_id]=c
            newcount +=1
        #print(c) 
    maxContestId = maxID
    #print(Fore.LIGHTRED_EX,"loaded",len(ret),"dupe",dupecount,"newcount",newcount,"maxContestId",maxContestId,"maxIDparam",maxIDparam)
    elapsedTS=time.time()-nowTS
    
    return ret



def insertLocations(param):
    insert_tuple = (param["Name"],param["Id"])
    sql_ins = '''
        insert  ignore into  ls_location
        ( name,id ) 
        values(?,?) 
        ;
        '''
    curs_prep.execute(sql_ins,insert_tuple)
    mydb.commit()

def insertLeagues(param):
    insert_tuple = (param["Name"],param["Id"],param["LocationId"],param["SportId"],param["Season"])
    sql_ins = '''
        insert  ignore into  ls_league
        ( name,id , locationid, sportid, season) 
        values(?,?,?,?,?) 
        ;
        '''
    curs_prep.execute(sql_ins,insert_tuple)
    mydb.commit()


def assembleStatusMessage():
    qsizes={}
    thisPID = os.getpid()
    process = psutil.Process(thisPID)
    
    d_msg={
        "main":{
        "mem":round(process.memory_info().rss / (1024 ** 2),2),
        #"qid":qid,
        #"t.isAlive":t.isAlive(),
        "qsize MB":round(sys.getsizeof(q)/1024,2),
        "idlist MB":round(sys.getsizeof(contestDict)/1024/1024,2),
        "msgCount":msgCount,
        "missList":len(missList),
        "Contest List":len(contestDict),
        "Outright List":len(outrightDict),
        "maxContestId":maxContestId,
        "maxOutrightId":maxOutrightId,
        "streamQueue":streamQueue.qsize(),
        "nowTS":time.time(),				
        
        #"jobs pending": qconsumers._work_queue.qsize(),
        #"threads:":len(qconsumers._threads)
        #"processes:":len(qconsumers._processes)
        },
        
        "msgTypeCountRaw":msgTypeCount.copy()
    }
    msgTypecountNames = {}

    for i in msgTypeCount:
        if i in msg_types:
            name = str(i) + ". " + msg_types[i] 
        else:
            name = str(i) + ". unknown"
        val = msgTypeCount[i]
        msgTypecountNames[name] = val

    d_msg["msgTypeCount"] = msgTypecountNames
    #d_msg["oddTypeCount"] = oddTypeCount.copy()

    for i in q:
        thisQ=q[i]
        qsizes[i]=thisQ.qsize()
        d_msg["queue size"]=qsizes

    #print(Fore.LIGHTBLACK_EX,"pstatus",pstatus_p)
    d_msg["processes"]=pstatus_p.copy()
    #for s in pstatus:
    #    pstatus_msg[s]=pstatus[s]
    #    print("--------------------")
    try:
        mq_client.publish('lsports/client',json.dumps(d_msg,indent=2),retain=True)
    except Exception as ex_mq:
        print(ex_mq)


def processNon1x2markets():
    pass


def processStreamMessage(msg,s_db,s_curs,s_cursd,msgTypeCount,oddTypeCount):       
    
    #findSmallestQueue(q)
    #print(s_db)

    boTypes = {52:'1x2',2:'total',3:'spread',1:"1x2"}
    outrightTypes = (160,299,274,639,300,2494,2534,2533,2495,463,1401,1429,1578,2709,2653,1559,2531,2699,2698,2697,2696,1755,2700,2701,2706,2708,2528,2522,2523,2524,2526,2527,310)
    #print(Fore.LIGHTGREEN_EX,"---------------------")
    #print(msg)
    j={}
    try:
        j = json.loads(msg)
    except:
        #exc_type, exc_obj, exc_tb = sys.exc_info()
        #fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        #print(exc_type, fname, exc_tb.tb_lineno)
        pass
        return
    
    #print(json.dumps(j))
    #print(Fore.GREEN,"---------------------")
    #print(json.dumps(j["Body"]))
    try:
        
        header = j["Header"]
        #print(header)

        msgType = header["Type"]
        if msgType in msgTypeCount:
            msgTypeCount[msgType] += 1
        else:
            msgTypeCount[msgType] = 1

        if msgType in (3,):
            pass
        elif msgType in (35,):
            #f=open("data_settlement/"+str(msgType)+"_"+str(round(time.time()))+".json","w")
            #f.write(json.dumps(j,indent=5))
            #f.close()
            pass            
        elif msgType in (37,38):            
            process_outright_fixture(j,s_curs,s_cursd,s_db)
            '''f=open("data_outright/"+str(msgType)+"_"+str(round(time.time()))+".json","w")
            f.write(json.dumps(j,indent=5))
            f.close()'''
            return
        elif header["Type"] == 2:
            #print("running processScores()",type(j))
            processScores(j,s_db,s_curs,s_cursd)
            return
            
        elif header["Type"] == 1:
            #print("insertFixtures_stream() ")
            insertFixtures_stream(j,s_curs,s_cursd,s_db,True,True) 
            return
        elif header["Type"] in (31,32,34):    
            return
        else:
            #print(header)
            #print(body)
            #fileName = str(header["Type"])+"_"+str(header["MsgGuid"]) +".json"
            #f=open("data_tmp/"+fileName,"w")
            #f.write(json.dumps(j,indent=5))
            #f.close()
            return
            


        body = j["Body"]
        events = body["Events"]
        for e in events:
            
            color = Fore.BLUE
            provider_id=e["FixtureId"]
            if provider_id not in contestDict:
                color = Fore.RED
                #contestNew = handleMissingSingle(provider_id,curs_prep,curs_dict,mydb)
                #print(Fore.RED,"Missing fill:",contestNew)
            hasUsableMarkets = False
            usableMarket=False
            
            try:
                if "Markets" not in e:
                    print("No market",e)
                else:
                    #print(e)
                    markets = e["Markets"]
                    #print(type(markets))
                    if len(markets)>1:
                        print(Fore.BLUE,"Market length:",len(markets))
                    extraMarkets = []
                    for m in markets:
                        marketName = m["Name"]    
                        market_id=m["Id"]
                        #print(market_id,m["Name"])
                        '''oddTypeName = str(market_id)+". "+m["Name"]
                        if oddTypeName in oddTypeCount:
                            oddTypeCount[oddTypeName] += 1
                        else:
                            oddTypeCount[oddTypeName] = 1'''
                        
                        if "Providers" in m:
                            providers = m["Providers"]
                            for book in m["Providers"]:
                                extraMarkets.append((market_id,marketName,book["Id"]+1000,provider_id))
                            if len(providers)>1:                    
                                print("Provider length",len(providers))
                            if market_id in boTypes:
                                '''oddTypeName = str(market_id)+". "+m["Name"]
                                if oddTypeName in oddTypeCount:
                                    oddTypeCount[oddTypeName] += 1
                                else:
                                    oddTypeCount[oddTypeName] = 1'''
                                #print("Market id:",m["Id"])
                                #print(Fore.GREEN,market_id,json.dumps(m,indent=2))
                                usableMarket = True
                            elif market_id in outrightTypes:
                                suffix = "_yep"
                                if provider_id not in outrightDict:
                                    suffix = "_nop"
                                else: 
                                    try:
                                        insertOutrightMarkets(j,s_curs,s_cursd,s_db,True)
                                    except Exception as ex:
                                        print(Fore.RED,"insertOutrightMarkets()",ex)
                                    

                                #f=open("data_outright/"+str(provider_id)+"_"+str(market_id)+"_"+suffix,"w")
                                #f.write(json.dumps(j,indent=5))
                                #f.close()
                            else:
                                pass
                                #print(Fore.RED,m["Name"])
                                                  
                            oddTypeName = str(market_id)+". " + marketName
                            
                            if oddTypeName in oddTypeCount:
                                oddTypeCount[oddTypeName] += 1
                            else:
                                oddTypeCount[oddTypeName] = 1
                        else:
                            print(Fore.YELLOW,"No provider in MSG", m)
                    
                    insert_extra_markets(extraMarkets,s_curs,s_cursd,s_db)
                    if usableMarket:
                        if provider_id not in contestDict:
                            color = Fore.RED
                            #contestNew = handleMissingSingle(provider_id,curs_prep,curs_dict,mydb)
                            #print(Fore.BLUE,"Missing fill:",contestNew)                            
                        else:
                            #print(Fore.YELLOW,msg)
                            insertMarket(provider_id,markets,s_curs,s_cursd,s_db)
                    else:
                        #f=open("data_tmp/"+str(provider_id)+"_"+str(market_id),"w")
                        #f.write(json.dumps(m,indent=5))
                        #f.close()
                        pass

                            

            except Exception as ex2:
                print(ex2)
                print("-------------------------------------------")
                #print(Fore.RED,msg)
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
        return usableMarket
    except Exception as ex3:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        #print(Fore.YELLOW,"HMM j is strange",type(j),j,"\n--------\n",msg)
    return False

if sys.argv[1]=='ena':
    controlPackage('EnablePackage')

if sys.argv[1]=='disa':    
    controlPackage('DisablePackage')


if sys.argv[1]=='bettest':

    sbid = 219
    bets = []
    #print(json.dumps(bets,indent=5))
    bos = {}
    for bet in bets:
        print("------------------")
        print(bet)
        
        modifier = ""
        if "BaseLine" in bet:
            #id=str(idGame)+"_"+item["type"]
            modifier="_"+re.sub('\(.*\)','',bet["BaseLine"]).strip()
            #print("modifier",modifier)

        id = "spread"+modifier 
        
        
        ocitem={}
        ocitem["id"]=bet["Id"]
        ocitem["name"]=bet["Name"]
        LastUpdate = bet["LastUpdate"];

        #print(LastUpdate)
        date_time_obj = datetime.strptime(LastUpdate[0:19], '%Y-%m-%dT%H:%M:%S')
        #print("Now:",LastUpdate,date_time_obj)
        ocitem["lastTS"]=date_time_obj.strftime("%Y-%m-%d %H:%M:%S")
        ocitem["lastTS_ts"]=round(time.mktime(date_time_obj.timetuple()))
        #ocitem["lastTS_ts_check"]=date_time_obj.timetuple()
        
        #ocitem["lastTS_ts_date"]=datetime.utcfromtimestamp().strftime('%Y-%m-%d %H:%M:%S')

        ocitem["odds"]=bet["Price"]
        ocitem["odds_spread"]=re.sub('\(.*\)','',bet["Line"]).strip()
        #item["oc"].append(ocitem)


        if id in bos:
            #print("found",bos[id])
            item=bos[id]
            
            item["oc"].append(ocitem)
            bos[id]=item
            print(json.dumps(item,indent=5))
        else:
            item={}		
            item["type"]="1x2"
            
            #print("NOT found",bos[id])
            item["id"]=id                  
            item["oc"]=[]
            
            item["oc"].append(ocitem)
            bos[id]=item
            
            #print("----------------------")
            #print(item)
        
    boIns=[]
    for id in bos:
        print("------------------"+id+"------------------")
        print(json.dumps(bos[id]))

if sys.argv[1] not in ('orl','orf'):
    maxOrID = 0
    outrightDict = manager.dict()
    outrightDict.update(loadOutrights(outrightDict,maxOrID))

    maxID = 0
    contestDict = manager.dict()
    contestDict.update(loadContests(contestDict,maxID))

    

print("maxContestId:",maxContestId)
if sys.argv[1]=='lines':
    sports = json.loads(getSports())
    pl = {}
    curs = {}
    cursd = {}
    db = {}  
    #print(json.dumps(sports,indent=5))
    missingListDict = {}
    successListDict = {}
    missingDict = {}
    stats = {}
    #contestDict = manager.dict()
    #contestDict.update(loadContests(contestDict,maxID))
    #print(type(contestDict))
    #sys.exit()
    missingDict = manager.dict()
    successDict = manager.dict()
    giveupDict = manager.dict()
    '''bookIDs =    {
        1   : "Unibet",
        110 : "PariMatch",
        210   : "RushBet"
        }'''
    for bookID in bookIDs:
        #contestDict[bookID]="bla"
        #if(sport["Id"]) != 6046:
        #    print(Fore.RED,"Skip soccer :O ")
        #    continue
        #if bookID == 145:
        #    print(Fore.RED,"Skipping 1XBET")

        time.sleep(2)

        bookName = bookIDs[bookID];                               
        proc_key = str(bookID)        
        db[proc_key] = app_config.create_mysql_connection()
        curs[proc_key] = db[proc_key].cursor(prepared=True)
        cursd[proc_key] = db[proc_key].cursor(dictionary=True)
        
        missingListDict[proc_key] = manager.list()
        successListDict[proc_key] = manager.list()
        stats[proc_key] = manager.dict()
        #pl[proc_key]=mp.Process(target=insertFixtureMarkets, args=(sports,bookID,bookName,curs[proc_key],cursd[proc_key],db[proc_key],successListDict[proc_key],missingListDict[proc_key],stats[proc_key]))
        if len(sys.argv)>2:
            if(sys.argv[2] =='ts'):
                #TS = round(time.time())-3600
                TS = getMarketsLastTimeStamp(bookID,bookName)
                pl[proc_key]=mp.Process(target=insertMarkets, args=(sports,bookID,bookName,curs[proc_key],cursd[proc_key],db[proc_key],successDict,missingDict,giveupDict,stats[proc_key],contestDict,TS))
                pl[proc_key].start()
        else:                     
            pl[proc_key]=mp.Process(target=insertMarkets, args=(sports,bookID,bookName,curs[proc_key],cursd[proc_key],db[proc_key],successDict,missingDict,giveupDict,stats[proc_key],contestDict,0))
            pl[proc_key].start()

        '''try:
            insertFixtureMarkets(m,curs[proc_key],cursd[proc_key],db[proc_key],missingListDict[proc_key],successListDict[proc_key],sport["Name"],None)
        except Exception as e2:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
        ''' 
    print(Fore.GREEN,"Startup done.... ")        
    print(mp.active_children())
    should_run = True
    
    #schedule.every(30).seconds.do(handleMissing,md=missingDict)
    
    #stats["misshandler"]
    #mp.Process(target=handleMissing, args=(missingDict))
    loadContests_TS = 0
    lsportsTS = 0
    while should_run:
        print(Fore.MAGENTA,"lsportsTS",lsportsTS)
        schedule.run_pending()
        #contestDict[time.time()]=time.time()
        print("main missingDict",len(missingDict))
        print("main successDict",len(successDict))
        print("main contestDict",len(contestDict))
        
        loadContests_elapsed_TS=time.time()-loadContests_TS  
        if loadContests_elapsed_TS > 60:
            loadContests_TS=time.time()
            contestDict.update(loadContests(contestDict,maxID))

            outrightDict.update(loadOutrights(outrightDict,maxOrID))
        #print("main:",len(missingListDict));
        run_count = 0

        pstatus_msg = {}
        for pid in pl:  
            try:
                pstatus_msg[pid]  = {}
                pstatus_msg[pid] = copy.deepcopy(stats[pid])
                #print(type(pstatus_msg[pid]),type(stats[pid]))
            except Exception as ex:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)

            

        for pid in pl:

            for i in missingListDict[pid]:
                missingDict[i]=i
            
            color = Fore.RED
            thisThreadStatus = False
            try:
                thisThreadStatus = pl[pid].is_alive()
            except:
                pass
            if thisThreadStatus:
                color = Fore.GREEN
                run_count += 1
            if thisThreadStatus:
                try:
                    
                    #print(color,"main:","(",pid,")",":::",pl[pid].is_alive(),":::",len(missingListDict[pid]),"of",len(successListDict[pid]),":: stats: ",stats[pid]);
                    mq_client.publish('lsports/status',json.dumps(pstatus_msg,indent=2),retain=True)
                except Exception as ex:
                    print(ex)
            #print(pl[sid])
            else:
                try:
                    pass
                    '''pl[pid].terminate()
                    cursd[pid].close()
                    curs[pid].close()
                    #missingListDict[proc_key]=None
                    #successListDict[proc_key]=None
                    db[pid].close()
                    #print(color,"main:",sportDict[pid],"(",pid,")",":::",pl[pid].is_alive(),":::",len(missingListDict[pid]),"of",len(successListDict[pid]));
                    '''
                except Exception as ex:
                    print(ex)
        print(Fore.CYAN,len(missingDict))        
            
        print("run_count:",run_count)
        if run_count == 0:
            print("no one else is alive")
            should_run = False
        time.sleep(1)
                        


if sys.argv[1]=='events':
    sports = json.loads(getSports())

    #print(json.dumps(sports,indent=5))
    for sport in sports["Body"]:
        print("------------")
        print(sport)
        getEvents(sport["Id"])
        #insertSports(sport)
        #getMarkets(sport["Id"])
        #insertFixtureMarkets('_GetFixtureMarkets_'+str(sport["Id"])+'.json')
        #time.sleep(5)
context = ssl.SSLContext()
# context.check_hostname = False
# context.verify_mode = ssl.CERT_NONE
context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE



def connect_amq():
    try:
        c = pika.BlockingConnection(app_config.lsports_remote_rabbitmq_params(heartbeat=20))
    except Exception as ex:
        print(Fore.RED, "Failed to connect")
        return None
    return c

def on_message(ch, method, properties, body):
    #global msgCount, msgTypeCount
    #msgCount += 1
    msgObj = {}
    routing_key = method.routing_key
    msgObj["routing_key"] = routing_key
    try:
        j = json.loads(body)
        #jbody = j["Body"]
        header = j["Header"]    
        msgType = header["Type"]
        #if msgType in msgTypeCount:
        #    msgTypeCount[msgType] += 1
        #else:
        #    msgTypeCount[msgType] = 1
    except Exception as ex:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
    with open('FIFO', 'w') as fifo:
        fifo.write(body.decode("utf-8"))

def on_message_client(ch, method, properties, body):
    #print(body)
    global msgCount, msgTypeCount

    #msgCount += 1
    msgObj = {}
    routing_key = method.routing_key
    msgObj["routing_key"] = routing_key
    ch.basic_ack(delivery_tag=method.delivery_tag)
    msgCount += 1
    try:
        j = json.loads(body)
        #jbody = j["Body"]
        header = j["Header"]    
        msgType = header["Type"]
        #if msgType in msgTypeCount:
        #    msgTypeCount[msgType] += 1
        #else:
        #    msgTypeCount[msgType] = 1
    except Exception as ex:
        #exc_type, exc_obj, exc_tb = sys.exc_info()
        #fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        #print(exc_type, fname, exc_tb.tb_lineno)
        print("Stream message parsing failed",ex)

    #with open('FIFO', 'w') as fifo:
    #fifo.write(body.decode("utf-8"))
    try: 
        loqQueueIndex = findSmallestQueue(q1)
        if q1[loqQueueIndex].full():
            print(Fore.RED,loqQueueIndex," is full:",q1[loqQueueIndex].full(),Fore.WHITE)
            time.sleep(0.5)            
            while q1[loqQueueIndex].full(): 
                print(Fore.LIGHTRED_EX,"still full:",q1[loqQueueIndex].qsize(),Fore.WHITE)                
                time.sleep(0.5)                
            print(Fore.GREEN,loqQueueIndex," cleared :",q1[loqQueueIndex].qsize(),Fore.WHITE)  
        q1[loqQueueIndex].put_nowait(body.decode("utf-8"))
        #q1[loqQueueIndex].qsize()
        #print(q1.qsize())
    except Exception as ex:
        print("Queue dispatch failed",ex)
        #print(exc_type, fname, exc_tb.tb_lineno)


def runClient():
    #ch.basic_consume(queue="dds-BetSlipRTv4",
    #                on_message_callback=on_message, auto_ack=True)
    try:
        ch.basic_consume(queue="_4620_",
                        on_message_callback=on_message, auto_ack=True)               
        '''ch.basic_consume(queue="plannatech",
                        on_message_callback=on_message, auto_ack=True)'''
        ch.start_consuming()
    except Exception as ex:
        print(ex)
        return None

def runClient_client():
    #ch.basic_consume(queue="dds-BetSlipRTv4",
    #                on_message_callback=on_message, auto_ack=True)
    try:
        ch.basic_consume(queue="_4620_",
                        on_message_callback=on_message_client, auto_ack=True)               
        '''ch.basic_consume(queue="plannatech",
                        on_message_callback=on_message, auto_ack=True)'''
        ch.start_consuming()
    except Exception as ex:
        print(ex)
        return None

def runClient_new_local():
    while(True):
        controlPackage('EnablePackage')
        time.sleep(5)        
        try:            
            now = datetime.now()
            dt_sting = now.strftime("%d/%m/%Y %H:%M:%S")
            print(Fore.BLUE,dt_sting,"Connecting...")
            connection = pika.BlockingConnection(
                app_config.local_rabbitmq_params(
                    "runClient_new_local_lsports",
                    heartbeat=20,
                )
            )	
            print(Fore.CYAN,dt_sting,"Connected")
            channel = connection.channel()
            channel.basic_qos(prefetch_count=100)
            channel.basic_consume('lsports', on_message_client)
            print(Fore.GREEN,dt_sting,"Consuming")
            #channel.basic_consume(queue="_4620_",on_message_callback=on_message_client, auto_ack=True) 
            try:
                channel.start_consuming()
                
            except KeyboardInterrupt:
                channel.stop_consuming()
                connection.close()
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


def runClient_new():
    while(True):
        controlPackage('EnablePackage')
        time.sleep(5)        
        try:            
            now = datetime.now()
            dt_sting = now.strftime("%d/%m/%Y %H:%M:%S")
            print(Fore.BLUE,dt_sting,"Connecting...")
            connection = pika.BlockingConnection(app_config.lsports_remote_rabbitmq_params(heartbeat=20))
            print(Fore.CYAN,dt_sting,"Connected")
            channel = connection.channel()
            channel.basic_qos(prefetch_count=100)
            channel.basic_consume('_4620_', on_message_client)
            print(Fore.GREEN,dt_sting,"Consuming")
            #channel.basic_consume(queue="_4620_",on_message_callback=on_message_client, auto_ack=True) 
            try:
                channel.start_consuming()
                
            except KeyboardInterrupt:
                channel.stop_consuming()
                connection.close()
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
        




q={}
pstatus = manager.dict()
pstatus_p = manager.dict()
context = ssl.SSLContext()
missList = manager.list()
gameIdList = manager.list()
streamQueue= mp.Queue(500000)
q = {}
q1 = {}

if sys.argv[1] in ('stream','client'):
    for i in range(0,4):
        
        q[i] = mp.Queue(50000)
        print("Creating PUBLISHING queue and process",i)
        #p4=mp.Process(target=qreader,args=((q[qid],qid,gameIdList,missList,pstatus),),daemon=True)
        #p4=mp.Process(target=publishHandler,args=((q[qid],qid,gameIdList,missList,pstatus),),daemon=True)
        p4=mp.Process(target=publishHandler, args=((q[i],False,i,pstatus_p,),))
        p4.start()
    db_stream = {}
    curs_stream = {} 
    cursd_stream = {}
    for i in range(0,4):
        db_stream[i] = app_config.create_mysql_connection()
        curs_stream[i] = db_stream[i].cursor(prepared=True)
        cursd_stream[i] = db_stream[i].cursor(dictionary=True)

        q1[i] = mp.Queue(15000)
        print("Creating READING queue and process",i)
        p5=mp.Process(target=qreader,args=((q1[i],i,pstatus_p,db_stream[i],curs_stream[i],cursd_stream[i],msgTypeCount,oddTypeCount,),))
        p5.start()

if sys.argv[1] == 'ormt':
    
    f = open('data/Markets_bla.json')
    j = json.load(f)
    f.close()
    
    insertOutrightMarkets(j,curs_prep,curs_dict,mydb,False)

if sys.argv[1] == 'orm':
    #outrights = getOutRightsStored(0)
    orStr = ""
    ocnt = 0
    ocnt2 = 0
    for provider_id in outrightDict:
        ocnt += 1 
        ocnt2 +=1
        if ocnt%10==0:
            print("--------------")
            print(Fore.GREEN,orStr)
            orStr = ""
            ocnt2 = 1
            try:            
                data = getMarketsByFixtures(orStr,0)
                j= json.loads(data)
                #print(j)
                insertOutrightMarkets(j,curs_prep,curs_dict,mydb,False)
                              
            except Exception as ex:
                print(Fore.RED,ex)
                print(data)
            time.sleep(2) 
        
        if ocnt2>1:
            orStr += ","
        orStr += str(provider_id)
    if len(orStr)>0:
        print("leftover",len(orStr),ocnt,ocnt2)
        print(Fore.LIGHTBLACK_EX,"---------------\n")
        print(orStr)
        try:            
            data = getMarketsByFixtures(orStr,0)
            j= json.loads(data)
            insertOutrightMarkets(j,curs_prep,curs_dict,mydb,False)
                           
        except Exception as ex:
            print(Fore.RED,ex)
            print(data)
        time.sleep(2) 
        
    #j= json.loads(getMarketsByFixtures(orStr,0))
    #insertOutrightMarkets(j)


if sys.argv[1] == 'onemarket':
    fixtureId = sys.argv[2]
    j= json.loads(getMarketsByFixture_singular(fixtureId,0))
    print(json.dumps(j,indent=5))
orlDo = False
orfDo = False

if sys.argv[1] == 'orlf':
    orlDo = True
    orfDo = True

if sys.argv[1] == 'orl' or orlDo:
    print('OutrightLeagues')
    TS = 0
    j = json.loads(getOutrightLeagues(TS))
    
    process_outright_fixture(j,curs_prep,curs_dict,mydb)

    '''for i in j["Body"]:
        ls_id = i["Id"]
        ls_name = i["Name"]
        ls_type = i["Type"]
        #print(ls_id,ls_name)
        ## this thing here not in Fixtures        
        for comp in i["Competitions"]:
            comp_type = comp["Type"]
            comp_name = comp["Name"]
            comp_id = comp["Id"]
            #print("-------------------")
            #print()
            for e in comp["Events"]:
                
                #print("-------------------")
                #print(e["OutrightFixture"])
                ## instead of fixture !!!!
                of = e["OutrightLeague"]
                LastUpdate = re.sub("T"," ",of["LastUpdate"][:19])
                sportName = of["Sport"]["Name"]
                if sportName in ("Horse Racing","Greyhounds","Trotting"):
                    continue
                locationName = of["Location"]["Name"]
                #run_date = re.sub("T"," ",of["StartDate"])
                run_date = "0000-00-00 00:00:00"
                provider_id = e["FixtureId"]
                
                

                insert_tuple=(
                    ls_id,
                    ls_name,
                    ls_type,
                    comp_id,
                    comp_type,
                    comp_name,
                    sportName,
                    locationName,                
                    run_date,
                    provider_id,
                    LastUpdate
                )
                #print(insert_tuple)
                insert_local_outright(insert_tuple,curs_prep,curs_dict,mydb,False)
            '''

if sys.argv[1] == 'orf' or orfDo:
    print('OutrightFixtures')
    TS = 0
    j= json.loads(getOutrightFixtures(TS))
    #print(json.dumps(j,indent=3))
    process_outright_fixture(j,curs_prep,curs_dict,mydb)
    '''
    for i in j["Body"]:
        ls_id = i["Id"]
        ls_name = i["Name"]
        ls_type = i["Type"]
        for e in i["Events"]:
            
            #print("-------------------")
            #print(e["OutrightFixture"])
            of = e["OutrightFixture"]
            LastUpdate = re.sub("T"," ",of["LastUpdate"][:19])
            sportName = of["Sport"]["Name"]
            if sportName in ("Horse Racing","Greyhounds","Trotting"):
                continue
            locationName = of["Location"]["Name"]
            run_date = re.sub("T"," ",of["StartDate"])
            provider_id = e["FixtureId"]
            
            comp_type = 0
            comp_name = ""
            comp_id = ""

            insert_tuple=(
                ls_id,
                ls_name,
                ls_type,
                comp_id,
                comp_type,
                comp_name,
                sportName,
                locationName,                
                run_date,
                provider_id,
                LastUpdate
            )
            #print(insert_tuple)
            insert_local_outright(insert_tuple,curs_prep,curs_dict,mydb,False)
        '''

if sys.argv[1] == 'client':
    #activemq crap
    '''context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE    
    c=connect_amq()
    ch = None
    connectionFailed = False
    try:
        ch = c.channel()
    except Exception as ex: 
        connectionFailed = True  
        print(ex)             
        pass'''
    threads = []
    elapsed=0
    #t = threading.Thread(target=runClient_new) #, args=(json.loads(body))
    t = threading.Thread(target=runClient_new_local) #, args=(json.loads(body))
    t.start()
    threads.append(t)
    print("thread count",len(threads))

    qsizes = {}
    d_msg = {}
    loadContests_TS = 0
    nowTS=time.time()
    while True:
        time.sleep(0.001)
        try:
            if isinstance(mydb,mysql.connector.connection_cext.CMySQLConnection):
                if mydb.is_connected():
                    isConnected = True
                else:
                    print('DB connection null')
                    isConnected = False
            if not isConnected:
                retryCount = 0
                while not isConnected:
                    retryCount += 1
                    #print(qid," retry loop ",retryCount)
                    time.sleep(1)
                    try:
                        mydb = mysql.connector.connect(**cparams)
                        curs_prep = mydb.cursor(prepared=True)
                        curs_dict = mydb.cursor(dictionary=True)
                        isConnected = True
                        print(Fore.GREEN)
                    except Exception as ex2:
                        print(ex2)
                else:
                    isConnected = True

            loadContests_elapsed_TS=time.time()-loadContests_TS  
            if loadContests_elapsed_TS > 30:
                loadContests_TS=time.time()
                contestDict.update(loadContests(contestDict,maxContestId))
                outrightDict.update(loadOutrights(outrightDict,maxOrID))
        except Exception as exload:
            print(exload)

        elapsedTS=time.time()-nowTS            
        if elapsedTS>1:
            nowTS=time.time()
            #print(Fore.LIGHTYELLOW_EX,pstatus_p)
            assembleStatusMessage()

        #MQ error detection end
        '''if c is None:
            print(Fore.RED,)
            c = connect_amq()
        if c is None:
            controlPackage('EnablePackage')
            time.sleep(1)                
        else:                
            if ch is None:
                print(Fore.YELLOW,"c",type(c),"ch",type(ch))
                print(Fore.MAGENTA,"channel is none")                                  
                ch = c.channel()                   
            if ch is None :
                #print(Fore.YELLOW,"c",type(c),"ch",type(ch))
                #print(Fore.CYAN,"Channel is null")
                #ch = c.channel()                 
                connectionFailed = True
            else:
                #ch = c.channel()
                #print("ch.is_open:",ch.connection.is_open)
                if not ch.connection.is_open:                
                    try:
                        time.sleep(5)
                        print("Trying reconnect ... ")
                        c = connect_amq()
                        ch = c.channel()    
                    except Exception as bla:
                        pass
                if ch.connection.is_open:
                    pass
                    #print(Fore.GREEN,"is open")
                    connectionFailed = False
                else:
                    connectionFailed = True            
        #MQ error detection end
        #MQ error handling start         
        if connectionFailed:
            try:
                #c.close()
                controlPackage('EnablePackage')
                time.sleep(2)
                c = connect_amq()  
                time.sleep(2)              
                if ch is None:
                    print(Fore.YELLOW,"c",type(c),"ch",type(ch))
                    print(Fore.MAGENTA,"channel is none")                                   
                    ch = c.channel()                            
            except Exception as ex5:
                print(Fore.RED, "Reconnect failed too :(",ex5)
        if not t.isAlive():                    
            print(Fore.RED,"Thread is dead, starting new")            
            #t = threading.Thread(target=runClient_client) #, args=(json.loads(body))
            #t.start()
            #threads.append(t)
            sys.exit()
        #MQ error handling end '''


    


if sys.argv[1]=='streamclient':
    
    # context.check_hostname = False
    # context.verify_mode = ssl.CERT_NONE
    context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    c=connect_amq()
    ch = None
    connectionFailed = False
    try:
        ch = c.channel()
    except Exception as ex: 
        connectionFailed = True               
        pass
    threads = []
    elapsed=0
    #runClient()
    t = threading.Thread(target=runClient) #, args=(json.loads(body))
    t.start()
    threads.append(t)

    qid =0
    process = psutil.Process(os.getpid())
    nowTS=time.time()
    while True:
        time.sleep(0.001)
        elapsedTS=time.time()-nowTS    
        
        if elapsedTS>5:
            
            nowTS=time.time()
            crap = {}

            d_msg={
                "main":{
                "mem":round(process.memory_info().rss / (1024 ** 2),2),
                "qid":qid,
                "t.isAlive":t.isAlive(),
                #"qsize MB":sys.getsizeof(q)/1024,2),
                #"contestDict MB":round(sys.getsizeof(json.dumps(list(contestDict)))/1024/1024,2),
                "msgCount":msgCount,
                "missList":len(missList),
                "maxContestId":maxContestId,
                #"contestDict":len(contestDict),
                "nowTS":time.time(),				
                
                #"jobs pending": qconsumers._work_queue.qsize(),
                #"threads:":len(qconsumers._threads)
                #"processes:":len(qconsumers._processes)
                },
                
                #"msgTypeCount":(msgTypeCount.copy())
                }
            print(d_msg)
            
            
            pstatus_msg={}            
            for s in pstatus:
                pstatus_msg[s]=pstatus[s]
            for s in pstatus_p:
                pstatus_msg[s]=pstatus_p[s]
            
            qsizes={}
            d_msg["processes"]=pstatus_msg
            
            for i in q:
                thisQ=q[i]
                qsizes[i]=thisQ.qsize()
                
            d_msg["queue size"]=qsizes
            
            try:
                d_msg["ch.is_open"]=ch.is_open
                d_msg["ch.connection.is_open"]=ch.connection.is_open
            except:
                pass
            try:
                mq_client.publish('lsports/streamclient',json.dumps(d_msg,indent=2),retain=True)
            except Exception as ex_mq:
                print(Fore.RED,ex_mq)
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)
            
            
            if c is None:
                print(Fore.RED,)
                c = connect_amq()
            if c is None:
                controlPackage('EnablePackage')
                time.sleep(1)                
            else:                
                if ch is None:
                    print(Fore.YELLOW,"c",type(c),"ch",type(ch))
                    print(Fore.MAGENTA,"channel is none")                                   
                    ch = c.channel()                   
                if ch is None :
                    #print(Fore.YELLOW,"c",type(c),"ch",type(ch))
                    #print(Fore.CYAN,"Channel is null")
                    #ch = c.channel()                 
                    connectionFailed = True
                else:
                    #ch = c.channel()
                    #print("ch.is_open:",ch.connection.is_open)
                    if not ch.connection.is_open:
                        try:
                            print("connection closed")
                            c = connect_amq()
                            ch = c.channel()
                            print("ch.is_open:",ch.connection.is_open)
                        except Exception as bla:
                            pass
                    if ch.connection.is_open:
                        pass
                        #print(Fore.GREEN,"is open")
                        connectionFailed = False
                    else:
                        connectionFailed = True

                

        if connectionFailed:
            try:
                #c.close()
                controlPackage('EnablePackage')
                time.sleep(2)
                c = connect_amq()                
                time.sleep(2)
                if ch is None:
                    print(Fore.YELLOW,"c",type(c),"ch",type(ch))
                    print(Fore.MAGENTA,"channel is none")                                   
                    ch = c.channel()           
            except Exception as ex5:
                print(Fore.RED, "Reconnect failed too :(",ex5)

        if not t.isAlive():                        
            print(Fore.RED,"Thread is dead, starting new")
            t = threading.Thread(target=runClient) #, args=(json.loads(body))
            t.start()
            threads.append(t)
                





if sys.argv[1]=='stream':
    FIFO = 'FIFO'

    loadContests_TS = 0
    try:
        os.mkfifo(FIFO)
    except OSError as oe: 
        if oe.errno != errno.EEXIST:
            raise
    prev_data_len = 0
    msgCount = 0
    msgCountPrev = 0
    usableCnt = 0
    unUsableCnt = 0
    msgTS = 0
    nowTS=time.time()
    process = psutil.Process(os.getpid())
    while True:
        
        loadContests_elapsed_TS=time.time()-loadContests_TS  
        if loadContests_elapsed_TS > 30:
            loadContests_TS=time.time()
            contestDict.update(loadContests(contestDict,maxContestId))

        elapsedTS=time.time()-nowTS   
        if elapsedTS>1:
            nowTS = time.time()

            qsizes={}
            d_msg={
                    "main":{
                    "mem":round(process.memory_info().rss / (1024 ** 2),2),
                    #"qid":qid,
                    #"t.isAlive":t.isAlive(),
                    "qsize MB":round(sys.getsizeof(q)/1024,2),
                    "idlist MB":round(sys.getsizeof(contestDict)/1024/1024,2),
                    "msgCount":msgCount,
                    "missList":len(missList),
                    "gameList":len(contestDict),
                    "maxContestId":maxContestId,
                    "streamQueue":streamQueue.qsize(),
                    "nowTS":time.time(),				
                    
                    #"jobs pending": qconsumers._work_queue.qsize(),
                    #"threads:":len(qconsumers._threads)
                    #"processes:":len(qconsumers._processes)
                    },
                    
                    "msgTypeCount":msgTypeCount.copy()
                    }
            pstatus_msg={}            
            for s in pstatus:
                pstatus_msg[s]=pstatus[s]
            for s in pstatus_p:
                pstatus_msg[s]=pstatus_p[s]
            
            qsizes={}
            d_msg["processes"]=pstatus_msg
            
            for i in q:
                thisQ=q[i]
                qsizes[i]=thisQ.qsize()
                d_msg["queue size"]=qsizes
            try:
                mq_client.publish('lsports/stream',json.dumps(d_msg,indent=2),retain=True)
            except Exception as ex_mq:
                print(ex_mq)
        # end publish status
        with open(FIFO) as fifo:
                #print("FIFO opened")
                try:
                    while True:                        
                        msg_elapsed_TS=time.time()-msgTS
                        if msg_elapsed_TS>10:
                            msg_elapsed_TS
                            msgPerSec = (msgCount-msgCountPrev)/msg_elapsed_TS
                            print(Fore.GREEN,"messages/sec:",msgPerSec,"totel:",msgCount,"usable/un-usable",usableCnt,"/",unUsableCnt)
                            msgCountPrev = msgCount
                            msgTS = time.time()

                        data = fifo.read()
                        
                        if len(data) == 0:
                            #print(Fore.RED,"Writer closed")
                            break
                        #print(len(data))
                        try:
                            if  prev_data_len == len(data):
                                continue
                                pass                      
                            else:       
                                prev_data_len = len(data)                                                                        
                        except:
                            pass
                        #print('Read: "{0}"'.format(data))
                        if "\n\n" in data:
                            msgs=data.split("\n\n")
                            for m in msgs:
                                if(len(m)>0):
                                    #pfl.processChunk(m)
                                    print(Fore.RED,m)
                                    
                        else:
                            msgCount += 1
                            
                            
                            #print(Fore.GREEN,data)
                            loqQueueIndex = findSmallestQueue(q1)                            
                            q1[loqQueueIndex].put_nowait(data)
                            '''if(processStreamMessage(data)):
                                usableCnt += 1
                            else:
                                unUsableCnt += 1'''
                            #pfl.processChunk(data)
                except Exception as ex:
                    print(ex)

if sys.argv[1]=='sports':
    sports = json.loads(getSports())

    #print(json.dumps(sports,indent=5))
    for sport in sports["Body"]:
        print("------------")
        print(sport)
        insertSports(sport)
        #getMarkets(sport["Id"])
        #insertFixtureMarkets('_GetFixtureMarkets_'+str(sport["Id"])+'.json')
        #time.sleep(5)

if sys.argv[1]=='books':
    books = json.loads(getBooks())

    print(json.dumps(books,indent=5))
    for book in books["Body"]:
        print("------------")
        print(book)
        insertBooks(book)

if sys.argv[1]=='locations':
    locations = json.loads(getLocations())

    print(json.dumps(locations,indent=5))
    for location in locations["Body"]:
        print("------------")
        print(location)
        insertLocations(location)

if sys.argv[1]=='leagues':
    leagues = json.loads(getLeagues())

    #print(json.dumps(leagues,indent=5))
    for league in leagues["Body"]:
        print("------------")
        print(league)
        insertLeagues(league)

if sys.argv[1] == 'dbload':
    nowTS = time.time()    
    contests = getContests()
    elapsedTS=time.time()-nowTS   
    #print("took",elapsedTS)
    nowTS = time.time()
    for c in contests:
        provider_id = c["provider_id"]
        contestDict[provider_id]=c
        #print(c)
    print(len(contestDict))
    elapsedTS=time.time()-nowTS   



def loadContest_DDS(contestDDS_Dict):
    nowTS = time.time()    
    contests = getContests_DDS()
    elapsedTS=time.time()-nowTS   
    #print("took",elapsedTS)
    ret = {}
    dupecount = 0
    newcount = 0
    nowTS = time.time()
    #print(contests)
    for c in contests:
        provider_id = c["provider_id"]
        
        if provider_id in contestDDS_Dict:
            dupecount += 1
            #print("+")
            pass
        else:
            ret[provider_id]=c
            newcount +=1
        #print(c)
    print(Fore.LIGHTCYAN_EX,"loaded",len(ret),"dupe",dupecount,"newcount",newcount)
    elapsedTS=time.time()-nowTS
    return ret

if sys.argv[1] == 'ddsload':
    contestDDS_Dict = manager.dict()
    contestDDS_Dict.update(loadContest_DDS(contestDDS_Dict))
    

if sys.argv[1] == 'tstestlines':
    for bookID in bookIDs:
        print(bookID,bookIDs[bookID])
        bookName = bookIDs[bookID]
        ts = getMarketsLastTimeStamp(bookID,bookName)
        now = round(time.time())
        print(ts,(now-ts))
    #tsMin = getFixtureLastTimeStamp()
    #print("tsMin found", tsMin)



if sys.argv[1] == 'scores':
    #tsMin = getFixtureLastTimeStamp()
    scores = getScores(0)
    #print(scores)
    j = json.loads(scores)
    processScores(j,mydb,curs_prep,curs_dict)
    #print("tsMin found", tsMin)

if sys.argv[1] == 'scorests':
    #tsMin = getFixtureLastTimeStamp()
    #scores = getScores(0);
    #print(scores)
    #j = json.loads(scores)
    #procesScores(j)
    #print("tsMin found", tsMin)    
    ts = getTS("data/GetScores.json")
    print(ts)
    scores = getScores(ts)
    j = json.loads(scores)
    processScores(j,mydb,curs_prep,curs_dict)


if sys.argv[1] == 'tstest':
    tsMin = getFixtureLastTimeStamp()
    print("tsMin found", tsMin)
    
    

if sys.argv[1] == 'misstest1':
    missingDict = {7028014: 7028014, 8554840: 8554840, 9241444:9241444,9241269:9241269}
    
    print("----------test 1 good--------------")
    contest = handleMissingSingle(9241269,curs_prep,curs_dict,mydb)
    print(type(contest),len(contest),contest)


    print("----------test 2 bad--------------")
    contest = handleMissingSingle(8554840,curs_prep,curs_dict,mydb)
    print(type(contest),len(contest),contest)



    
if sys.argv[1] == 'misstest':
    missingDict = {7028014: 7028014, 8554840: 8554840, 9241444:9241444,9241269:9241269}
    handleMissing(missingDict,curs_prep,curs_dict,mydb)
    #schedule.every(10).seconds.do(handleMissing,md=missingDict)    
    #while True:
    #    time.sleep(1)
    #    schedule.run_pending()

if sys.argv[1] == 'cachetest':
    getContentTest()



if sys.argv[1]=='fixturests':
    tsMin = round(time.time())-7200
    try:
        tsMin = getFixtureLastTimeStamp()
        print("tsMin found", tsMin)
    except Exception as ex:
        print(ex)
    
    contestDict = manager.dict()
    contestDict.update(loadContests(contestDict,maxID))
    fixtures = getFixturesByTS(tsMin)

    #print(type(fixtures))
    insertFixtures(fixtures,curs_prep,curs_dict,mydb,False,True)
    

if sys.argv[1]=='fixtures':    
    
    pf = {}
    curs = {}
    cursd = {}
    db = {}    
    
    #contestDict = manager.dict()
    #contestDict.update(loadContests(contestDict))
    sports = json.loads(getSports())    
    for sport in sports["Body"]:
        sid =sport["Id"]
        db[sid] = app_config.create_mysql_connection()
        curs[sid] = db[sid].cursor(prepared=True)
        cursd[sid] = db[sid].cursor(dictionary=True)
        fixtures = getFixtures(sport["Id"])
        if  not isinstance(fixtures,bytes):
            fixtures = str.encode(fixtures)

        pf[sid]=mp.Process(target=insertFixtures, args=(fixtures,curs[sid],cursd[sid],db[sid],False,True))
        pf[sid].start()
        #time.sleep(5)
        #insertFixtures(fixtures,curs[sid],db[sid],None)
    should_run = True
    print(Fore.GREEN,"Startup done.... ") 
    print(mp.active_children())    
    while should_run:
        run_count = 0
        for pid in pf:
            
            try:
                thisThreadStatus = pf[pid].is_alive()
            except:
                pass

            if thisThreadStatus:
                color = Fore.GREEN
                run_count += 1
                try:                    
                    print(color,"main:","(",pid,")",":::",pf[pid].is_alive())
                except:
                    pass

        if run_count == 0:
            print("no one else is alive")
            should_run = False
        time.sleep(5)
    
if sys.argv[1]=='fixtures_football':    
    
    pf = {}
    curs = {}
    cursd = {}
    db = {}    
    
    
    sid =6046
    print(sid)    
    print("SOCCER FOUND")           
    #db[sid] = dbcon.mydb
    #curs[sid] = db[sid].cursor(prepared=True)
    db[sid] = app_config.create_mysql_connection()
    curs[sid] = db[sid].cursor(prepared=True)
    cursd[sid] = db[sid].cursor(dictionary=True)
    #print("Getting fixtures:",sport["Name"],sid)
    fixtures = getFixtures(sid)
    if  not isinstance(fixtures,bytes):
        fixtures = str.encode(fixtures)
        
    insertFixtures(fixtures,curs[sid],cursd[sid],db[sid],False,True)
    
    



#get_schedule()
#get_fixture_markets()
