import redis
import time
import sys
import requests
import json

r = redis.StrictRedis(host='127.0.0.1', port=8080)
baseurl='http://127.0.0.1:7000'
headers={'content-type':'application/json'}

def save_jvm_monitor(uid,j): 
    jj=j.split(';')
    val={}
    val['monitor_time']=jj[0]
    val['used_heap_mem']=jj[1]
    val['used_nonheap_mem']=jj[2]
    val['thread_count']=jj[3]
    val['cpu_load']=jj[4]
    val['class_count']=jj[5]
    val['open_file_descriptor']=jj[6]
    val['id']=uid
    url = str(baseurl)+'/jvm/'+str(uid)+'/monitor'
    try:
        requests.post(url,data=json.dumps(val),headers=headers)
    except Exception,e:
        print e

def save_jvm_bes_monitor(uid,b):
    bb=b.split(';')
    val={}
    val['monitor_time']=bb[0]
    val['http_total_threads']=bb[1]
    val['http_idle_threads']=bb[2]
    val['http_busy_threads']=bb[3]
    val['session_active']=bb[4]
    val['queued']=bb[5]
    val['id']=uid
    url = str(baseurl)+'/jvm/'+str(uid)+'/bes/monitor'
    try:
        requests.post(url,data=json.dumps(val),headers=headers)
    except Exception,e:
        print e

def save_jvm_tomcat_monitor(uid,b):
    bb=b.split(';')
    val={}
    val['monitor_time']=bb[0]
    val['http_total_threads']=bb[1]
    val['http_busy_threads']=bb[2]
    val['ajp_total_threads']=bb[3]
    val['ajp_busy_threads']=bb[4]
    val['session_active']=bb[5]
    val['id']=uid
    url = str(baseurl)+'/jvm/'+str(uid)+'/tomcat/monitor'
    try:
        requests.post(url,data=json.dumps(val),headers=headers)
    except Exception,e:
        print e

def save_jvm_c3p0_monitor(uid,b):
    bb=b.split(';')
    val={}
    val['monitor_time']=bb[0]
    val['name']=bb[1]
    val['num_connections']=bb[2]
    val['num_busy_connections']=bb[3]
    val['thread_pool_size']=bb[4]
    val['thread_pool_num_idle_threads']=bb[5]
    val['id']=uid
    url = str(baseurl)+'/jvm/'+str(uid)+'/c3p0/monitor'
    try:
        requests.post(url,data=json.dumps(val),headers=headers)
    except Exception,e:
        print e

def worker(info):
    if info:
        while True:
            #print "worker run"
            try:
                #print info+"-jvm"
                uid=hash(info)
                j=r.lpop(str(info)+'-jvm')
                if j:
                        if r.exists(str(info)+'-bes'):
                                b=r.lpop(str(info)+'-bes')
                                if b:
                                        save_jvm_bes_monitor(uid,b)
                        if r.exists(str(info)+'-tomcat'):
                                b=r.lpop(str(info)+'-tomcat')
                                if b:
                                        save_jvm_tomcat_monitor(uid,b)
                        save_jvm_monitor(uid,j)
                if r.exists(str(info)+'-c3p0'):
                        b=r.lpop(str(info)+'-c3p0')
                        if b:
                                save_jvm_c3p0_monitor(uid,b)
            except Exception,e:
                print e
        time.sleep(0.2)

if __name__=="__main__":
    if len(sys.argv)>1:
            info=sys.argv[1]
            worker(info)
    else:
            print "usage:\npython worker.py <info>"