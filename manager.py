import redis
import time
import os
import requests
import json

jvmlist=[]
r = redis.StrictRedis(host='127.0.0.1', port=8080)
headers = {'content-type':'application/json'}
baseurl = 'http://127.0.0.1:7000'
def save_jvm_base(uid,val):
    val['id']=uid
    url = str(baseurl)+'/jvm/'+str(uid)
    requests.post(url,data=json.dumps(val),headers=headers)

def save_jvm_bes_base(uid,val):
    val['id']=uid
    url = str(baseurl)+'/jvm/'+str(uid)+'/bes'
    requests.post(url,data=json.dumps(val),headers=headers)

def save_jvm_tomcat_base(uid,val):
    val['id']=uid
    url = str(baseurl)+'/jvm/'+str(uid)+'/tomcat'
    requests.post(url,data=json.dumps(val),headers=headers)

def save_jvm_c3p0_base(uid,b):
    bb=b.split(';')
    val = {}
    val['id']=uid
    val['name'] = bb[0]
    val['initial_pool_size'] = bb[1]
    val['min_pool_size'] = bb[2]
    val['max_pool_size'] = bb[3]
    val['id']=uid
    url = str(baseurl)+'/jvm/'+str(uid)+'/c3p0'
    requests.post(url,data=json.dumps(val),headers=headers)

def startworker(info):
    print info
    os.system('/usr/local/bin/python /root/redis2oracle/worker.py '+info+' &')

def manager():
    while True:
        #print "manager run"
        try:
            lists=r.hkeys('jvm-list')
            for l in lists:
                if not r.exists(str(l)+"-jvmbase"):
                    continue
                if l not in jvmlist:
                    #print l
                    jvmlist.append(l)
                    startworker(l)
                uid=hash(l)
                j=r.hgetall(str(l)+"-jvmbase")
                save_jvm_base(uid,j)
                if r.exists(str(l)+"-besbase"): 
                    #print "has bes"
                    b=r.hgetall(str(l)+"-besbase")
                    save_jvm_bes_base(uid,b)
                if r.exists(str(l)+"-tomcatbase"):
                    #print "has tomcat"
                    b=r.hgetall(str(l)+"-tomcatbase")
                    save_jvm_tomcat_base(uid,b)
                if r.exists(str(l)+"-c3p0base"):
                    for i in range(r.llen(str(l)+"-c3p0base")):
                        b=r.lindex(str(l)+"-c3p0base",i)
                        save_jvm_c3p0_base(uid,b);
            time.sleep(600)
        except Exception,e:
            print e

if __name__=="__main__":
    manager()