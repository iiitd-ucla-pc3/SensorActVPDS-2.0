import time
import json,urllib,urllib2,subprocess,time,csv,sys,random

# upload script for mango m2m http receiver datastore

INTERVAL = 0.1
URL = "http://128.97.93.30:8080/httpds?__device=cpuload&load="

def getTimeList():
    """
    Fetches a list of time units the cpu has spent in various modes
    Detailed explanation at http://www.linuxhowtos.org/System/procstat.htm
    """
    cpuStats = file("/proc/stat", "r").readline()
    columns = cpuStats.replace("cpu", "").split(" ")
    return map(int, filter(None, columns))

def deltaTime(interval):
    """
    Returns the difference of the cpu statistics returned by getTimeList
    that occurred in the given time delta
    """
    timeList1 = getTimeList()
    time.sleep(interval)
    timeList2 = getTimeList()
    return [(t2-t1) for t1, t2 in zip(timeList1, timeList2)]

def getCpuLoad():
    """
    Returns the cpu load as a value from the interval [0.0, 1.0]
    """
    dt = list(deltaTime(INTERVAL))
    idle_time = float(dt[3])
    total_time = sum(dt)
    load = 1-(idle_time/total_time)
    return load


while True:
    print "CPU usage=%.2f%%" % (getCpuLoad()*100.0)
    cpuload = getCpuLoad()*100.0
    requrl = URL + '%.2f' % cpuload
    print requrl

    try:
    	response = urllib2.urlopen(requrl)
	print "RESPONSE : " + response.read()
	response.close()
    except Exception, e:
	print "Couldn't do it: %s" % e	

    #r = urllib2.urlopen('http://128.97.93.30:9000')
    time.sleep(1)
