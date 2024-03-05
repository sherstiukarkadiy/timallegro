from datetime import datetime,timedelta
import time
from threading import Lock
from functools import wraps


def call_counter(func):
    def helper(*args,**kwargs):
        global __counter__
        __counter__ += 1
        return func(*args,**kwargs)
    return helper

def more_than_3000(*args,**kwargs):
    global __counter__, __starttime__,__hou_counter__
    lock = Lock()
    now = datetime.now()
    minute = timedelta(minutes=1)
    if __counter__ >= 3000 and (now-__starttime__) <= minute:
        print(__counter__, now-__starttime__)
        lock.acquire()
        time.sleep((__starttime__ + minute - now ).total_seconds())
        lock.release()
        __starttime__ = datetime.now()
        __hou_counter__ += __counter__
        __counter__ = 0
    elif (now-__starttime__) >= minute:
        __starttime__ = datetime.now()
        __hou_counter__ += __counter__
        __counter__ = 0
    else:
        pass
    
def more_than_10000(*args,**kwargs):
    global __counter__, __starttime__,__hou_counter__
    lock = Lock()
    if __counter__ >= 10000:
        lock.acquire()
        print("Sleeping")
        time.sleep(3600)
        print("Continue")
        __counter__ = 0
        lock.release()
    
def more_than_250000(*args,**kwargs):
    global __hou_starttime__,__hou_counter__
    now = datetime.now()
    hour = timedelta(hours=1)
    if __hou_counter__ >= 249000 and (now-__starttime__) <= hour:
        print(__counter__, now-__starttime__)
        time.sleep((__hou_starttime__ + hour - now ).total_seconds())
        __hou_starttime__ = datetime.now()
        __hou_counter__ = 0
    elif (now-__starttime__) >= hour:
        __hou_starttime__ = datetime.now()
        __hou_counter__ = 0
    else:
        pass
    
__counter__ = 0
__hou_counter__ = 0
__starttime__ =__hou_starttime__ = datetime.now()
