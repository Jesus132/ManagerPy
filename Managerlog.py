import re
import sys
import json
import time
from datetime import datetime
import os, stat
import threading
from fluent import sender
from fluent import event

timestamp=True
changetimestamp = False
nametimestamp = False
module=True
LogLevel=True
Payload=True

contmodule=False

Split=['\x00 ','\x00']

auxtimestamp=[]
listReplaceP=[]
dataJson = dict()
dataJson['Data']={}
dataJson['Data']['timestamp']=''
dataJson['Data']['log_level']=''
dataJson['Data']['module']=''
dataJson['Data']['payload']=''

patronTimestamp = re.compile(r'\d{1,4}[-|:]\d{1,2}[-|:]\d{1,2}')
patronLogLevel = re.compile(r'\bInfo|info|INFORMACIÓN|información|adv|ADV|advertencia|ADVERTENCIA|Error|Err|ERROR\b')

def SearchLogLevel(line, Splits):
    listReplace = patronLogLevel.findall(line)
    if(listReplace == []):
        dataJson['Data']['log_level'] = 'Info'
        return line
    else:
        dataJson['Data']['log_level'] = listReplace[0]
        line = line.replace(listReplace[0],'')
        return line

def SearchTimestamp(line, Splits, namemod, Fluentd):
    global timestamp, LogLevel, Payload
    t = ''
    TimeTampTake = line
    listReplace = patronTimestamp.findall(line)
    for lr in listReplace:
        t += lr + ' '
        TimeTampTake = TimeTampTake.replace(lr,'')
    tim = listReplace[0].replace('-','/') + ' ' + listReplace[1]
    if(dataJson['Data']['timestamp'] == tim):
        LogLevel = False
    else:
        LogLevel = True
        Fluentd.emit(namemod, dataJson)
        # print('Send: ', dataJson)
        dataJson['Data']['timestamp'] = tim
    return TimeTampTake

        

def Search(line, Splits, namemod, Fluentd):
    global timestamp, LogLevel, Payload
    if(timestamp):
        line = SearchTimestamp(line, Splits, namemod, Fluentd)
    if(LogLevel):
        line = SearchLogLevel(line, Splits)
        dataJson['Data']['payload'] = line
    else:
        dataJson['Data']['payload'] += line

def CountF(file):
    count = 0
    for i in file:
        count += 1
    return count

def ReadFile(nameFile, namemod):
    listline = []
    numberline = None
    lastline = True
    # listlnumberline = []
    dataJson['Data']['module'] = namemod
    # FluentSender(tag, self.host, port, buffer_max, timeout)
    Fluentd = sender.FluentSender('testpy', host='localhost', port=24224)
    try:
        f = open('log.txt', 'r')
        for i in f:
            if(i.split(',')[0] == namemod):
                numberline = i.split(',')[1]
        f = open(nameFile, 'r')
        countline = CountF(f)
        f.close()
        f = open(nameFile, 'r')
        while True:
            line = ''
            while len(line) == 0 or line[-1] != '\n':
                tail = f.readline()
                if tail == '':
                    if(lastline):
                        Fluentd.emit(namemod, dataJson)
                        # print('Send: ', dataJson)
                        lastline = False
                    time.sleep(0.1)
                    continue
                line += tail
                listline.append(tail)
                lastline= True
                if(countline >= 100):
                    if(len(listline) >= countline - 5):
                        Search(tail, Split, namemod, Fluentd)
                else:
                    Search(tail, Split, namemod, Fluentd)
    except:
        print('PermissionError: [Errno 13] Permission denied')

def find_all(name, path):
    result = []
    dot=False
    moduleDat=''
    nameFile=''
    for root, dirs, files in os.walk(path):
        for file in files:
            for f in file:
                if (f == '.' or dot):
                    dot=True
                    nameFile += f
                else:
                    moduleDat += f
            dot=False
            if (nameFile == name):
                pathLog = os.path.join(root, file)
                print(pathLog, moduleDat)
                input()
                # ReadFile('Setup.log', moduleDat)
                threading.Thread(name=moduleDat, target=ReadFile, args=(pathLog,moduleDat,)).start()
            moduleDat=''
            nameFile=''

if __name__ == '__main__':
    find_all('.log', 'C:/')
