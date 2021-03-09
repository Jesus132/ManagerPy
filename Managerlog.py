import re
import sys
# import time
import tailer
import os, stat
import threading
from datetime import datetime
from elasticsearch import Elasticsearch
 
IndexElastic = 'managerlogs'
ElasticBody = dict()
ElasticBody={}
ElasticBody['timestamp']=''
ElasticBody['log_level']='Info'
ElasticBody['module']=''
ElasticBody['payload']=''
patronTimestamp = re.compile(r'\d{1,4}[-|:|_|/]\d{1,2}[-|:|_|/]\d{1,2}')
patronLogLevel = re.compile(r'\bInfo|info|INFORMACIÓN|información|adv|ADV|advertencia|ADVERTENCIA|Error|Err|error|ERROR\b')
def SearchLogLevel(line):
   listReplace = patronLogLevel.findall(line)
   if(listReplace == []):
       ElasticBody['log_level'] = 'Info'
       return line
   else:
       ElasticBody['log_level'] = listReplace[0]
       line = line.replace(listReplace[0],'')
       return line
def SearchLine(line):
   dataJson = dict()
   dataJson={}
   date=''
   h=''
   listTimeTamp = patronTimestamp.findall(line)
   #print(listTimeTamp, 'tiempo encontrado en expresion')
   if( listTimeTamp == []):
       dataJson['timestamp']=''
       dataJson['payload'] = line
       return dataJson
   line = SearchLogLevel(line) # Quitamos LogLevel de la linea
   for time in listTimeTamp: # Quitamos timetamp de la linea
       line = line.replace(time,'')
       time = time.replace('/','-')
       time = time.replace('_','-')
       if( len(time.split('-')[0]) == 4 and date == ''):
           date=time
       else:
           if( h == ''):
               h=time
   if(date == '' or h == ''):
       dataJson['timestamp'] = ''
       dataJson['payload'] = ''
       return dataJson
   dataJson['timestamp'] = date + 'T' + h.replace('_',':') + '.000Z'
   dataJson['timestamp'] = date + 'T' + h.replace('-',':') + '.000Z'
   dataJson['payload'] = line
   return dataJson
def SednElastic(pathFile, numberline, namemod, es):
   SendElasticBody = dict()
   SendElasticBody={}
   SendElasticBody['timestamp']=''
   SendElasticBody['log_level']='Info'
   SendElasticBody['module']=namemod
   SendElasticBody['path']=pathFile
   SendElasticBody['payload']=''
   listline = []
   n=-1
   f = open(pathFile, 'r')
   for i in f:
       n += 1
       if( n >= numberline-40):
           listline.append(i)
   f.close()
   for line in listline:
       auxSendElasticBody = SearchLine(line)
       if( SendElasticBody['timestamp'] == auxSendElasticBody['timestamp']):
           SendElasticBody['payload'] += auxSendElasticBody['payload']
       else:
           if( SendElasticBody['payload'] != '' and SendElasticBody['timestamp'] != '' ):
                   es.index(index=IndexElastic, body=SendElasticBody)
                   # print(SendElasticBody)
           SendElasticBody['timestamp'] = auxSendElasticBody['timestamp']
           SendElasticBody['payload'] = auxSendElasticBody['payload']
def ReadFile(pathFile, namemod, es):
   try:
       ElasticBody = dict()
       ElasticBody={}
       ElasticBody['timestamp']=''
       ElasticBody['log_level']=''
       ElasticBody['module']=namemod
       ElasticBody['path']=pathFile
       ElasticBody['payload']=''
       # ElasticBody['module'] = namemod
       numberline = -1
       auxnumberline = -1
       BD=['','','']
       LogTxt=''
       f = open(pathFile, 'r')
       for i in f:
           numberline += 1
       f.close()
       f = open('log.txt', 'r')
       for i in f:
           if(i.split(',')[0] == namemod):
               BD = i.split(', ')
       f.close()
       SednElastic(pathFile, numberline, namemod, es)
       auxnumberline = numberline
       for line in tailer.follow(open(pathFile)):
           auxElasticBody = SearchLine(line)
           numberline += 1
           if( BD[2] == auxElasticBody['timestamp']):
               if( auxElasticBody['timestamp'] == '' ):
                   auxElasticBody['timestamp'] = str(datetime.today()).replace(' ','T') + 'Z'
                   auxElasticBody['log_level']='info'
                   auxElasticBody['module']=namemod
                   auxElasticBody['path']=pathFile
                   auxElasticBody['payload']=line
                   es.index(index=IndexElastic, body=auxElasticBody)
                   #print(auxElasticBody)
               else:
                   ElasticBody['payload'] += auxElasticBody['payload']
           else:
               if( ElasticBody['payload'] != '' and ElasticBody['timestamp'] != ''):
                   es.index(index=IndexElastic, body=ElasticBody)
                   # print(ElasticBody)
               ElasticBody['timestamp'] = auxElasticBody['timestamp']
               ElasticBody['payload'] = auxElasticBody['payload']
               if( BD[2] != ''):
                   log = open('log.txt', 'r+')
                   LogTxt = log.read().replace(namemod + ', '+ str(auxnumberline) + ', '+ BD[2], namemod + ', '+ str(numberline) + ', '+ ElasticBody['timestamp'])
                   log.close()
                   auxnumberline = numberline
                   BD[2] = ElasticBody['timestamp']
                   log = open('log.txt', 'w')
                   log.writelines(LogTxt)
                   log.close()
               else:
                   log = open('log.txt', 'a')
                   log.write(namemod + ', '+ str(numberline) + ', '+ ElasticBody['timestamp'])
                   log.write('\n')
                   log.close()
                   BD[2] = ElasticBody['timestamp']
   except:
       ELK={}
       ELK['timestamp'] = str(datetime.today()).replace(' ','T') + 'Z'
       ELK['log_level']='err'
       ELK['module']=namemod
       ELK['path']=pathFile
       ELK['payload:']='PermissionError: [Errno 13] Permission denied: ' + pathFile
       es.index(index=IndexElastic, body=ELK)
def SearchLogs(ext, path, es):
   conthilos=0
   # result = []
   dot=False
   NameFile=''
   extFile=''
   ind=0
   for root, dirs, files in os.walk(path):
       for file in files:
           for f in file:
               if (f == '.' or dot):
                   dot=True
                   extFile += f
               else:
                   NameFile += f
           dot=False
           if (extFile == ext):
               pathLog = os.path.join(root, file)
               timeinfile = patronTimestamp.findall(NameFile)
               if( timeinfile == [] ):
                   ind += 1
                   # print(NameFile, pathLog, '<---')  input()
                   threading.Thread(name=NameFile, target=ReadFile, args=(pathLog,NameFile,es,)).start()
                   print(ind, NameFile, pathLog)
                  
                  
               else:
                   print(NameFile, pathLog)
                   # input('-------------------')
           NameFile=''
           extFile=''
if __name__ == '__main__':
   SearchLogs('.log', '/', Elasticsearch())
   Elasticsearch().indices.put_settings(index=["*"], body={"number_of_replicas": 0})
   #time.sleep(15)var/log
 