# -*- coding: UTF-8 -*-
print('seeee')
from elasticsearch import helpers, Elasticsearch
import csv
import os
command = os.popen('ls -a')
print(command.read())
print(command.close())
L=["sudo cat /var/log/squid/cache.log.1 > /home/xibalpa/squidcachelog1.csv","sudo cat /var/log/squid/cache.log > /home/xibalpa/squidcachelog.csv","cat /var/log/kern.log.1 > /home/xibalpa/kernlog1.csv","sudo cat>
for i in L:
        command = os.popen(i)
        print(command.read())
        print(command.close())
 
#Télécharger pip et pandas

import pandas as pd

import datetime
import numpy as np
datetime = datetime.datetime.now().replace(microsecond=0)

df_syslog =pd.read_csv('/home/xibalpa/syslog.csv',header=None,error_bad_lines=False)
df_squidcachelog1 =pd.read_csv('/home/xibalpa/squidcachelog1.csv',header=None,error_bad_lines=False)
df_squidcachelog =pd.read_csv('/home/xibalpa/squidcachelog.csv',header=None,error_bad_lines=False)
df_casperlog =pd.read_csv('/home/xibalpa/casperlog.csv',header=None,error_bad_lines=False)
df_auth=pd.read_csv('/home/xibalpa/auth.csv',header=None,error_bad_lines=False)
df_kernlog1 =pd.read_csv('/home/xibalpa/kernlog1.csv',header=None,error_bad_lines=False)
df_kern =pd.read_csv('/home/xibalpa/kern.csv',header=None,error_bad_lines=False)
df_debug =pd.read_csv('/home/xibalpa/debug.csv',header=None,error_bad_lines=False)
df_alternative =pd.read_csv('/home/xibalpa/alternatives.csv',header=None,error_bad_lines=False)
df_dmesg =pd.read_csv('/home/xibalpa/dmesg.csv',header=None,error_bad_lines=False)
df_sysloginstaller =pd.read_csv('/home/xibalpa/sysloginstaller.csv',header=None,error_bad_lines=False)


#Niveau du log
def defi_level(message):
    z = re.findall(r'[C,c][R,r][I,i][T,t][I,i][C,c]\w+',message)
    if len(z)==0:
        z = re.findall(r'[C,c][R,r][I,i][T,t][I,i][C,c]',message)
    if len(z)==0:
        z = re.findall(r'[I,i][N,n][F,f][O,o]\w+',message)
    if len(z)==0:
        z = re.findall(r'[W,w][A,a]R,r][N,n]\w+',message)
    if len(z)==0:
        z = re.findall(r'[F,f][A,a][T,t][A,a][L,l]\w+',message)
    if len(z)==0:
        z = re.findall(r'[E,e][R,r][R,r]\w+',message)    
    if len(z)==0:
        z = re.findall(r'[I,i][N,n][F,f][O,o]',message)
    if len(z)==0:
        z = re.findall(r'[W,w][A,a]R,r][N,n]',message)
    if len(z)==0:
        z = re.findall(r'[F,f][A,a][T,t][A,a][L,l]',message)
    if len(z)==0:
        z = re.findall(r'[E,e][R,r][R,r]',message)
    if len(z)!=0:
        level = z[0]
    if len(z)==0:
        level = "trace"
    return(level)


#Formatage de la date
def formatagedate(date_d):
    MOIS = {"Jan" : "01","Feb" : "02","Mar" : "03","Apr" : "04","May" : "05","Jun" : "06",
            "Jul" : "07","Aug" : "08","Sep" : "09","Oct" : "10","Nov" : "11","Dec" : "12"}   
    liste_date = date_d.split()
    if len(liste_date)== 3:
        mois = MOIS.get("%s"%liste_date[0])
        date_d = "2021"+"/"+str(mois)+"/"+ liste_date[1]+" "+liste_date[2]
    return(date_d)

#Traitement alternative

df_alternative['raw']=df_alternative[0]
df_alternative['utilisateur']=df_alternative[0].str.extract('^(\S+)\s',expand=False).str.strip()
df_alternative[0]=df_alternative[0].str.replace('^(\S+)\s','')
df_alternative['date']=df_alternative[0].str.extract(r'([0-2][0-9][0-2][0-9]-[0-2][0-9]-[0-2][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])',expand=False).str.strip()
df_alternative[0]=df_alternative[0].str.replace(r'([0-2][0-9][0-2][0-9]-[0-2][0-9]-[0-2][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])','')
df_alternative['process']="no process"
df_alternative['message_log']=df_alternative[0]
df_alternative['source']="alternatives"
df_alternative['date_traitement']=datetime
df_alternative=df_alternative.drop([0],axis=1)



#Traitement dmesg

df_dmesg["raw"]=df_dmesg[0]
df_dmesg["utilisateur"]="no utilisateur"
df_dmesg["date"]="0000/00/00 00:00:00"
df_dmesg["process"]="no process"
df_dmesg["message_log"]=df_dmesg[0]
df_dmesg["source"]="dmesg"
df_dmesg['date_traitement']=datetime
df_dmesg=df_dmesg.drop([0],axis=1)
del df_dmesg[1]


#Traitement syslog

df_syslog['raw']=df_syslog[0]
df_syslog['date']=df_syslog[0].str.extract(r'([A-Z][a-z][a-z] [0-2][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])',expand=False).str.strip()
df_syslog[0]=df_syslog[0].str.replace(r'([A-Z][a-z][a-z] [0-2][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])','')
df_syslog['utilisateur']=df_syslog[0].str.extract('^( \S+)\s',expand=False).str.strip()
df_syslog[0]=df_syslog[0].str.replace('^( \S+)\s','')
df_syslog['process']=df_syslog[0].str.extract('^(\S+)\s',expand=False).str.strip()
df_syslog[0]=df_syslog[0].str.replace('^(\S+)\s','')
df_syslog['message_log']=df_syslog[0]
df_syslog['source']="syslog"
df_syslog['date_traitement']=datetime
df_syslog=df_syslog.drop([0],axis=1)


#Traitement sysloginstaller

df_sysloginstaller['raw']=df_sysloginstaller[0]
df_sysloginstaller['date']=df_sysloginstaller[0].str.extract(r'([A-Z][a-z][a-z] [0-2][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])',expand=False).str.strip()
df_sysloginstaller[0]=df_sysloginstaller[0].str.replace(r'([A-Z][a-z][a-z] [0-2][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])','')
df_sysloginstaller['utilisateur']=df_sysloginstaller[0].str.extract('^( \S+)\s',expand=False).str.strip()
df_sysloginstaller[0]=df_sysloginstaller[0].str.replace('^( \S+)\s','')
df_sysloginstaller['process']=df_sysloginstaller[0].str.extract('^(\S+)\s',expand=False).str.strip()
df_sysloginstaller[0]=df_sysloginstaller[0].str.replace('^(\S+)\s','')
df_sysloginstaller['message_log']=df_sysloginstaller[0]
df_sysloginstaller['source']="syslog"
df_sysloginstaller['date_traitement']=datetime
df_sysloginstaller=df_sysloginstaller.drop([0],axis=1)

#Traitement squdcachelog1

df_squidcachelog1['raw']=df_squidcachelog1[0]
df_squidcachelog1['date']=df_squidcachelog1[0].str.extract(r'([0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9])',expand=False).str.strip()
df_squidcachelog1[0]=df_squidcachelog1[0].str.replace(r'([0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9])','')
df_squidcachelog1['utilisateur']="no utilisateur"
df_squidcachelog1['process']=df_squidcachelog1[0].str.extract('^( \S+)\s',expand=False).str.strip()
df_squidcachelog1[0]=df_squidcachelog1[0].str.replace('^( \S+)\s','')
df_squidcachelog1['process']=df_squidcachelog1['process'].replace(np.nan,"no process")
df_squidcachelog1['date']=df_squidcachelog1['date'].replace(np.nan,"0000/00/00 00:00:00")
df_squidcachelog1['message_log']=df_squidcachelog1[0]
df_squidcachelog1['source']="squidcachelog1"
df_squidcachelog1['date_traitement']=datetime
df_squidcachelog1=df_squidcachelog1.drop([0],axis=1)

#Traitement squidcachelog

df_squidcachelog['raw']=df_squidcachelog[0]
df_squidcachelog['date']=df_squidcachelog[0].str.extract(r'([0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9])',expand=False).str.strip()
df_squidcachelog[0]=df_squidcachelog[0].str.replace(r'([0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9])','')
df_squidcachelog['utilisateur']="no utilisateur"
df_squidcachelog['process']=df_squidcachelog[0].str.extract('^( \S+)\s',expand=False).str.strip()
df_squidcachelog[0]=df_squidcachelog[0].str.replace('^( \S+)\s','')
df_squidcachelog['process']=df_squidcachelog['process'].replace(np.nan,"no process")
df_squidcachelog['message_log']=df_squidcachelog[0]
df_squidcachelog['source']="squidcache"
df_squidcachelog['date_traitement']=datetime
df_squidcachelog=df_squidcachelog.drop([0],axis=1)

#Traitement kernlog1

df_kernlog1['raw']=df_kernlog1[0]
df_kernlog1['date']=df_kernlog1[0].str.extract(r'([A-Z][a-z][a-z]  [0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])',expand=False).str.strip()
df_kernlog1[0]=df_kernlog1[0].str.replace(r'([A-Z][a-z][a-z]  [0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])','')
df_kernlog1['utilisateur']=df_kernlog1[0].str.extract('^( \S+)\s',expand=False).str.strip()
df_kernlog1[0]=df_kernlog1[0].str.replace('^( \S+)\s','')
df_kernlog1['process']=df_kernlog1[0].str.extract('^(\S+)\s',expand=False).str.strip()
df_kernlog1[0]=df_kernlog1[0].str.replace('^(\S+)\s','')
df_kernlog1['message_log']=df_kernlog1[0]
df_kernlog1['source']="kernlog1"
df_kernlog1['date_traitement']=datetime
df_kernlog1=df_kernlog1.drop([0],axis=1)

#Traitement kern

df_kern['raw']=df_kern[0]
df_kern['date']=df_kern[0].str.extract(r'([A-Z][a-z][a-z] [0-2][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])',expand=False).str.strip()
df_kern[0]=df_kern[0].str.replace(r'([A-Z][a-z][a-z] [0-2][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])','')
df_kern['utilisateur']=df_kern[0].str.extract('^( \S+)\s',expand=False).str.strip()
df_kern[0]=df_kern[0].str.replace('^( \S+)\s','')
df_kern['process']=df_kern[0].str.extract('^(\S+)\s',expand=False).str.strip()
df_kern[0]=df_kern[0].str.replace('^(\S+)\s','')
df_kern['message_log']=df_kern[0]
df_kern['source']="kern"
df_kern['date_traitement']=datetime
df_kern=df_kern.drop([0],axis=1)

#Traitement debug

df_debug['raw']=df_debug[0]
df_debug['date']="0000/00/00 00:00:00"
df_debug['utilisateur']="no utilisateur"
df_debug['process']=df_debug[0].str.extract(r'(\W[a-z][a-z][a-z][a-z][a-z][a-z][a-z][a-z]\W[0-9][0-9][0-9][0-9]\W\W)',expand=False).str.strip()
df_debug[0]=df_debug[0].str.replace(r'(\W[a-z][a-z][a-z][a-z][a-z][a-z][a-z][a-z]\W[0-9][0-9][0-9][0-9]\W\W)','')
df_debug['process']=df_debug['process'].replace(np.nan,"no process")
df_debug['message_log']=df_debug[0]
df_debug['source']="debug"
df_debug['date_traitement']=datetime
df_debug=df_debug.drop([0],axis=1)


#Traitement casperlog

df_casperlog ['raw']=df_casperlog [0]
df_casperlog ['date']=df_casperlog [0].str.extract(r'([A-Z][a-z][a-z] [0-2][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])',expand=False).str.strip()
df_casperlog[0]=df_casperlog[0].str.replace(r'([A-Z][a-z][a-z] [0-2][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])','')
df_casperlog ['date']= df_casperlog ['date'].fillna("0000/00/00 00:00:00")
df_casperlog ['utilisateur']=df_casperlog [0].str.extract('^( \S+):\s',expand=False).str.strip()
df_casperlog [0]=df_casperlog [0].str.replace('^( \S+):\s','')
df_casperlog ['utilisateur']= "no utilisateur"
df_casperlog ['process']=df_casperlog [0].str.extract('^(\S+):\s',expand=False).str.strip()
df_casperlog [0]=df_casperlog [0].str.replace('^(\S+):\s','')
df_casperlog ['process']= df_casperlog ['process'].fillna("no process")
df_casperlog ['message_log']=df_casperlog [0]
df_casperlog['source']="casperlog"
df_casperlog['date_traitement']=datetime
df_casperlog = df_casperlog.drop([0],axis=1)

#Traitement auth

df_auth['raw']=df_auth[0]
df_auth['date']=df_auth[0].str.extract(r'([A-Z][a-z][a-z] [0-2][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])',expand=False).str.strip()
df_auth[0]=df_auth[0].str.replace(r'([A-Z][a-z][a-z] [0-2][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])','')
df_auth['date']= df_auth['date'].fillna("0000/00/00 00:00:00")
df_auth['utilisateur']=df_auth[0].str.extract('^( \S+)\s',expand=False).str.strip()
df_auth['utilisateur']= df_auth['utilisateur'].fillna("no utilisateur")
df_auth[0]=df_auth[0].str.replace('^( \S+):\s','')
df_auth['process']=df_auth [0].str.extract('^(\S+)\s',expand=False).str.strip()
df_auth[0]=df_auth[0].str.replace('^(\S+):\s','')
df_auth['process']= df_auth['process'].fillna("no process")
df_auth['message_log']=df_auth[0]
df_auth['source']="auth"
df_auth['date_traitement']=datetime
df_auth= df_auth.drop([0],axis=1)

#Création du dataframe final

df=pd.concat([df_kern, df_debug,df_syslog,df_auth,df_casperlog,df_kernlog1,df_squidcachelog,df_squidcachelog1,df_dmesg,df_sysloginstaller], ignore_index=True)

#formatage date
df['date'] = df['date'].apply(formatagedate)
df['level']=df['raw'].apply(defi_level)

#Mise en format csv
df.to_csv('dataframe_multilog.csv',index=False)




