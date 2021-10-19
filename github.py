
!cat system.journal  > /home/xibalpa/systemjournal.csv
!cat  > /home/xibalpa/vsftpd2.csv
!cat vsftpd.log.1 > /home/xibalpa/vsftpd1.csv
!cat cache.log.1 > /home/xibalpa/squidcachelog1.csv
!cat cache.log > /home/xibalpa/squidcachelog.csv
!cat kern.log.1 > /home/xibalpa/kernlog1.csv
!cat syslog > /home/xibalpa/sysloginstaller.csv
!cat debug > /home/xibalpa/debug.csv
!cat casper.log > /home/xibalpa/casperlog.csv
!cat access_log > /home/xibalpa/cupsaccesslog.csv
!cat auth.log.1 > /home/xibalpa/authlog1.csv
!cat alternatives.log > /home/xibalpa/alternatives.csv
!cat auth.log > /home/xibalpa/auth.csv
!cat access.log.1 > /home/xibalpa/accesslog1.csv
!cat kern.log > /home/xibalpa/kern.csv
!cat dmesg > /home/xibalpa/dmesg.csv





spark-shell --master spark://master:7077

    
#Télécharger pip et pandas
!apt install pip 
!pip install pandas

#Créer fichier dataframe
import pandas as pd
import re
import datetime
import numpy as np
datetime = datetime.datetime.now().replace(microsecond=0)



df_alternative =pd.read_csv(r'C:\Users\axelg\Downloads\youpi\alternatives.csv',header=None,error_bad_lines=False)
df_dmesg =pd.read_csv(r'C:\Users\axelg\Downloads\youpi\dmesg.csv',header=None,error_bad_lines=False)
df_syslog =pd.read_csv(r'C:\Users\natha\Documents\Projet_log\syslog.csv',header=None,error_bad_lines=False)
df_squidcachelog1 =pd.read_csv(r'C:\Users\natha\Documents\Projet_log\squidcachelog1.csv',header=None,error_bad_lines=False)
df_squidcachelog =pd.read_csv(r'C:\Users\natha\Documents\Projet_log\squidcachelog.csv',header=None,error_bad_lines=False)
df_casperlog =pd.read_csv(r'casperlog.csv',header=None,error_bad_lines=False)

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



#Traitement dmesg

df_dmesg["raw"]=df_dmesg[0]
df_dmesg["utilisateur"]="no utilisateur"
df_dmesg["date"]="0000 00 00 00:00:00"
df_dmesg["process"]="no process"
df_dmesg["message_log"]=df_dmesg[0]
df_dmesg["source"]="dmesg"
df_dmesg['date_traitement']=datetime_object
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
df_syslog.drop([0],axis=1)

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
df_squidcachelog1.drop([0],axis=1)

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
df_squidcachelog.drop([0],axis=1)

#Traitement kernlog1

#Traitement casperlog

df_casperlog ['raw']=df_casperlog [0]
df_casperlog ['date']=df_casperlog [0].str.extract(r'([A-Z][a-z][a-z] [0-2][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])',expand=False).str.strip()
df[0]=df[0].str.replace(r'([A-Z][a-z][a-z] [0-2][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])','')
df_casperlog ['date']= df_casperlog ['date'].fillna("0000/00/00 00:00:00")
df_casperlog ['utilisateur']=df_casperlog [0].str.extract('^( \S+):\s',expand=False).str.strip()
df_casperlog [0]=df_casperlog [0].str.replace('^( \S+):\s','')
df_casperlog ['utilisateur']= "no utilisateur"
df_casperlog ['process']=df_casperlog [0].str.extract('^(\S+):\s',expand=False).str.strip()
df_casperlog [0]=df_casperlog [0].str.replace('^(\S+):\s','')
df_casperlog ['process']= df_casperlog ['process'].fillna("no process")
df_casperlog ['message_log']=df_casperlog [0]
df_casperlog = df_casperlog .drop([0],axis=1)
