# -*- coding: UTF-8 -*-
import warnings
#On bloque tout les warning de python qui pourrait s'afficher
warnings.filterwarnings("ignore")
from elasticsearch import helpers, Elasticsearch
import csv
import re
import pickle
import os

#Permet de récupérer les logs et dans les sauvegarder en fichier csv et aussi d'installer les bibliothèques pythons nécessaires
L=["sudo cat /var/log/vsftpd.log > /home/xibalpa/vsftpd.csv","sudo cat /var/log/apache2/access.log > /home/xibalpa/accesslog.csv","sudo cat /var/log/vsftpd.log > /home/xibalpa/vsftpdlog.csv","sudo cat /var/log/squid/cache.log.1 > /home/xibalpa/squidcachelog1.csv","sudo cat /var/log/squid/cache.log > /home/xibalpa/squidcachelog.csv","cat /var/log/kern.log.1 > /home/xibalpa/kernlog1.csv","sudo cat /var/log/installer/syslog > /home/xibalpa/sysloginstaller.csv","sudo cat /var/log/installer/debug > /home/xibalpa/debug.csv","sudo cat /var/log/installer/casper.log > /home/xibalpa/casperlog.csv","cat /var/log/alternatives.log.1 > /home/xibalpa/alternatives.csv","cat /var/log/auth.log > /home/xibalpa/auth.csv","cat /var/log/kern.log > /home/xibalpa/kern.csv","cat /var/log/dmesg > /home/xibalpa/dmesg.csv","cat /var/log/syslog > /home/xibalpa/syslog.csv","apt install pip","pip install pandas","pip install datetime","pip install numpy"]
for i in L:
	command = os.popen(i)
	print(command.read())
	print(command.close())

#Télécharger pip et pandas
import pandas as pd

from datetime import datetime, timedelta
import numpy as np
datetime = datetime.now().replace(microsecond=0)

#Les différents dataframes de tous les logs transformés en csv
#On utilise les try except pour gérer les dataframes vide, du aux logs vides
#Si le dataframe n'est pas vide on l'ajoute à la liste L_dataframe qui sera utilisé pour concaténer les dataframes
L_dataframe=[]
try:
	df_syslog =pd.read_csv('/home/xibalpa/syslog.csv',header=None,error_bad_lines=False)
	L_dataframe.append(df_syslog)
except Exception as e:
	print(e)	
try:	
	df_squidcachelog1 =pd.read_csv('/home/xibalpa/squidcachelog1.csv',header=None,error_bad_lines=False)
	L_dataframe.append(df_squidcachelog1)
except Exception as e:
	print(e)	
try:	
	df_squidcachelog =pd.read_csv('/home/xibalpa/squidcachelog.csv',header=None,error_bad_lines=False)
	L_dataframe.append(df_squidcachelog)
except Exception as e:
	print(e)	
try:	
	df_casperlog =pd.read_csv('/home/xibalpa/casperlog.csv',header=None,error_bad_lines=False)
	L_dataframe.append(df_casperlog)
except Exception as e:
	print(e)	
try:	
	df_auth=pd.read_csv('/home/xibalpa/auth.csv',header=None,error_bad_lines=False)
	L_dataframe.append(df_auth)
except Exception as e:
	print(e)	
try:	
	df_kernlog1 =pd.read_csv('/home/xibalpa/kernlog1.csv',header=None,error_bad_lines=False)
	L_dataframe.append(df_kernlog1)
except Exception as e:
	print(e)	
try:	
	df_kern =pd.read_csv('/home/xibalpa/kern.csv',header=None,error_bad_lines=False)
	L_dataframe.append(df_kern)
except Exception as e:
	print(e)	
try:	
	df_debug =pd.read_csv('/home/xibalpa/debug.csv',header=None,error_bad_lines=False)
	L_dataframe.append(df_debug)
except Exception as e:
	print(e)	
try:	
	df_alternative =pd.read_csv('/home/xibalpa/alternatives.csv',header=None,error_bad_lines=False)
	L_dataframe.append(df_alternative)
except Exception as e:
	print(e)	
try:	
	df_dmesg =pd.read_csv('/home/xibalpa/dmesg.csv',header=None,error_bad_lines=False)
	L_dataframe.append(df_dmesg)
except Exception as e:
	print(e)	
try:	
	df_sysloginstaller =pd.read_csv('/home/xibalpa/sysloginstaller.csv',header=None,error_bad_lines=False)
	L_dataframe.append(df_sysloginstaller)
except Exception as e:
	print(e)	
try:	
	df_vsftpdf =pd.read_csv('/home/xibalpa/vsftpd.csv',header=None,error_bad_lines=False)
	L_dataframe.append(df_vsftpdf)
except Exception as e:
	print(e)	
try:	
	df_accesslog =pd.read_csv('/home/xibalpa/accesslog.csv',header=None,error_bad_lines=False)
	L_dataframe.append(df_accesslog)
except Exception as e:
	print(e)	


#Niveau du log, regex permettant de chercher la criticité du log, si le log contient critic, info, warn, warning, err, error
#Si un des logs contient un de ces mots il est ajouté à la colonne level
def defi_level(message):
    z = re.findall(r'[C,c][R,r][I,i][T,t][I,i][C,c]\w+',message)
    a = 0
    if len(z)==0:
        z = re.findall(r'[C,c][R,r][I,i][T,t][I,i][C,c]',message)
    if len(z)==0:
        z = re.findall(r'[I,i][N,n][F,f][O,o]\w+',message)
    if len(z)==0:
        z = re.findall(r'[W,w][A,a]R,r][N,n]\w+',message)
    if len(z)==0:
        z = re.findall(r'[F,f][A,a][T,t][A,a][L,l]\w+',message)
    if len(z)==0:
        z = re.findall(r'([E,e][R,r][R,r].? )|([E,e][R,r][R,r][O,o]\w+)',message)
        if len(z)!= 0:
           a = 1
           level = "Err"
    if len(z)==0:
        z = re.findall(r'[I,i][N,n][F,f][O,o]',message)
    if len(z)==0:
        z = re.findall(r'[W,w][A,a]R,r][N,n]',message)
    if len(z)==0:
        z = re.findall(r'[F,f][A,a][T,t][A,a][L,l]',message)
    if len(z)==0:
        z = re.findall(r'[E,e][R,r][R,r]',message)
    if len(z)!=0 and a == 0:
        level = z[0]
    if len(z)==0:
        level = "trace"
    return(level)

#Formatage de la date pour avoir le format date adapté dans ES et avoir une unicité de format pour la colonne date
def formatagedate(date_d):
    MOIS = {"Jan" : "01","Feb" : "02","Mar" : "03","Apr" : "04","May" : "05","Jun" : "06",
            "Jul" : "07","Aug" : "08","Sep" : "09","Oct" : "10","Nov" : "11","Dec" : "12"}
    date_d = str(date_d)
    liste_date = date_d.split()
    if len(liste_date)== 3:
        mois = MOIS.get("%s"%liste_date[0])
        date_d = "2021"+"/"+str(mois)+"/"+ liste_date[1]+" "+liste_date[2]
    return(date_d)



#Traitement des logs alternative

#Les traitements des différents logs sont en majeurs parties identiques:
#On traite le log qui sera dans la colonne df_nomDuFichierLog[0], depuis cette colonne on va extraire les différentes données
#Une fois les données extraites on supprime ces données de la colonne[0] et on passe aux données suivantes à extraire
#Voici un exemple sur le fichier log Alternative
#On fait un try except sur tous les traitements 

#On garde une colonne raw ou le log est gardé sous sa forme première
df_alternative['raw']=df_alternative[0]
#Regex permettant de d'extraire l'utilisateur dans la colonne['utilisateur'], il s'aretter à la premiere tabulation
df_alternative['utilisateur']=df_alternative[0].str.extract('^(\S+)\s',expand=False).str.strip()
#On supprime l' utilisateur que l'on a extraie dans la colonne[0]
df_alternative[0]=df_alternative[0].str.replace('^(\S+)\s','')
#Regex permettant de d'extraire la date dans la colonne['date']
df_alternative['date']=df_alternative[0].str.extract(r'([0-2][0-9][0-2][0-9]-[0-2][0-9]-[0-2][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])',expand=False).str.strip()
#On supprime la date que l'on a extraie juste avant dans la colonne[0]
df_alternative[0]=df_alternative[0].str.replace(r'([0-2][0-9][0-2][0-9]-[0-2][0-9]-[0-2][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])','')
#Ici le log ne contient pas de process
df_alternative['process']="no process"
#Dans la colonne[0] il ne reste plus que le message du log
df_alternative['message_log']=df_alternative[0]
#On indique la source de ce log: "alternatives"
df_alternative['source']="alternatives"
#cette colonnes correspond à la date de traitement de ce log
df_alternative['date_traitement']=datetime
#On supprime la colonne[0] qui nous ai plus d'aucune utilité
df_alternative=df_alternative.drop([0],axis=1)

#Les subtilitées qui changent selon les fichiers logs sont les regex ou bien alors une données d'une colonne qui n'existe pas, le procédé en général reste le même


#Traitement dmesg
try:
	df_dmesg["raw"]=df_dmesg[0]
	df_dmesg["utilisateur"]="no utilisateur"
	df_dmesg["date"]="2030/12/31 23:58:59"
	df_dmesg["process"]="no process"
	df_dmesg["message_log"]=df_dmesg[0]
	df_dmesg["source"]="dmesg"
	df_dmesg['date_traitement']=datetime
	df_dmesg=df_dmesg.drop([0],axis=1)
	del df_dmesg[1]
except Exception as e:
	print(e)


#Traitement syslog
try:
	df_syslog['raw']=df_syslog[0]
	df_syslog['date']=df_syslog[0].str.extract(r'([A-Z][a-z][a-z] \d+ [0-2][0-9]:[0-5][0-9]:[0-9][0-9])',expand=False).str.strip()
	df_syslog[0]=df_syslog[0].str.replace(r'([A-Z][a-z][a-z] \d+ [0-2][0-9]:[0-5][0-9]:[0-9][0-9])','')
	df_syslog['utilisateur']=df_syslog[0].str.extract('^( \S+)\s',expand=False).str.strip()
	df_syslog[0]=df_syslog[0].str.replace('^( \S+)\s','')
	df_syslog['process']=df_syslog[0].str.extract('^(\S+)\s',expand=False).str.strip()
	df_syslog[0]=df_syslog[0].str.replace('^(\S+)\s','')
	df_syslog['message_log']=df_syslog[0]
	df_syslog['source']="syslog"
	df_syslog['date_traitement']=datetime
	df_syslog=df_syslog.drop([0],axis=1)
except Exception as e:
	print(e)


#Traitement sysloginstaller
try:
	df_sysloginstaller['raw']=df_sysloginstaller[0]
	df_sysloginstaller['date']=df_sysloginstaller[0].str.extract(r'([A-Z][a-z][a-z] \d+ [0-2][0-9]:[0-5][0-9]:[0-9][0-9])',expand=False).str.strip()
	df_sysloginstaller[0]=df_sysloginstaller[0].str.replace(r'([A-Z][a-z][a-z] \d+ [0-2][0-9]:[0-5][0-9]:[0-9][0-9])','')
	df_sysloginstaller['utilisateur']=df_sysloginstaller[0].str.extract('^( \S+)\s',expand=False).str.strip()
	df_sysloginstaller[0]=df_sysloginstaller[0].str.replace('^( \S+)\s','')
	df_sysloginstaller['process']=df_sysloginstaller[0].str.extract('^(\S+)\s',expand=False).str.strip()
	df_sysloginstaller[0]=df_sysloginstaller[0].str.replace('^(\S+)\s','')
	df_sysloginstaller['message_log']=df_sysloginstaller[0]
	df_sysloginstaller['source']="syslog"
	df_sysloginstaller['date_traitement']=datetime
	df_sysloginstaller=df_sysloginstaller.drop([0],axis=1)
except Exception as e:
	print(e)

#Traitement squdcachelog1
try:
	df_squidcachelog1['raw']=df_squidcachelog1[0]
	df_squidcachelog1['date']=df_squidcachelog1[0].str.extract(r'([0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9])',expand=False).str.strip()
	df_squidcachelog1[0]=df_squidcachelog1[0].str.replace(r'([0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9])','')
	df_squidcachelog1['utilisateur']="no utilisateur"
	df_squidcachelog1['process']=df_squidcachelog1[0].str.extract('^( \S+)\s',expand=False).str.strip()
	df_squidcachelog1[0]=df_squidcachelog1[0].str.replace('^( \S+)\s','')
	df_squidcachelog1['process']=df_squidcachelog1['process'].replace(np.nan,"no process")
	df_squidcachelog1['date']=df_squidcachelog1['date'].replace(np.nan,"2030/12/31 23:58:59")
	df_squidcachelog1['message_log']=df_squidcachelog1[0]
	df_squidcachelog1['source']="squidcachelog1"
	df_squidcachelog1['date_traitement']=datetime
	df_squidcachelog1=df_squidcachelog1.drop([0],axis=1)
except Exception as e:
	print(e)

#Traitement squidcachelog
try:
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
except Exception as e:
	print(e)

#Traitement kernlog1
try:
	df_kernlog1['raw']=df_kernlog1[0]
	df_kernlog1['date']=df_kernlog1[0].str.extract(r'([A-Z][a-z][a-z] \d+ [0-2][0-9]:[0-5][0-9]:[0-9][0-9])',expand=False).str.strip()
	df_kernlog1[0]=df_kernlog1[0].str.replace(r'([A-Z][a-z][a-z] \d+ [0-2][0-9]:[0-5][0-9]:[0-9][0-9])','')
	df_kernlog1['utilisateur']=df_kernlog1[0].str.extract('^( \S+)\s',expand=False).str.strip()
	df_kernlog1[0]=df_kernlog1[0].str.replace('^( \S+)\s','')
	df_kernlog1['process']=df_kernlog1[0].str.extract('^(\S+)\s',expand=False).str.strip()
	df_kernlog1[0]=df_kernlog1[0].str.replace('^(\S+)\s','')
	df_kernlog1['message_log']=df_kernlog1[0]
	df_kernlog1['source']="kernlog1"
	df_kernlog1['date_traitement']=datetime
	df_kernlog1=df_kernlog1.drop([0],axis=1)
except Exception as e:
	print(e)

#Traitement kern
try:
	df_kern['raw']=df_kern[0]
	df_kern['date']=df_kern[0].str.extract(r'([A-Z][a-z][a-z] \d+ [0-2][0-9]:[0-5][0-9]:[0-9][0-9])',expand=False).str.strip()
	df_kern[0]=df_kern[0].str.replace(r'([A-Z][a-z][a-z] \d+ [0-2][0-9]:[0-5][0-9]:[0-9][0-9])','')
	df_kern['utilisateur']=df_kern[0].str.extract('^( \S+)\s',expand=False).str.strip()
	df_kern[0]=df_kern[0].str.replace('^( \S+)\s','')
	df_kern['process']=df_kern[0].str.extract('^(\S+)\s',expand=False).str.strip()
	df_kern[0]=df_kern[0].str.replace('^(\S+)\s','')
	df_kern['message_log']=df_kern[0]
	df_kern['source']="kern"
	df_kern['date_traitement']=datetime
	df_kern=df_kern.drop([0],axis=1)
except Exception as e:
	print(e)

#Traitement debug
try:
	df_debug['raw']=df_debug[0]
	df_debug['date']="2030/12/31 23:58:59"
	df_debug['utilisateur']="no utilisateur"
	df_debug['process']=df_debug[0].str.extract(r'(\W[a-z][a-z][a-z][a-z][a-z][a-z][a-z][a-z]\W[0-9][0-9][0-9][0-9]\W\W)',expand=False).str.strip()
	df_debug[0]=df_debug[0].str.replace(r'(\W[a-z][a-z][a-z][a-z][a-z][a-z][a-z][a-z]\W[0-9][0-9][0-9][0-9]\W\W)','')
	df_debug['process']=df_debug['process'].replace(np.nan,"no process")
	df_debug['message_log']=df_debug[0]
	df_debug['source']="debug"
	df_debug['date_traitement']=datetime
	df_debug=df_debug.drop([0],axis=1)
except Exception as e:
	print(e)

#Traitement casperlog
try:
	df_casperlog ['raw']=df_casperlog [0]
	df_casperlog ['date']=df_casperlog [0].str.extract(r'([A-Z][a-z][a-z] \d+ [0-2][0-9]:[0-5][0-9]:[0-9][0-9])',expand=False).str.strip()
	df_casperlog[0]=df_casperlog[0].str.replace(r'([A-Z][a-z][a-z] \d+ [0-2][0-9]:[0-5][0-9]:[0-9][0-9])','')
	df_casperlog ['date']= df_casperlog ['date'].fillna("2030/12/31 23:58:59")
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
except Exception as e:
	print(e)

#Traitement auth
try:
	df_auth['raw']=df_auth[0]
	df_auth['date']=df_auth[0].str.extract(r'([A-Z][a-z][a-z] \d+ [0-2][0-9]:[0-5][0-9]:[0-9][0-9])',expand=False).str.strip()
	df_auth[0]=df_auth[0].str.replace(r'([A-Z][a-z][a-z] \d+ [0-2][0-9]:[0-5][0-9]:[0-9][0-9])','')
	df_auth['date']= df_auth['date'].fillna("2030/12/31 23:58:59")
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
except Exception as e:
	print(e)


#Traitement vsftpdf
try:
	df_vsftpdf['raw']=df_vsftpdf[0]
	df_vsftpdf['date']=df_vsftpdf[0].str.extract(r'([A-Z][a-z][a-z] [0-2][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])',expand=False).str.strip()
	df_vsftpdf[0]=df_vsftpdf[0].str.replace(r'([A-Z][a-z][a-z] [0-2][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])','')
	df_vsftpdf['utilisateur']="unknown"
	df_vsftpdf[0]=df_vsftpdf[0].str.replace(r'([A-Z][a-z][a-z] )','')
	df_vsftpdf[0]=df_vsftpdf[0].str.replace(r'([0-9][0-9][0-9][0-9] )','')
	df_vsftpdf['process']=df_vsftpdf[0].str.extract(r'(\[[a-z][a-z][a-z] [0-9][0-9][0-9][0-9][0-9]])',expand=False).str.strip()
	df_vsftpdf[0]=df_vsftpdf[0].str.replace(r'\[[a-z][a-z][a-z] [0-9][0-9][0-9][0-9][0-9]]','')
	df_vsftpdf['message_log']=df_vsftpdf[0]
	df_vsftpdf['source']="vsftpdf"
	df_vsftpdf['date_traitement']=datetime
	df_vsftpdf=df_vsftpdf.drop([0],axis=1)
except Exception as e:
	print(e)

#Traitement accesslog

try:
	df_accesslog['raw']=df_accesslog[0]

	def datepouraccesslog(liste):
	    testz= re.findall(r"[0-3][0-9]/[A-Z][a-z][a-z]/[0-2][0-9][0-2][0-9]:[0-2][0-9]:[0-5][0-9]:[0-9][0-9]",liste)
	    testlist = testz[0].split("/")
	    testlistb = testlist[2].split(":")
	    testlist = testlist[:2] + testlistb

	    MOIS = {"Jan" : "01","Feb" : "02","Mar" : "03","Apr" : "04","May" : "05","Jun" : "06",
		    "Jul" : "07","Aug" : "08","Sep" : "09","Oct" : "10","Nov" : "11","Dec" : "12"}
	    mois = MOIS.get("%s"%testlist[1])
	    datefinal = testlist[2] +"/"+ str(mois)+"/"+ testlist[0]+" "+":".join(testlistb[1:])
	    return(datefinal)
	df_accesslog['date'] = df_accesslog['raw'].apply(datepouraccesslog)
	df_accesslog['raw']=df_accesslog[0]
	df_accesslog['process']="no process"
	df_accesslog["utilisateur"]="no utilisateur"
	df_accesslog['message_log']=df_accesslog[0]
	df_accesslog['source']="accesslog"
	df_accesslog['date_traitement']=datetime
	df_accesslog=df_accesslog.drop([0],axis=1)
except Exception as e:
	print(e)



#Création du dataframe final


#On concaténe tous les dataframes traités en un seul

df=L_dataframe[0]
for i in L_dataframe[1:]:

	df=pd.concat([df,i], ignore_index=True) 
	
print(df)

#type attaque
def typeattaque(regex_attaque,string_a_tester):
    prog = re.compile(regex_attaque)
    return int((bool(prog.search(string_a_tester)))*1)

#formatage date
#date vide remplacer par une date générique
df['date'] = df['date'].replace('','2030/12/31 23:58:59')
#NaN remplacer par la générique
df['date']= df['date'].fillna("2030/12/31 23:58:59")
#On applique la foncion formatagedate sur tout le dataframe pour avoir le bon format de date
df['date'] = df['date'].apply(formatagedate)
#On applique la foncion defi_level sur tout le dataframe pour avoir la criticité du log
df['level']=df['raw'].apply(defi_level)
df[df['date'].astype(str).str.match(r'([0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9] [0-2][0-9]:[0-5][0-9]:[0-9][0-9])')]
#On essaie de récupérer pour chaque log du dataframe concaténé des messages logs qui sont suceptibles d'être une attaque
#Cela nous sera utile pour la classification des attaques

df['sql']= df.apply(lambda x: typeattaque("[sS][qQ][Ll]|[uU][nN][iI][Oo][nN]|[sS][eE][lL][eE][cC][tT]|[dD][rR][oO][pP]|[cC][oO][uU][nN][tT]|[iI][nN][sS][eE][rR][tT]|[uU][pP][dD][aA][tT][eE]",x['raw']), axis=1)
df['nmap']= df.apply(lambda x: typeattaque("[nN][mM][aA][pP]",x['raw']), axis=1)
df['ssh']= df.apply(lambda x: typeattaque("[sS][sS][hH]",x['raw']), axis=1)
df['ftp']= df.apply(lambda x: typeattaque("[fF][tT][pP]",x['raw']), axis=1)
df['http']= df.apply(lambda x: typeattaque("[hH][tT][tT][pP]",x['raw']), axis=1)
df['Nikto']= df.apply(lambda x: typeattaque("[nN][iI][kK][tT][oO]",x['raw']), axis=1)
df['password']= df.apply(lambda x: typeattaque("[pP][aA][sS][sS][wW][oO][rR][dD]",x['raw']), axis=1)
df['ip']=df['raw'].str.extract(r'\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\b',expand=False).str.strip()

#On récupère l'ip local de la machine hôte
iplocal = os.popen("hostname -I | awk '{print $1}'")
ipLocalStr = str(iplocal.read())
print(ipLocalStr)
#On insére l'adresse ip locale de la machine hote dans la colonne ip
df['ip'] = df['ip'].replace('', ipLocalStr, inplace=True)
df['ip'] = df['ip'].replace('None', ipLocalStr, inplace=True)


#On gère les Nan en les remplaçant par l'adresse ip locale de la machine hote dans la colonne ip
df['ip']= df['ip'].fillna(ipLocalStr,inplace=True)


df['ip'] = df['ip'].replace(to_replace=[None],  value= ipLocalStr, inplace=True)

#on compare l'ip récupéré et l'ip de notre machine.
df['ipp']=(df['ip'].str.contains(ipLocalStr)==False)*1
df = df[ df.date != '2030/12/31 23:58:59']


#Mise en format csv du dataframe concaténé
try:
	with open('/home/xibalpa/Documents/dataframe_multilogdeuxpc.csv','a') as f:
		df.to_csv(f,index=False,header=True)

	#Transfert du csv créé à ElasticSearch
	es = Elasticsearch(['localhost'],port= 9200)

	with open('/home/xibalpa/Documents/dataframe_multilogdeuxpc.csv') as f:
	    reader = csv.DictReader(f)
	    helpers.bulk(es, reader, index='testdixseptvingttrois')

	print('dfkernlog1=',len(df_kernlog1.index))
	print('dfsyslog=',len(df_syslog.index))
	print('dfmesg=',len(df_dmesg.index))
	print('dfconcat=',len(df.index))
except Exception as e:
	print(e)




############ Partie Détection / Machine Learning  ########


#dernier cleaning des datas avant passage dans le modèle

df['date'] =  pd.to_datetime(df['date'])
df.set_index('date',inplace=True)             # ligne à enlever si pas de compact   **********
#df=df.resample('2s').mean()               # ligne à enlever si pas de compact       ************

df = df[df['ssh'].notna()&df["nmap"].notna()&df["Nikto"].notna()&df["ftp"].notna()&df["ipp"].notna()&df["sql"].notna()&df["ftp"].notna()&df["password"].notna()]





# load le fichier pkl contenant les paramètres de notre modèle (fichier à mettre dans le meme repertoire que le script)
with open('RandForest_model_IA_Logs.pkl', 'rb') as f:
    RandFor = pickle.load(f)


# On transforme certaines features en type categories pour que le modèle les comprenne correctement
for col in ['ssh',"nmap","Nikto","ftp","ipp","sql","http","password"]:
    df[col] = df[col].astype('category',copy=False)



#df['Result'] = RandFor.predict(df)		#on effectue les prédictions et on les met dans une colonne Result    # *********
df['Result'] = RandFor.predict(df[['ssh',"nmap","Nikto","ftp","ipp","sql","http","password"]])        #         &&&&&&&

def message_rapport(code):
	Message = {"RAS" : "Aucune tentative d'intrusion n'a été détecté.",
	"SSH":"On a détecté une tentative de connexion ssh. On est au début d'une attaque.",
	"SP": "On a détecté un nmap et une adresse IP différente. On est sur un scan de port donc au début d'attaque.",
	"SVM": "On a détecté un scan de vulnérabilité Nikto. On est au milieu de l'attaque.",
	"SPM":"On a détecté une tentative d\'attaque avec un scan de port. On est au milieu de l'attaque.",
	"ASM": "On a détecté une tentative d\'attaque de serveur ftp. On est au stade avancé de l'attaque.",
	"SQLA":  "On a détecté une tentative d\'injection SQL. On est au stade avancé de l'attaque.",
	"EC": "On a détecté de trop nombreuses d'intrusions. On est sur un état critique de votre pc. Eteignez-TOUT.",
	"IPD":  "On a détecté une IP différente dans les logs "}
	code = str(code)
	return(Message.get("%s"%code))



print("Entrez la période sur laquelle vous voulez avoir votre rapport:")    
Request=input("Vous avez le choix entre Minute, Heure, Jour:")

to_substract=input("Choissez le nombre de "+Request+" que vous souhaitez: ")
to_substract=int(to_substract)

end_date = datetime.today().strftime("%Y-%m-%d %H:%M:%S") # A Remplacer par date du jour

if Request=="Jour":
    d = datetime.today() - timedelta(days=to_substract)
if Request=="Heure":
    d = datetime.today() - timedelta(hours=to_substract)
if Request == "Minute":
    d = datetime.today() - timedelta(minutes=to_substract)

start_date = d.strftime("%Y-%m-%d %H:%M:%S")
print(start_date)
mask = (df.index >= start_date) & (df.index <= end_date)   # *********
#mask = (df.date >= start_date) & (df.date <= end_date)      #              &&&&&&



dfsortie = df[mask]

print("Entre ",start_date,"et ",end_date," :")
dico =  dfsortie['Result'].value_counts().to_dict()
print("Stats de la détection :")
for k in dico:
    print("\t on a détecté ",dico.get(k),"logs avec le label", k )
print(dfsortie.groupby(['source','Result']).size().unstack(fill_value = 0))
print("Légende : \n - RAS = Rien a Signaler  \n - IPD = IP différente détectée \n - ASM = Attaque serveur ftp \n - SQLA = Injection SQL \n - SQLA = Injection SQL\n - SSH = Tentative connexion SSH  \n - SP = Scan de Port \n - SVM = Scan vulnérabilité Nikto")
dfsortie['group'] = dfsortie['Result'].ne(dfsortie['Result'].shift()).cumsum()


#column_names = ['sql','ipp','ftp','Nikto','nmap','password','ssh','http']         #***********
#dfsortie['Somme_patterns']= dfsortie[column_names].sum(axis=1)
#dfsortie['Somme_patterns'].fillna(0, inplace=True) 
#dfsortie.loc[(dfsortie['Somme_patterns']>=4),"critique"] = 1
#dfsortie['critique'].fillna(0, inplace=True)                                 #***********

column_names = ['sql','ipp','ftp','Nikto','nmap','password','ssh','http']
dfsortie['Somme_patterns']= dfsortie[column_names].sum(axis=1)
dfsortie['Somme_patterns'].fillna(0, inplace=True)
dfsortie.loc[(dfsortie['Somme_patterns']>=4),"critique"] = int(1)
dfsortie['critique'].fillna(int(0), inplace=True)
for col in ['Somme_patterns',"critique"]:
    dfsortie[col] = dfsortie[col].astype('int8',copy=False)







dfsortie.drop_duplicates(subset="group",inplace=True)

print(" Souhaitez vous un rapport détaillé de ce qui s'est passé?")
Response=input("Oui ou Non : ")

if Response=="Oui":
    for i in range(1,dfsortie.group.max()+1):
        print("  À",dfsortie.loc[dfsortie["group"]  == i].index[0],":\t",message_rapport(dfsortie.loc[dfsortie["group"]  == i].Result.values[0]),(("\n \t\t\t\t Une IP différente a été détectée : " + str(dfsortie.loc[dfsortie["group"]  == i].ip.values[0])) *(dfsortie.loc[dfsortie["group"]  == i].ipp.values[0])),("On a détecté de trop nombreuses d'intrusions. On est sur un état critique de votre pc." *(dfsortie.loc[dfsortie["group"]  == i].critique.values[0])))
else:
    print("Votre rapport est fini")

