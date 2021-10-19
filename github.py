
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
datetime = datetime.datetime.now().replace(microsecond=0)



df_alternative =pd.read_csv(r'C:\Users\axelg\Downloads\youpi\alternatives.csv',header=None,error_bad_lines=False)
df_dmesg =pd.read_csv(r'C:\Users\axelg\Downloads\youpi\dmesg.csv',header=None,error_bad_lines=False)

#Formatage de la date
def formatagedate(date):
    MOIS = {"Jan" : "01","Feb" : "02","Mar" : "03","Apr" : "04","May" : "05","Jun" : "06",
            "Jul" : "07","Aug" : "08","Sep" : "09","Oct" : "10","Nov" : "11","Dec" : "12",}  
    liste_date = date.split()
    if len(liste_date)== 3:
        mois = MOIS.get("%s"%liste_date[0])
        date = "2021"+"/"+str(mois)+"/"+ liste_date[1]+" "+liste_date[2]
    return(date)

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
df_dmesg["date"]="0000/00/00 00:00:00"
df_dmesg["process"]="no process"
df_dmesg["message_log"]=df_dmesg[0]
df_dmesg["source"]="dmesg"
df_dmesg['date_traitement']=datetime_object







df['date'] = df['date'].apply(formatagedate) # pour formater les dates sur le dataframe final.
