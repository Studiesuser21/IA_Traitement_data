
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






#Télécharger pip et pandas
!apt install pip 
!pip install pandas

#Créer fichier dataframe
import pandas as pd
data=pd.read_csv('syslog.csv')

spark-shell --master spark://master:7077

