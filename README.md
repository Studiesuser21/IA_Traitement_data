# IA pour le Traitement de la donnée
Description: Analyse de logs et détection intrusion

Vous trouverez sur répertoire: 
 
 -code_final_commente_V2.py: le script qui effectue la collecte des logs, leur traitement et l'identification de l'attaque. Le type d'attaque est prédite par un algorithme de machine learning pré-entrainé sur une machine de la salle Noether.

  -RandForest_model_IA_Logs.pkl: fichier de sauvegarde contenant les paramètres de notre algorithme. Notre algorithme de classification est une Random Forest(forêt aléatoire). 

  -Training_RF_IA_TTT_Log.ipnb: script de code permettant l'entraînement du modèle de Machine Learning et la génération du fichier de sauvegarde RandForest_model_IA_Logs.pkl. Fichier qu'utilise ensuite notre script code_final_commente_V2.py pour prédire le type d'attaque.


Pour effectuer une analyse des logs d'un pc de la salle Noether de manière automatique, c'est à dire de la collecte des logs au résultat de notre algorithme de détection d'intrusion, il suffit de lancer code_final_commente_V2.py dans un dossier contenant RandForest_model_IA_Logs.pkl.
Le fichier training.ipnb n'est pas nécessaire pour exécuter le code. Il n'est là que pour illustrer le travail effectué sur le modèle.

Les logs analysés sont :
- vsftpd.log 
- access.log 
- vsftpd.log
- cache.log.1 
- cache.log 
- kern.log.1 
- /home/xibalpa/syslog 
- debug 
- casper.log 
- alternatives.log.1 
- auth.log 
- kern.log 
- dmesg 
- var/log/syslog 

L'output du script est constitué de deux élements, les statistiques de la détection et un rapport d'intrusion.

Pour les statistiques, on aura les nombres des différentes attaques détectées sur les logs et un tableau avec le nombre d'attaques en fonction de la source des logs.

Pour le rapport d'intrusion, l'utilisateur précise le nombre de jours / d'heures / ou de minutes qu'il désire afficher. Ainsi s'il rentre "Minute" au premier input puis "5" au deuxième input, il aura les résultats de la détection d'intrusion sur les logs des 5 dernières minutes.

La légende correspondant au sigle choisi pour les classes est explicité ci-dessous:
  - "RAS" :  "Aucune tentative d'intrusion n'a été détecté "
  - "IPD"   : On a détecté une adresse IP différente."
  - "SQLA":  "On a détecté une tentative d'injection SQL. On est au stade avancé de l'attaque."
  - "SVM":  "On a détecté un scan de vulnérabilité Nikto. On est au début de l'attaque "
  - "SSH" : "On a détecté une tentative de connexion ssh. On est au début/milieu de l'attaque."
  - "ASM": "On a détecté une tentative d'attaque de serveur ftp. On est au stade avancé de l'attaque."
  - "EC": "On a détecté de trop nombreuses de tentatives d'intrusions. On est sur un état critique de votre pc, il se peut qu'il ait été compromis"
      
Groupe composé de GAMER Axel, HADDOUNE Mohamed-Salah et LE ROUX Nathan
