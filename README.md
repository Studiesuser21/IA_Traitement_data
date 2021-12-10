# IA pour le Traitement de la donnée
Description: Analyse de logs et détection intrusion

Ci-joint:
-code_final.py: le code principal ou l'on effectue la collecte, le traitement et le machine learning sur une machine de la salle Noether.

-RandForest_model_IA_Logs.pkl: l'entrainement de notre algorithme de machine learning sauvegardé dans un fichier .pkl

-training.py: le code permettant le training du machine learning et la génération du fichier RandForest_model_IA_Logs.pkl que l'on utilise ensuite dans notre aglorithme de machine learning dans code_final.py

Pour effectuer une analyse des logs sur n'importe quel pc de la salle Noether de manière automatique de la collecte au résultat de notre algorithme de machine learning de détection d'intrusion, il suffit de lancer code_final.py dans un dossier contenant RandForest_model_IA_Logs.pkl.
Le fichier training.py n'est la que pour montrer le code de notre entrainement de machine learning, il n'est pas nécessaire à l'excecution de detection d'intrusion.

Groupe composé de GAMER Axel, HADDOUNE Mohamed-Salah et LE ROUX Nathan
