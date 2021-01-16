#!/bin/bash
USERNAME=someUser
HOSTS="pikachu.enseeiht.fr carapuce.enseeiht.fr salameche.enseeiht.fr"


CURRENT_HOST=$HOSTNAME # Le nom du serveur qui héberge le HdfsServer et le rmiserver ATTENTION a verifier si c'est defini sur les pc de l'enseeiht
NameserverPort=30000
RmiserverPort=4000

# Le script execute sur chaque machine - adresse du nameserver, port du nameserver, nom du dossier dans lequel y'a les fichiers de Hdfs,adresse du RMI, port du RMI
SCRIPT="cd Hidoop/NodeHDFS && java -Xms8G -Xmx8G BiNode.class" ${CURRENT_HOST} ${NameserverPort} "files" ${CURRENT_HOST} ${RmiserverPort}


# On demarre le RMI
rmiregistry ${RmiserverPort} & # On fait un RMI registry en tache de fond

java "HdfsServer.class" # TODO : ajouter les arguments

# Execution des commandes sur les machines distantes
for HOSTNAME in ${HOSTS} ; do
    ssh -l ${USERNAME} -p $mypassword ${HOSTNAME} "${SCRIPT}"
done



# TODO : STOPPER LES SERVEURS LORSQUE ON APPUIE SUR UN CARACTERE !
echo "\nAppuyez sur un bouton pour arreter"
while [ true ] ; do
read -t 3 -n 1
if [ $? = 0 ] ; then
exit ;
fi
done
for HOSTNAME in ${HOSTS} ; do
    ssh -l ${USERNAME} -p $mypassword ${HOSTNAME} "pkill -9 -f BiNode"
done

