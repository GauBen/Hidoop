#!/bin/bash
USERNAME=$USER
HOSTS="pikachu carapuce salameche"

# TODO : PAS SUR QUE $HOSTNAME MARCHE !!!
CURRENT_HOST=$HOSTNAME # Le nom du serveur qui héberge le HdfsServer et le rmiserver ATTENTION a verifier si c'est defini sur les pc de l'enseeiht
NameserverPort=30000
RmiserverPort=4000

CUSTOM_PATH="/work/"$USERNAME"/Hidoop"


# Le script execute sur chaque machine - adresse du nameserver, port du nameserver, nom du dossier dans lequel y'a les fichiers de Hdfs,adresse du RMI, port du RMI
SCRIPT=mkdir "node" && "java -cp" $CUSTOM_PATH "application.BiNode" ${CURRENT_HOST} ${NameserverPort} "node" ${CURRENT_HOST} ${RmiserverPort}  > BiNodeLog.log

cd Hidoop

# On demarre le RMI
java application.RmiCustom ${RmiserverPort} & # On fait un RMI registry en tache de fond

java "hdfs.HdfsNameServer" # TODO : ajouter les arguments

# Execution des commandes sur les machines distantes
for HOST in ${HOSTS} ; do
    echo "On demarre " $HOST
    ssh -l ${USERNAME} ${HOST} "${SCRIPT}"
done


# TODO : STOPPER LES SERVEURS LORSQUE ON APPUIE SUR UN CARACTERE !
echo "Appuyez sur un bouton pour arreter"
while [ true ] ; do
read -t 3 -n 1
if [ $? = 0 ] ; then
exit ;
fi
done
pkill -9 -f RmiCustom

echo "On stoppe les serveurs..."
for HOST in ${HOSTS} ; do
    echo "On stoppe " $HOST
    ssh -l ${USERNAME} ${HOST} "pkill -9 -f BiNode" &
done

cd ..
