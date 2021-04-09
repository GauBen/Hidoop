#!/bin/bash
USERNAME=$USER
HOSTS="pikachu.enseeiht.fr carapuce.enseeiht.fr salameche.enseeiht.fr"

CURRENT_HOST=$HOSTNAME # Le nom du serveur qui héberge le HdfsServer et le rmiserver ATTENTION a verifier si c'est defini sur les pc de l'enseeiht
NameserverPort=51200
RmiserverPort=4000

CUSTOM_PATH="/work/"$USERNAME"/Hidoop"

# Le script execute sur chaque machine - adresse du nameserver, port du nameserver, nom du dossier dans lequel y'a les fichiers de Hdfs,adresse du RMI, port du RMI
SCRIPT="java -cp $CUSTOM_PATH application.BiNode ${CURRENT_HOST} ${NameserverPort} ${CUSTOM_PATH}/node/ ${CURRENT_HOST} ${RmiserverPort}  > BiNodeLog.log"

# On demarre le RMI
java -cp $CUSTOM_PATH application.RmiCustom $RmiserverPort & # On fait un RMI registry en tache de fond

# Execution des commandes sur les machines distantes
for HOST in ${HOSTS}; do
  echo "On demarre " $HOST
  ssh -l ${USERNAME} ${HOST} "${SCRIPT}" &
done

java -cp $CUSTOM_PATH "hdfs.HdfsNameServer" $NameserverPort &

# STOPPER LES SERVEURS LORSQUE ON APPUIE SUR UN CARACTERE !

while [ true ]; do
  echo "Appuyez sur un bouton pour arreter"
  read -t 3 -n 1
  if [ $? = 0 ]; then
    exit
  fi
done
echo "Arret en cours..."


echo "On stoppe les serveurs..."
for HOST in ${HOSTS}; do
  echo "On stoppe " $HOST
  ssh -l ${USERNAME} ${HOST} pkill -9 -f BiNode &
done

sleep 1
echo "On arrete le RmiCustom..."
pkill -9 -f RmiCustom &
