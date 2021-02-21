#!/bin/bash
USERNAME=$USER
HOSTS="pikachu.enseeiht.fr carapuce.enseeiht.fr salameche.enseeiht.fr"

NAME_SERVER_PORT=30000
RMI_SERVER_PORT=4000

CUSTOM_PATH="/work/"$USERNAME"/Hidoop"

# Le script execute sur chaque machine - adresse du nameserver, port du nameserver, nom du dossier dans lequel y'a les fichiers de Hdfs,adresse du RMI, port du RMI
SCRIPT=mkdir "node" && "java -cp " $CUSTOM_PATH " application.BiNode" ${CURRENT_HOST} ${NAME_SERVER_PORT} "node" ${CURRENT_HOST} ${RMI_SERVER_PORT} >BiNodeLog.log

cd Hidoop

# On demarre le RMI
java application.RmiCustom ${RMI_SERVER_PORT} &# On fait un RMI registry en tache de fond

java "hdfs.HdfsNameServer" $NAME_SERVER_PORT

# Execution des commandes sur les machines distantes
for HOST in ${HOSTS}; do
  echo "On demarre " $HOST
  ssh -l ${USERNAME} ${HOST} "${SCRIPT}"
done

# TODO : STOPPER LES SERVEURS LORSQUE ON APPUIE SUR UN CARACTERE !
echo "Appuyez sur un bouton pour arreter"
while [ true ]; do
  read -t 3 -n 1
  if [ $? = 0 ]; then
    exit
  fi
done
pkill -9 -f RmiCustom

echo "On stoppe les serveurs..."
for HOST in ${HOSTS}; do
  echo "On stoppe " $HOST
  ssh -l ${USERNAME} ${HOST} "pkill -9 -f BiNode" &
done

cd ..
