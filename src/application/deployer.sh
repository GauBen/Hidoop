#!/bin/bash
USERNAME=someUser
HOSTS="pikachu carapuce dracaufeu salameche"


CURRENT_HOST=$HOSTNAME # Le nom du serveur qui héberge le HdfsServer et le rmiserver ATTENTION a verifier si c'est defini sur les pc de l'enseeiht
NameserverPort=30000
RmiserverPort=4000

# Le script execute sur chaque machine - adresse du nameserver, port du nameserver, adresse du RMI, port du RMI
SCRIPT="cd Hidoop/NodeHDFS && java BiNode.class" ${CURRENT_HOST} ${NameserverPort} ${CURRENT_HOST} ${RmiserverPort}


# On demarre le RMI
&rmiserver ${RmiserverPort} # On fait un RMI server dans le fond

"java HdfsServer.class" # TODO : ajouter les arguments


# Execution des commandes sur les machines distantes
for HOSTNAME in ${HOSTS} ; do
    ssh -l ${USERNAME} ${HOSTNAME} "${SCRIPT}"
done
