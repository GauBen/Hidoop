#!/bin/bash
# Deployer les fichiers java sur les machines ENSEEIHT


USERNAME=someUser
HOSTS="pikachu.enseeiht.fr carapuce.enseeiht.fr salameche.enseeiht.fr"


java "HdfsServer.class" # TODO : ajouter les arguments

SCRIPTNETTOYAGE="rm -rf /work/"$someUser



# Execution des commandes sur les machines distantes
for HOST in ${HOSTS} ; do
  echo "On nettoie " $HOST
    ssh -l ${USERNAME} ${HOST} "${SCRIPTNETTOYAGE}"
    echo "Envoi du nouveau dossier ... "
    "scp -r Hiddop" $someUser"@"$HOST":/work/"$someUser"/Hidoop"
done

#



