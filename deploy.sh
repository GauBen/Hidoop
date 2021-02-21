#!/bin/bash
# Deployer les fichiers java sur les machines ENSEEIHT

USERNAME=$USER
HOSTS="pikachu.enseeiht.fr carapuce.enseeiht.fr salameche.enseeiht.fr"

SCRIPTNETTOYAGE="rm -rf /work/"$USERNAME
SCRIPTCREATION="mkdir /work/"$USERNAME

# Execution des commandes sur les machines distantes
for HOST in ${HOSTS} ; do
  echo "On nettoie " $HOST
    ssh -l ${USERNAME} ${HOST} "${SCRIPTNETTOYAGE}"
    echo "Envoi du nouveau dossier ... "
    ssh -l ${USERNAME} ${HOST} "${SCRIPTCREATION}"
    scp -r "../Hidoop" $USERNAME"@"$HOST":/work/"$USERNAME"/Hidoop"
done
echo "Done !"
sleep 3

#



