#!/bin/bash
# == Déploie le projet Hidoop sur les machines ==
USERNAME=$USER
HOSTS="pikachu.enseeiht.fr carapuce.enseeiht.fr salameche.enseeiht.fr"
WORK_DIR="/work/$USER/Hidoop"

# Vérification du dossier courant
if [ ! -d "./src" ]; then
  echo "./deploy.sh doit être exécuté dans le dossier Hidoop"
  exit
fi

# Compilation
bash compile.sh

# Nettoyage et upload du projet Hidoop
SCRIPT_NETTOYAGE="rm -rf /work/"$USERNAME
SCRIPT_CREATION="mkdir -p /work/"$USERNAME
SCRIPT_CREATION_DOSSIER_NODE="mkdir -p /work/"$USERNAME/"Hidoop/node"

# Puis pareil sur les machines distantes
for HOST in ${HOSTS}; do
  echo "On nettoie " $HOST
  ssh "$USERNAME@$HOST" "$SCRIPT_NETTOYAGE"
  echo "Envoi du nouveau dossier..."
  ssh "$USERNAME@$HOST" "$SCRIPT_CREATION"
  scp -qr $WORK_DIR "$USERNAME@$HOST:$WORK_DIR"
  ssh "$USERNAME@$HOST" "$SCRIPT_CREATION_DOSSIER_NODE"
done

echo "Prêt !"
