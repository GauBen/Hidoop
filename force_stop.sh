#!/bin/bash
USERNAME=$USER
HOSTS="pikachu carapuce salameche"

echo "On stoppe les serveurs..."
for HOST in ${HOSTS}; do
  echo "On stoppe " $HOST
  ssh -l ${USERNAME} ${HOST} pkill -9 -f BiNode &
done

echo "On arrete le RmiCustom..."
pkill -9 -f RmiCustom &
echo "On arrete HdfsNameServer..."
pkill -9 -f HdfsNameServer &
