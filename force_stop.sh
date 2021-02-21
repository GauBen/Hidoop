#!/bin/bash
USERNAME=$USER
HOSTS="pikachu carapuce salameche"


pkill -9 -f RmiCustom
pkill -9 -f HdfsNameServer

echo "On stoppe les serveurs..."
for HOST in ${HOSTS} ; do
    echo "On stoppe " $HOST
    ssh -l ${USERNAME} ${HOST} "pkill -9 -f BiNode" &
done


