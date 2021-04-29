echo -n "On envoie fatlorem sur le HDFS!"

CUSTOM_PATH="/work/"$USERNAME"/Hidoop"

java -cp $CUSTOM_PATH hdfs.HdfsClient write line fat_lorem.txt & # On fait un RMI registry en tache de fond
