# Hidoop

Le projet s’utilise à travers trois scripts en plus de l’application distribuée. Voici les commandes à exécuter pour :
./deploy.sh : compile le projet et l’installe sur trois machines distantes
./exec.sh : démarre les noeuds et le serveur de nom
java votre.ApplicationDistribuee : uploadez des fichiers et lancez une ou plusieurs applications
./force_stop.sh : arrête les serveurs
Pour uploader un fichier les noeuds, il faut utiliser
java -cp /work/$USER/Hidoop hdfs.HdfsClient write line <fichier>
