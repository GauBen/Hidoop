WORK_DIR="/work/$USER/Hidoop"
mkdir -p $WORK_DIR
echo "Compilation..."
javac -cp /home/$USER/Hidoop/src/application/jsoup-1.13.1.jar -d $WORK_DIR ./src/**/*.java
echo "Ok !"
