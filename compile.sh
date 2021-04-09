WORK_DIR="/work/$USER/Hidoop"
mkdir -p $WORK_DIR
echo "Compilation..."
javac -d $WORK_DIR ./src/**/*.java
echo "Ok !"
