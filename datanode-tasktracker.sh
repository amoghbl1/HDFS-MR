# amoghbl1
# Uses Makefile, just making this cause of the submission format

make datanode
make tasktracker

make rmiregistry

cd Datanode && java -cp ./bin/:../protobuf-java-2.6.1.jar: DataNode &
cd Tasktracker && java -cp ./bin:../protobuf-java-2.6.1.jar:../mapper-reducer.jar: TaskTracker &
