# amoghbl1
# Uses Makefile, just making this cause of the submission format

make datanode

make rmiregistry

cd Datanode && java -cp ./bin/:../protobuf-java-2.6.1.jar: DataNode
