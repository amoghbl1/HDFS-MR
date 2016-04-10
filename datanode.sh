# amoghbl1
# Uses Makefile, just making this cause of the submission format

make datanode

cd Datanode/bin && nohup rmiregistry&

cd datanode && java -cp ./bin/:../protobuf-java-2.6.1.jar: DataNode
