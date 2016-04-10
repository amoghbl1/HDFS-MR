# amoghbl1
# Uses Makefile, just making this cause of the submission format

make namenode

cd Namenode/bin && nohup rmiregistry&

cd Namenode && java -cp ./bin/:../protobuf-java-2.6.1.jar: NameNode
