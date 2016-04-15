# amoghbl1
# Uses Makefile, just making this cause of the submission format

make namenode

make rmiregistry

cd Namenode && java -cp ./bin/:../protobuf-java-2.6.1.jar: NameNode
