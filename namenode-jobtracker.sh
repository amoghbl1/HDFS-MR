# amoghbl1
# Uses Makefile, just making this cause of the submission format

make namenode
make jobtracker

make rmiregistry

cd Namenode && java -cp ./bin/:../protobuf-java-2.6.1.jar: NameNode &
cd Jobtracker && java -cp ./bin/:../protobuf-java-2.6.1.jar: JobTracker &

