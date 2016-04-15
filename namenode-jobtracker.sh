# amoghbl1
# Uses Makefile, just making this cause of the submission format

make namenode
make jobtracker

make rmiregistry
sleep 2

cd Namenode && java -cp ./bin/:../protobuf-java-2.6.1.jar: NameNode &
cd Jobtracker && java -cp ./bin/:../protobuf-java-2.6.1.jar: JobTracker &

