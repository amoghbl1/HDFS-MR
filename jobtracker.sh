
# Uses Makefile, just making this cause of the submission format

make jobtracker

cd Jobtracker/bin && nohup rmiregistry&

cd Jobtracker && java -cp ./bin/:../protobuf-java-2.6.1.jar: JobTracker
