
# Uses Makefile, just making this cause of the submission format

make tasktracker

cd Tasktracker/bin && nohup rmiregistry&

cd Tasktracker && java -cp ./bin/:../protobuf-java-2.6.1.jar: TaskTracker
