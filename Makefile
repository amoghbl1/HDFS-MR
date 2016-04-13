all: compile

compile: clean protobuf namenode datanode client jobtracker tasktracker
	@echo "Start rmiregistry and run the code now!"

namenode: protobuf
	cd Namenode && mkdir -p bin && javac -d bin/ -cp .:../protobuf-java-2.6.1.jar: source/NameNodeInterface.java source/NameNode.java source/RendezvousRunnableInterface.java source/DataNodeInterface.java
	@echo "Name Node compiled..."

datanode: protobuf
	cd Datanode && mkdir -p bin && javac -d bin/ -cp .:../protobuf-java-2.6.1.jar: source/DataNodeInterface.java source/NameNodeInterface.java source/DataNode.java
	@echo "Data Node compiled..."

client: protobuf
	cd Client && mkdir -p bin && javac -d bin -cp .:../protobuf-java-2.6.1.jar: source/Client.java source/NameNodeInterface.java source/DataNodeInterface.java source/RendezvousRunnableInterface.java source/JobTrackerInterface.java
	@echo "Client compiled..."

jobtracker: protobuf
	cd Jobtracker && mkdir -p bin && javac -d bin/ -cp .:../protobuf-java-2.6.1.jar: source/JobTracker.java source/JobTrackerInterface.java source/NameNodeInterface.java source/RendezvousRunnableInterface.java
	@echo "Job Tracker compiled..."

tasktracker: protobuf
	cd Tasktracker && mkdir -p bin && javac -d bin/ -cp .:../protobuf-java-2.6.1.jar: source/TaskTracker.java source/JobTrackerInterface.java com/distributed/systems/MRProtos.java
	@echo "Task Tracker Compiled..."

clean:
	rm -rf com */com
	find . -name "nohup.out" -type f -delete
	find . -name "*.class" -type f -delete
	find . -name "*.pbuf" -type f -delete
	find . -name "*.swp" -type f -delete
	if pgrep rmiregistry; then pkill rmiregistry; fi

protobuf:
	protoc -I=. --java_out=. hdfs.proto
	protoc -I=. --java_out=. mr.proto
	cp -r com Namenode
	cp -r com Datanode
	cp -r com Client
	cp -r com Jobtracker
	cp -r com Tasktracker
