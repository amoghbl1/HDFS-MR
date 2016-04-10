all: compile

compile: clean protobuf namenode datanode client
	@echo "Start rmiregistry and run the code now!"

namenode: protobuf
	cd Namenode && javac -d bin/ -cp .:../protobuf-java-2.6.1.jar: source/NameNodeInterface.java source/NameNode.java source/RendezvousRunnableInterface.java source/DataNodeInterface.java
	@echo "Name Node compiled"

datanode: protobuf
	cd Datanode && javac -d bin/ -cp .:../protobuf-java-2.6.1.jar: source/DataNodeInterface.java source/NameNodeInterface.java source/DataNode.java
	@echo "Data Node compiled"

client: protobuf
	cd Client && javac -d bin -cp .:../protobuf-java-2.6.1.jar: source/Client.java source/NameNodeInterface.java source/DataNodeInterface.java source/RendezvousRunnableInterface.java source/JobTrackerInterface.java
	@echo "Client Compiled..."

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
