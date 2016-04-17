import com.distributed.systems.HDFSProtos.AssignBlockResponse;
import com.distributed.systems.HDFSProtos.OpenFileRequest;
import com.distributed.systems.HDFSProtos.OpenFileResponse;
import com.distributed.systems.HDFSProtos.ReadBlockRequest;
import com.distributed.systems.HDFSProtos.ReadBlockResponse;
import com.distributed.systems.HDFSProtos.WriteBlockRequest;
import com.distributed.systems.HDFSProtos.WriteBlockResponse;
import com.distributed.systems.MRProtos.HeartBeatRequest;
import com.distributed.systems.MRProtos.HeartBeatResponse;
import com.distributed.systems.MRProtos.MapTaskInfo;
import com.distributed.systems.MRProtos.MapTaskStatus;
import com.distributed.systems.MRProtos.ReducerTaskInfo;
import com.distributed.systems.MRProtos.ReduceTaskStatus;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class TaskTracker {

    private int BLOCK_SIZE_IN_BYTES = 32000000;

    // Using hdfs_data_node.conf as the default config file.
    private static String configFile = "hdfs_mr_task_tracker.conf";

    // This is parsed from the config file on object creation
    private static String jobTrackerIP;
    private static String nameNodeIP;
    private static String myIP;
    private static int myNumMapSlotsFree;
    private static int myNumReduceSlotsFree;
    private static int myID;
    private final Object queueLock = new Object();
    private static MapTaskStatus myMapTaskStatus;
    private static ReduceTaskStatus myReduceTaskStatus;
    public static HashMap<Integer, MapThreadRunnable> processingMapQueue = new HashMap<Integer, MapThreadRunnable>();
    public static HashMap<Integer, ReduceThreadRunnable> processingReduceQueue = new HashMap<Integer, ReduceThreadRunnable>();
    public static HashMap<Integer, MapThreadRunnable> completeMapQueue = new HashMap<Integer, MapThreadRunnable>();
    public static HashMap<Integer, ReduceThreadRunnable> completeReduceQueue = new HashMap<Integer, ReduceThreadRunnable>();

    public MapTaskStatus getMapTaskStatus() {
        return this.myMapTaskStatus;
    }

    public ReduceTaskStatus getReduceTaskStatus() {
        return this.myReduceTaskStatus;
    }

    public String getJobTrackerIP() {
        return this.jobTrackerIP;
    }

    public String getMyIP() {
        return this.myIP;
    }
    public int getMyID() {
        return this.myID;
    }

    public int getNumMapSlotsFree() {
        return this.myNumMapSlotsFree;
    }

    public int getNumReduceSlotsFree() {
        return this.myNumReduceSlotsFree;
    }

    public TaskTracker(String conf) throws RemoteException{
        this.myNumMapSlotsFree = 1;//change
        this.myNumReduceSlotsFree = 1;//change
        //this.myMapTaskStatus = getMapStatus();
        //this.myReduceTaskStatus = getReduceStatus();
        this.configFile = conf;

        BufferedReader fileReader = null;
        String configLine;
        // Parsing the config file for configs
        try {
            fileReader = new BufferedReader(new FileReader(this.configFile));
        } catch (Exception e) {
            System.out.println("Bad config file?? " + e.getMessage());
            e.printStackTrace();
        } 
        while(true) { 
            try {
                if((configLine = fileReader.readLine()) == null)
                    break;
            } catch (Exception e) {
                System.out.println("Config file read problems?? " + e.getMessage());
                e.printStackTrace();
                break;
            }   
            if(configLine.startsWith("jobTrackerIP")) {
                this.jobTrackerIP = configLine.split(" ")[1];
            }
            if(configLine.startsWith("nameNodeIP")) {
                this.nameNodeIP = configLine.split(" ")[1];
            }
            if(configLine.startsWith("myIP")) {
                this.myIP = configLine.split(" ")[1];
            }
            if(configLine.startsWith("myID")) {
                this.myID = Integer.parseInt(configLine.split(" ")[1]);
            }
        } 
    }

    static class HeartBeatThread extends Thread {

        private static TaskTracker parentTT;
        private static int HEART_BEAT_TIME = 1000;

        public HeartBeatThread(TaskTracker parent) {
            this.parentTT = parent;
        }

        public void run() {
            byte[] responseEncoded = null;
            HeartBeatResponse heartBeatResponse = null;
            while(true) {
                try {
                    String jobTrackerIP = parentTT.getJobTrackerIP();
                    HeartBeatRequest.Builder heartBeatRequestBuilder = HeartBeatRequest.newBuilder();
                    heartBeatRequestBuilder.setTaskTrackerId(parentTT.getMyID());
                    heartBeatRequestBuilder.setTaskTrackerIp(parentTT.getMyIP());
                    heartBeatRequestBuilder.setNumMapSlotsFree(parentTT.getNumMapSlotsFree());
                    heartBeatRequestBuilder.setNumReduceSlotsFree(parentTT.getNumReduceSlotsFree());

                    Iterator<Map.Entry<Integer, MapThreadRunnable>> completeMapTaskIterator
                        = parentTT.completeMapQueue.entrySet().iterator();
                    MapThreadRunnable mapTh;
                    while(completeMapTaskIterator.hasNext()) {
                        MapTaskStatus.Builder mapTaskStatusBuilder = MapTaskStatus.newBuilder();
                        Map.Entry<Integer, MapThreadRunnable> compMapQueueEntry = completeMapTaskIterator.next();
                        mapTh = compMapQueueEntry.getValue();
                        mapTaskStatusBuilder.setJobId(mapTh.jobID);
                        mapTaskStatusBuilder.setTaskId(mapTh.taskID);
                        mapTaskStatusBuilder.setTaskCompleted(true);
                        mapTaskStatusBuilder.setMapOutputFile(mapTh.mapOutputFile);
                        heartBeatRequestBuilder.addMapStatus(mapTaskStatusBuilder);
                        synchronized(parentTT.queueLock) {
                            completeMapTaskIterator.remove();
                        }
                    }

                    Iterator<Map.Entry<Integer, ReduceThreadRunnable>> completeReduceTaskIterator 
                        = parentTT.completeReduceQueue.entrySet().iterator();
                    ReduceThreadRunnable reduceTh;
                    while(completeReduceTaskIterator.hasNext()) {
                        ReduceTaskStatus.Builder reduceTaskStatusBuilder = ReduceTaskStatus.newBuilder();
                        Map.Entry<Integer, ReduceThreadRunnable> compReduceQueueEntry = completeReduceTaskIterator.next();
                        reduceTh = compReduceQueueEntry.getValue();
                        reduceTaskStatusBuilder.setJobId(reduceTh.jobID);
                        reduceTaskStatusBuilder.setTaskId(reduceTh.taskID);
                        reduceTaskStatusBuilder.setTaskCompleted(true);
                        heartBeatRequestBuilder.addReduceStatus(reduceTaskStatusBuilder);
                        synchronized(parentTT.queueLock) {
                            completeReduceTaskIterator.remove();
                        }
                    }

                    try {
                        JobTrackerInterface jobtracker = (JobTrackerInterface) Naming.lookup("//" +
                                jobTrackerIP + "/HDFSMRJobTracker");
                        responseEncoded = jobtracker.heartBeat(
                                heartBeatRequestBuilder.build().toByteArray());
                    } catch (Exception e) {
                        System.out.println("Job Tracker Down??");
                    }

                    try{

                        // Parse the Heart Beat Response and spawn a new thread with that data
                        heartBeatResponse = HeartBeatResponse.parseFrom(responseEncoded);

                        if(heartBeatResponse.getStatus() != 0) {
                            // Don't print, too noisy
                            // System.out.println(heartBeatResponse.toString());
                            if(heartBeatResponse.getMapTasksList().size() != 0) {
                                System.out.println("Map Task(s) Received");

                                int jobID;
                                int taskID;
                                int blockNumber;
                                String mapperName;
                                String ip;
                                MapTaskInfo mapTask;

                                for(int i = 0; i < heartBeatResponse.getMapTasksList().size(); i++) {
                                    mapTask = heartBeatResponse.getMapTasks(i);
                                    jobID = mapTask.getJobId();
                                    taskID = mapTask.getTaskId();
                                    mapperName = mapTask.getMapperName();
                                    blockNumber = mapTask.getBlockNumber();
                                    ip = mapTask.getIp();

                                    synchronized(parentTT.queueLock) {
                                        System.out.println("Starting a map thread!! ");

                                        MapThreadRunnable r  = new MapThreadRunnable(jobID, //int
                                                taskID, //int
                                                mapperName, //String
                                                blockNumber, //int
                                                ip, //String
                                                parentTT); //Tasktracker
                                        Thread th = new Thread(r);
                                        th.start();
                                        parentTT.processingMapQueue.put(taskID, r);
                                        //decrease the num of free map slots
                                        parentTT.myNumMapSlotsFree--;
                                    }
                                    System.out.println("Map Thread with taskID " + taskID + "started");
                                    System.out.println("processing queue after task with starting task with task ID: "
                                            + taskID + ": "  + processingMapQueue.toString());
                                }
                            }

                            else if(heartBeatResponse.getReduceTasksList().size() != 0) {
                                System.out.println("Reduce Task(s) Received");

                                int jobID;
                                int taskID;
                                String reducerName;
                                String outputFile = "";
                                String mapOutputFile = "";
                                ReducerTaskInfo reduceTaskInfo;

                                for(int i = 0; i < heartBeatResponse.getReduceTasksList().size(); i++) {
                                    reduceTaskInfo = heartBeatResponse.getReduceTasks(i);
                                    jobID = reduceTaskInfo.getJobId();
                                    taskID = reduceTaskInfo.getTaskId();
                                    reducerName = reduceTaskInfo.getReducerName();
                                    outputFile = reduceTaskInfo.getOutputFile();
                                    //mapOutputFile = reduceTaskInfo.getMapOutputFile();
                                    synchronized(parentTT.queueLock) {
                                        System.out.println("Starting a reduce thread!! ");

                                        ReduceThreadRunnable r  = new ReduceThreadRunnable(jobID, //int
                                                taskID, //int
                                                reducerName, //String
                                                mapOutputFile, //String
                                                outputFile, //String
                                                parentTT); //Tasktracker
                                        Thread th = new Thread(r);
                                        th.start();
                                        parentTT.processingReduceQueue.put(taskID, r);
                                        //decrease the num of free reduce slots
                                        parentTT.myNumReduceSlotsFree--;
                                    } 
                                    System.out.println("Reduce Thread with taskID " + taskID + "started");
                                    System.out.println("processing queue after task with starting task with task ID: "
                                            + taskID + ": "  + processingReduceQueue.toString());
                                }
                            }
                            else {
                                // Don't print, too noisy
                                // System.out.println("No Task Received");
                            }
                        }
                        else {
                            System.out.println("Heart Beat Response Status found to be zero for unknown reasons");
                        }
                    } catch (Exception err) {
                        System.out.println("Parsing get Response Problem?? " + err.getMessage());
                        err.printStackTrace();
                    }

                } catch (Exception e) {
                    System.out.println("Problem heart beating?? " + e.getMessage());
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(HEART_BEAT_TIME);
                } catch (Exception e) {
                    System.out.println("Thread Interrupted?? " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }

    }

    public static void main(String[] args) {
        TaskTracker me = null;
        try {
            me = new TaskTracker(configFile);
        } catch (Exception e) {
            System.out.println("Some Error: " + e.getMessage());
            e.printStackTrace();
        }
        new HeartBeatThread(me).start();
        System.out.println("Heart Beat Thread Started.");
    }

    public static class MapThreadRunnable implements Runnable {

        int jobID;
        int taskID;
        int blockNumber;
        String mapperName;
        String ip;
        TaskTracker parentTT;
        int type;
        String mapOutputFile;

        String BLOCKS_PREFIX = ".";
        int BLOCK_SIZE_IN_BYTES = 32000000;

        String rendezvousIdentifier;

        public MapThreadRunnable(int jobId, int taskId, String mapperNam, 
                int blockNo, String Ip, TaskTracker TT) throws RemoteException{
            this.jobID = jobId;
            this.taskID = taskId;
            this.mapperName = mapperNam;
            this.blockNumber = blockNo;
            this.ip = Ip;
            this.parentTT = TT;
            this.type = 1;
            this.mapOutputFile = "job_" + jobId + "_map_" + taskId;
        }


        public AssignBlockResponse getBlock() {
            AssignBlockResponse assignBlockResponse = null;
            byte[] responseEncoded = null;
            try {
                RendezvousRunnableInterface rendezvous = (RendezvousRunnableInterface) Naming.lookup("//" +
                        this.parentTT.nameNodeIP + "/" + this.rendezvousIdentifier);
                responseEncoded = rendezvous.assignBlock();
            } catch (Exception e) {
                System.out.println("Connecting to HDFS for assign block problem?? " + e.getMessage());
                e.printStackTrace();
            }

            try {
                assignBlockResponse = AssignBlockResponse.parseFrom(responseEncoded);
            } catch (Exception e) {
                System.out.println("Problem parsing assign block response?? " + e.getMessage());
                e.printStackTrace();
            }
            return assignBlockResponse;
        }


        public boolean put(String filename) {
            byte[] block = new byte[ this.BLOCK_SIZE_IN_BYTES ];
            byte[] blockToSend = null;
            int readSize;
            WriteBlockResponse writeBlockResponse = null;
            byte[] responseEncoded = null;
            try {
                FileInputStream fileInputStream = new FileInputStream(filename);
                while(true) {
                    readSize = fileInputStream.read(block);
                    if(readSize <= 0) {
                        System.out.println("Done reading file!");
                        fileInputStream.close();
                        break;
                    }
                    // Copying only the necessary bytes, out of 32MB
                    if(readSize != BLOCK_SIZE_IN_BYTES) {
                        blockToSend = new byte[readSize];
                        for(int i=0; i < readSize; i++)
                            blockToSend[i] = block[i];
                    }

                    System.out.println("Read " + readSize + " bytes");
                    WriteBlockRequest.Builder writeBlockRequestBuilder = WriteBlockRequest.newBuilder();

                    AssignBlockResponse assignBlockResponse = this.getBlock();
                    writeBlockRequestBuilder.setBlockNumber(assignBlockResponse.getBlockNumber());
                    ArrayList<String> ipsToSend = new ArrayList<String>(assignBlockResponse.getDataNodeIPsCount());
                    for(int i=0; i < assignBlockResponse.getDataNodeIPsCount(); i++)
                        ipsToSend.add(assignBlockResponse.getDataNodeIPs(i));
                    String ipToSend = ipsToSend.get(0);
                    System.out.println("IP to send: " + ipToSend);

                    for(int i=1; i < ipsToSend.size(); i++)
                        writeBlockRequestBuilder.addRemainingDataNodeIPs(ipsToSend.get(i));

                    DataNodeInterface datanode = (DataNodeInterface) Naming.lookup("//" +
                            ipToSend + "/HDFSDataNode");
                    // We send a smaller block if 
                    if(readSize != BLOCK_SIZE_IN_BYTES) {
                        responseEncoded = datanode.writeBlock(writeBlockRequestBuilder.build().toByteArray(), blockToSend);
                    } else {
                        responseEncoded = datanode.writeBlock(writeBlockRequestBuilder.build().toByteArray(), block);
                    }
                }

            } catch (Exception e) {
                System.out.println("put file problems?? " + e.getMessage());
                e.printStackTrace();
            }
            try {
                writeBlockResponse = WriteBlockResponse.parseFrom(responseEncoded);
            } catch (Exception e) {
                System.out.println("Parsing response from threads write call?? " + e.getMessage());
                e.printStackTrace();
            }
            if(writeBlockResponse.getStatus() == 1)
                return true;
            return false;
        }


        public boolean open(String fileName, boolean forRead) {

            OpenFileRequest.Builder openFileRequestBuilder = OpenFileRequest.newBuilder();
            openFileRequestBuilder.setFileName(fileName);
            openFileRequestBuilder.setForRead(forRead);
            OpenFileResponse openFileResponse = null;

            byte[] requestEncoded = openFileRequestBuilder.build().toByteArray();
            byte[] responseEncoded = null;

            try {

                NameNodeInterface nameNode = (NameNodeInterface) Naming.lookup("//" +
                        this.parentTT.nameNodeIP + "/HDFSNameNode"); // This name node request location is hard coded.
                responseEncoded = nameNode.openFile(requestEncoded);

            } catch (Exception e) {
                System.out.println("Connecting to HDFS for open file problem?? " + e.getMessage());
                e.printStackTrace();
            }

            try {
                openFileResponse = OpenFileResponse.parseFrom(responseEncoded);
            } catch (Exception e) {
                System.out.println("Problem parsing open response?? " + e.getMessage());
                e.printStackTrace();
            }

            System.out.println("Open file reponse rendezvous: " + openFileResponse.getRendezvousIndentifier());
            this.rendezvousIdentifier = openFileResponse.getRendezvousIndentifier();

            try { Thread.sleep(1000); } catch (Exception e) {}// Prevent talking to rendezvous before it binds

            return true;
        }

        public boolean close() {
            try {
                RendezvousRunnableInterface rendezvous = (RendezvousRunnableInterface) Naming.lookup("//" +
                        this.parentTT.nameNodeIP + "/" + this.rendezvousIdentifier);
                rendezvous.closeFile();

            } catch (Exception e) {
                System.out.println("Connecting to HDFS for close file problem?? " + e.getMessage());
                e.printStackTrace();
            }
            return true;
        }

        public void run() {
            System.out.println("Map Thread running task with tid: " + this.taskID + " for jid: " + this.jobID);

            // Run map function on Mapper class, who we have from interface.
            // Map function takes string and loads that class and does whatever it has to.
            try {
                MapperInterface mapper = (MapperInterface) Class.forName(this.mapperName).newInstance();
                String tempFileName = BLOCKS_PREFIX + this.blockNumber;
                try {
                    ReadBlockRequest.Builder readBlockRequestBuilder = ReadBlockRequest.newBuilder();
                    byte[] recievedBytes = null;
                    readBlockRequestBuilder.setBlockNumber(this.blockNumber);
                    DataNodeInterface datanode = (DataNodeInterface) Naming.lookup("//" +
                            this.ip + "/HDFSDataNode");
                    recievedBytes = datanode.readBlock(
                            readBlockRequestBuilder.build().toByteArray());
                    try {
                        FileOutputStream fileOutputStream = new FileOutputStream(tempFileName, true);
                        fileOutputStream.write(recievedBytes);
                        fileOutputStream.close();
                    } catch (Exception e) {
                        System.out.println("Problem appending to file??" + e.getMessage());
                        e.printStackTrace();
                    }

                } catch(Exception e) {
                    System.out.println("Problem downloading block from Data Node??" + e.getMessage());
                    e.printStackTrace();
                }
                // Actually compute the map, and store it in the output file.
                mapper.map(tempFileName, this.mapOutputFile);
                // Open the file on HDFS
                this.open(this.mapOutputFile, false);
                // Push the output file to the HDSF
                this.put(this.mapOutputFile);
                // Closing the file on HDFS
                this.close();

                // Done with all processing, doing cleanup.
                try {
                    new File(tempFileName).delete();
                    new File(this.mapOutputFile).delete();
                } catch (Exception e) {
                    System.out.println("Problem cleaning up?? " + e.getMessage());
                    e.printStackTrace();
                }
            } catch(Exception e) {
                System.out.println("Problem loading class dynamically??" + e.getMessage());
                e.printStackTrace();
            }
            synchronized(this.parentTT.queueLock) {
                MapThreadRunnable r;
                if((r = this.parentTT.processingMapQueue.get(this.taskID)) == null) {
                    System.out.println("Can not fetch non-existent key from processing map.");
                }
                if(this.parentTT.processingMapQueue.remove(this.taskID) == null) {
                    System.out.println("Can not remove non-existent key from processing map.");
                }
                this.parentTT.completeMapQueue.put(this.taskID, r);
            }
            System.out.println("Map Thread completed task with tid: " + this.taskID);
            System.out.println("proccesing queue after completing map task, tid: " + taskID + ": " + processingMapQueue);
            System.out.println("complete queue after completing map task, tid: " + taskID + ": " + completeMapQueue);
        }
    }

    public static class ReduceThreadRunnable implements Runnable {

        int jobID;
        int taskID;
        String reducerName;
        String mapOutputFile;
        String outputFile;
        TaskTracker parentTT;
        int type;
        String reduceOutputFile;

        public ReduceThreadRunnable(int jobId, int taskId, String reducerNam, 
                String mapOpFile, String opFile, TaskTracker TT) throws RemoteException{
            this.jobID = jobId;
            this.taskID = taskId;
            this.reducerName = reducerNam;
            this.mapOutputFile = "job_" + jobID + "_map_" + taskID;
            this.outputFile = opFile;
            this.parentTT = TT;
            this.type = 2;
            this.reduceOutputFile = opFile + "_ " + jobId + "_" + taskId;
        }
        public void run() {
            //put to sleep for 2 seconds for testing purpose
            try { Thread.sleep(2000); } catch (Exception e) {
                System.out.println("Problem in trying to sleep thread?? " + e.getMessage());
                e.printStackTrace();
            }
            System.out.println("Map Thread running task with tid: " + this.taskID);

            synchronized(this.parentTT.queueLock) {
                ReduceThreadRunnable r;
                if((r = this.parentTT.processingReduceQueue.get(this.taskID)) == null) {
                    System.out.println("Can not fetch non-existent key from processing reduce.");
                }
                if(this.parentTT.processingReduceQueue.remove(this.taskID) != null) {
                    System.out.println("Can not remove non-existent key from processing reduce.");
                }
                this.parentTT.completeReduceQueue.put(this.taskID, r);
            }

            System.out.println("Reduce Thread completed task with tid: " + this.taskID);
            System.out.println("proccesing queue after completing reduce task, tid: " + taskID + ": " + processingReduceQueue);
            System.out.println("complete queue after completing reduce task, tid: " + taskID + ": " + completeReduceQueue);
        }
    }

}
