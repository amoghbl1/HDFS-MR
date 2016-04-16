import com.distributed.systems.HDFSProtos.NameNodeBlockDataNodeMapping;
import com.distributed.systems.HDFSProtos.NameNodeBlockDataNodeMappings;
import com.distributed.systems.HDFSProtos.NameNodeBlockDataNodeMappingsRequest;
import com.distributed.systems.HDFSProtos.NameNodeBlockDataNodeMappingsResponse;
import com.distributed.systems.HDFSProtos.OpenFileRequest;
import com.distributed.systems.HDFSProtos.OpenFileResponse;
import com.distributed.systems.MRProtos.HeartBeatRequest;
import com.distributed.systems.MRProtos.HeartBeatResponse;
import com.distributed.systems.MRProtos.JobStatusRequest;
import com.distributed.systems.MRProtos.JobStatusResponse;
import com.distributed.systems.MRProtos.JobSubmitRequest;
import com.distributed.systems.MRProtos.JobSubmitResponse;
import com.distributed.systems.MRProtos.MapTaskInfo;
import com.distributed.systems.MRProtos.MapTaskStatus;
import com.distributed.systems.MRProtos.ReducerTaskInfo;
import com.distributed.systems.MRProtos.ReduceTaskStatus;
import java.io.BufferedReader;
import java.io.FileReader;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class JobTracker extends UnicastRemoteObject implements JobTrackerInterface {

    // Using this default config file
    private static String configFile = "hdfs_mr_job_tracker.conf";

    private static String nameNodeIP = "127.0.0.1";

    private static int currentJobID = 0;

    public static HashMap<Integer, JobRunnerThread> currentJobThreads = new HashMap<Integer, JobRunnerThread>();

    public static HashMap<String, ArrayList<TaskData>> toProcessMapQueue = new HashMap<String, ArrayList<TaskData>>();

    public static HashMap<String, ArrayList<TaskData>> processingMapQueue = new HashMap<String, ArrayList<TaskData>>();

    public static HashMap<String, ArrayList<TaskData>> completeMapQueue = new HashMap<String, ArrayList<TaskData>>();

    public static HashMap<String, ArrayList<TaskData>> toProcessReduceQueue = new HashMap<String, ArrayList<TaskData>>();

    public static HashMap<String, ArrayList<TaskData>> processingReduceQueue = new HashMap<String, ArrayList<TaskData>>();

    public static HashMap<String, ArrayList<TaskData>> completeReduceQueue = new HashMap<String, ArrayList<TaskData>>();


    private final Object CJTLock = new Object();
    private final Object queueLock = new Object();

    public JobTracker(String conf) throws RemoteException {
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
            if(configLine.startsWith("nameNodeIP")) {
                this.nameNodeIP = configLine.split(" ")[1];
            }
        }
    }

    //Counts the no of tasks for a jobid in the queue hashmaps
    public int countTasks(HashMap<String, ArrayList<TaskData>> queueMap, int jid) {
        int count = 0;
        for(ArrayList<TaskData> tdList : queueMap.values()) {
            for(TaskData td : tdList) {
                if(td.jobID == jid) {
                    count += 1;
                }
            }
        }
        return count;
    }

    //Move all the tasks of a particular job from one queu to antother
    public void moveTasks(HashMap<String, ArrayList<TaskData>> fromQueue, HashMap<String, ArrayList<TaskData>> toQueue, int jid) {
        for(Map.Entry<String, ArrayList<TaskData>> entry : toQueue.entrySet()) {
            String ip = entry.getKey();
            ArrayList<TaskData> tdlist = new ArrayList<TaskData>();
            ArrayList<TaskData> tdlistiterator = entry.getValue();
            for(TaskData td : tdlistiterator) {
                if(td.jobID == jid) {
                    synchronized(queueLock) {
                        fromQueue.get(ip).remove(td);
                    }
                    tdlist.add(td);
                }
            }
            synchronized(queueLock) {
                toQueue.put(ip, tdlist);
            }
        }
    }

    // Checks if a certain JID is present in any of the taskData objs in all mappings of the ToProcessQueue
    public boolean inToProcessQueue(int JID, ArrayList<String> taskIPs) {
        synchronized(queueLock) {
            for(String ip : taskIPs) {
                ArrayList<TaskData> tds = this.toProcessMapQueue.get(ip);
                for(TaskData td : tds) {
                    if(td.jobID == JID)
                        return true;
                }
            }
        }
        return false;
    }

    // Checks if a certain JID is present in any of the taskData objs in all mappings of the ProcessingQueue
    public boolean inProcessingQueue(int JID, ArrayList<String> taskIPs) {
        synchronized(queueLock) {
            for(String ip : taskIPs) {
                ArrayList<TaskData> tds = this.processingMapQueue.get(ip);
                for(TaskData td : tds) {
                    if(td.jobID == JID)
                        return true;
                }
            }
        }
        return false;
    }


    public int addToProcessQueue(String Ip, TaskData taskData, int type) {
        HashMap<String, ArrayList<TaskData>> toProcessQueue;
        if(type == 1) {
            toProcessQueue = this.toProcessMapQueue;
        }
        else {
            toProcessQueue = this.toProcessReduceQueue;
        }

        if(toProcessQueue.containsKey(Ip)) {
            if(toProcessQueue.get(Ip).add(taskData)){
                return 1;
            }
            else {
                return 0;
            }
        }
        else {
            ArrayList<TaskData> td = new ArrayList<TaskData>();
            td.add(taskData);
            toProcessQueue.put(Ip, td);
            return 1;
        }
    }

    public int addToProcessingQueue(String Ip, TaskData taskData, int type) {
        HashMap<String, ArrayList<TaskData>> processingQueue;
        if(type == 1) {
            processingQueue = this.processingMapQueue;
        }
        else {
            processingQueue = this.processingReduceQueue;
        }

        if(processingQueue.containsKey(Ip)) {
            if(processingQueue.get(Ip).add(taskData)) {
                return 1;
            }
            else {
                return 0;
            }
        }
        else {
            ArrayList<TaskData> td = new ArrayList<TaskData>();
            td.add(taskData);
            processingQueue.put(Ip, td);
            return 1;
        }
    }

    public int addToCompleteQueue(String Ip, TaskData taskData, int type) {
        HashMap<String, ArrayList<TaskData>> completeQueue;
        if(type == 1) {
            completeQueue = this.completeMapQueue;
        }
        else {
            completeQueue = this.completeReduceQueue;
        }

        if(completeQueue.containsKey(Ip)) {
            if(completeQueue.get(Ip).add(taskData)) {
                return 1;
            }
            else {
                return 0;
            }
        }
        else {
            ArrayList<TaskData> td = new ArrayList<TaskData>();
            td.add(taskData);
            completeQueue.put(Ip, td);
            return 1;
        }
    }

    public TaskData getFromToProcessQueue(String Ip, int type) {
        HashMap<String, ArrayList<TaskData>> toProcessQueue;
        if(type == 1) {
            toProcessQueue = this.toProcessMapQueue;
        }
        else {
            toProcessQueue = this.toProcessReduceQueue;
        }

        if(toProcessQueue.containsKey(Ip)) {
            if(!toProcessQueue.get(Ip).isEmpty()) {
                return toProcessQueue.get(Ip).get(0);
            }
        }
        return null;
    }

    public TaskData getFromProcessingQueue(String Ip, int type) {
        HashMap<String, ArrayList<TaskData>> processingQueue;
        if(type == 1) {
            processingQueue = this.processingMapQueue;
        }
        else {
            processingQueue = this.processingReduceQueue;
        }

        if(processingQueue.containsKey(Ip)) {
            if(!processingQueue.get(Ip).isEmpty()) {
                return processingQueue.get(Ip).get(0);
            }
        }
        return null;
    }

    public TaskData getFromCompleteQueue(String Ip, int type) {
        HashMap<String, ArrayList<TaskData>> completeQueue;
        if(type == 1) {
            completeQueue = this.completeMapQueue;
        }
        else {
            completeQueue = this.completeReduceQueue;
        }

        if(completeQueue.containsKey(Ip)) {
            if(!completeQueue.get(Ip).isEmpty()) {
                return completeQueue.get(Ip).get(0);
            }
        }
        return null;
    }

    public int rmFromToProcessQueue(String Ip, TaskData td, int type) {
        HashMap<String, ArrayList<TaskData>> toProcessQueue;
        if(type == 1) {
            toProcessQueue = this.toProcessMapQueue;
        }
        else {
            toProcessQueue = this.toProcessReduceQueue;
        }

        if(toProcessQueue.containsKey(Ip)) {
            if(toProcessQueue.get(Ip).remove(td)) {
                return 1;
            }
            else {
                return 0;
            }
        }
        return 0;
    }

    public int rmFromProcessingQueue(String Ip, TaskData td, int type) {
        HashMap<String, ArrayList<TaskData>> processingQueue;
        if(type == 1) {
            processingQueue = this.processingMapQueue;
        }
        else {
            processingQueue = this.processingReduceQueue;
        }

        if(processingQueue.containsKey(Ip)) {
            if(processingQueue.get(Ip).remove(td)) {
                return 1;
            }
            else {
                return 0;
            }
        }
        return 0;
    }

    public int rmFromCompleteQueue(String Ip, TaskData td, int type) {
        HashMap<String, ArrayList<TaskData>> completeQueue;
        if(type == 1) {
            completeQueue = this.completeMapQueue;
        }
        else {
            completeQueue = this.completeReduceQueue;
        }

        if(completeQueue.containsKey(Ip)) {
            if(completeQueue.get(Ip).remove(td)) {
                return 1;
            }
            else {
                return 0;
            }
        }
        return 0;
    }

    public synchronized int getAndIncrementJobID() {
        this.currentJobID += 1;
        return this.currentJobID - 1;
    }

    public void removeJobRunnerFromJRList(int id) {
        synchronized(CJTLock) {
            if(currentJobThreads.containsKey(id)) {
                currentJobThreads.remove(id);
                System.out.println("Job Completed!! JID: " + id);
            }
        }
    }

    /* JobSubmitResponse jobSubmit(JobSubmitRequest) */
    public byte[] jobSubmit(byte[] encodedRequest) throws RemoteException {
        System.out.println("Received job submit!!");
        JobSubmitResponse.Builder jobSubmitResponseBuilder = JobSubmitResponse.newBuilder();
        try {
            JobSubmitRequest jobSubmitRequest = JobSubmitRequest.parseFrom(encodedRequest);
            System.out.println(jobSubmitRequest.toString());

            JobRunnerThread jrt = new JobRunnerThread(this,
                    jobSubmitRequest.getMapperName(),
                    jobSubmitRequest.getReducerName(),
                    jobSubmitRequest.getInputFile(),
                    jobSubmitRequest.getOutputFile(),
                    jobSubmitRequest.getNumReduceTasks());
            jrt.start();
            synchronized(CJTLock) {
                currentJobThreads.put(
                        new Integer(jrt.getJID()),
                        jrt);
            }

            jobSubmitResponseBuilder.setStatus(1);
            // Queue job and get job ID and then setting job ID.
            jobSubmitResponseBuilder.setJobId(jrt.getJID());
        } catch (Exception e) {
            System.out.println("Problem parsing job submit request??" + e.getMessage());
            e.printStackTrace();
        }
        return jobSubmitResponseBuilder.build().toByteArray();
    }

    /* JobStatusResponse getJobStatus(JobStatusRequest) */
    public byte[] getJobStatus(byte[] encodedRequest) throws RemoteException {
        System.out.println("Received job status!!");
        JobStatusResponse.Builder jobStatusResponseBuilder = JobStatusResponse.newBuilder();
        try {
            JobStatusRequest jobStatusRequest = JobStatusRequest.parseFrom(encodedRequest);
            System.out.println(jobStatusRequest.toString());

            jobStatusResponseBuilder.setStatus(1);
            synchronized(CJTLock) {
                if(currentJobThreads.containsKey(jobStatusRequest.getJobId())) {
                    jobStatusResponseBuilder.setJobDone(false);
                    int jid = jobStatusRequest.getJobId();


                    int totalTasks = this.countTasks(this.toProcessMapQueue, jid);
                    totalTasks += this.countTasks(this.processingMapQueue, jid);
                    int completedTasks = this.countTasks(this.completeMapQueue, jid);
                    totalTasks += completedTasks;
                    jobStatusResponseBuilder.setTotalMapTasks(totalTasks);
                    jobStatusResponseBuilder.setNumMapTasksCompleted(completedTasks);

                    totalTasks = this.countTasks(this.toProcessReduceQueue, jid);
                    totalTasks += this.countTasks(this.processingReduceQueue, jid);
                    completedTasks = this.countTasks(this.completeReduceQueue, jid);
                    totalTasks += completedTasks;
                    jobStatusResponseBuilder.setTotalReduceTasks(0);
                    jobStatusResponseBuilder.setNumReduceTasksCompleted(0);
                }
                else {
                    jobStatusResponseBuilder.setJobDone(true);
                }
            }
        } catch (Exception e) {
            System.out.println("Problem parsing job status request??" + e.getMessage());
            e.printStackTrace();
        }
        return jobStatusResponseBuilder.build().toByteArray();
    }

    /* HeartBeatResponse heartBeat(HeartBeatRequest) */
    public byte[] heartBeat(byte[] encodedRequest) throws RemoteException {
        // Don't print, too noisy
        // System.out.println("Received heart beat!!");
        HeartBeatResponse.Builder heartBeatResponseBuilder = HeartBeatResponse.newBuilder();
        HeartBeatRequest heartBeatRequest = null;
        try {
            heartBeatRequest = HeartBeatRequest.parseFrom(encodedRequest);
            // Don't print, too noisy
            // System.out.println(heartBeatRequest.toString());

            int type;//to decide which hashmap to perform operations on

            //parsing the heart beat request
            int taskTrackerID;
            int numMapSlotsFree; 
            int numReduceSlotsFree; 
            String taskTrackerIP;
            MapTaskStatus mapTaskStatus;
            ReduceTaskStatus reduceTaskStatus;

            taskTrackerIP = heartBeatRequest.getTaskTrackerIp();

            //fetching all the complete map tasks and moving them from processing to complete queue
            for(int i = 0; i < heartBeatRequest.getMapStatusList().size(); i++) {
                mapTaskStatus = heartBeatRequest.getMapStatus(i);
                if(mapTaskStatus.getTaskCompleted()) {
                    TaskData td;
                    if((td = getFromProcessingQueue(taskTrackerIP, 1)) != null) {
                        synchronized(queueLock) {
                            rmFromProcessingQueue(taskTrackerIP, td, 1);
                            addToCompleteQueue(taskTrackerIP, td, 1);
                        }
                    }
                    System.out.println("processingMapQueue after reading heartbeat request: " + processingMapQueue.toString());
                    System.out.println("completeMapQueue after reading heartbeat request: " + completeMapQueue.toString());
                }
            }

            //fetching all the complete reduce tasks and moving them from processing to complete queue
            for(int i = 0; i < heartBeatRequest.getReduceStatusList().size(); i++) {
                reduceTaskStatus = heartBeatRequest.getReduceStatus(i);
                if(reduceTaskStatus.getTaskCompleted()) {
                    TaskData td;
                    if((td = getFromToProcessQueue(taskTrackerIP, 2)) != null) {
                        synchronized(queueLock) {
                            rmFromToProcessQueue(taskTrackerIP, td, 2);
                            addToCompleteQueue(taskTrackerIP, td, 2);
                        }
                    }
                    System.out.println("processingReduceQueue after reading heartbeat request: " + processingMapQueue.toString());
                    System.out.println("completeReduceQueue after reading heartbeat request: " + completeMapQueue.toString());
                }
            }

            taskTrackerID = heartBeatRequest.getTaskTrackerId();
            numMapSlotsFree = heartBeatRequest.getNumMapSlotsFree();
            numReduceSlotsFree = heartBeatRequest.getNumReduceSlotsFree();

            try {
                //creating the heart beat response
                heartBeatResponseBuilder.setStatus(1);
                if(!toProcessMapQueue.isEmpty()) {//currently using for map
                    type = 1;
                    TaskData taskData;
                    taskData = getFromToProcessQueue(taskTrackerIP, type);

                    //got no task for the tasktracker's ip
                    if(taskData == null) {
                        return heartBeatResponseBuilder.build().toByteArray();
                    }

                    int jobID;
                    int taskID;
                    String mapperName;
                    int blockNumber;
                    jobID = taskData.jobID;
                    taskID = taskData.taskID;
                    blockNumber = taskData.blockNumber;
                    mapperName = taskData.mapper;

                    //Building up the protobuf object
                    MapTaskInfo.Builder mapTaskInfoBuilder = MapTaskInfo.newBuilder();
                    mapTaskInfoBuilder.setJobId(jobID);
                    mapTaskInfoBuilder.setTaskId(taskID);
                    mapTaskInfoBuilder.setMapperName(mapperName);
                    mapTaskInfoBuilder.setBlockNumber(blockNumber);
                    mapTaskInfoBuilder.setIp(taskTrackerIP);

                    heartBeatResponseBuilder.addMapTasks(mapTaskInfoBuilder);

                    System.out.println("Sending task " + taskData.toString() +" to " + taskTrackerIP);
                    synchronized(queueLock) {
                        //move task from toProcess to processing queue
                        rmFromToProcessQueue(taskTrackerIP, taskData, type);
                        addToProcessingQueue(taskTrackerIP, taskData, type);
                        System.out.println("toProcessQueue after sending task with taskid: "
                                + taskID + ": " + toProcessMapQueue.toString());
                        System.out.println("processingReduceQueue after sending task with taskid: "
                                + taskID + ": " + processingMapQueue.toString());
                    }
                }
                else if(!toProcessReduceQueue.isEmpty()) { //currently using for reduce
                    type = 2;
                    int jobID;
                    int taskID;
                    String reducerName;
                    String outputFile;

                    TaskData taskData;
                    taskData = getFromToProcessQueue(taskTrackerIP, type);

                    //got no task for the tasktracker's ip
                    if(taskData == null) {
                        return heartBeatResponseBuilder.build().toByteArray();
                    }


                    //Building up the protobuf object
                    ReducerTaskInfo.Builder reducerTaskInfoBuilder = ReducerTaskInfo.newBuilder();
                    jobID = taskData.jobID;
                    taskID = taskData.taskID;
                    outputFile = taskData.output;
                    reducerName = taskData.reducer;
                    reducerTaskInfoBuilder.setJobId(jobID);
                    reducerTaskInfoBuilder.setTaskId(taskID);
                    reducerTaskInfoBuilder.setReducerName(reducerName);
                    reducerTaskInfoBuilder.setOutputFile(outputFile);

                    heartBeatResponseBuilder.addReduceTasks(reducerTaskInfoBuilder);

                    System.out.println("Sending task " + taskData.toString() +" to " + taskTrackerIP);
                    synchronized(queueLock) {
                        //move task from toProcess to processing queue
                        rmFromToProcessQueue(taskTrackerIP, taskData, type);
                        addToProcessingQueue(taskTrackerIP, taskData, type);
                        System.out.println("toProcessReduceQueue after sending task with taskid: "
                                + taskID + ": " + toProcessReduceQueue.toString());
                        System.out.println("processingReduceQueue after sending task with taskid: "
                                + taskID + ": " + processingReduceQueue.toString());
                    }
                }
                else {
                    // Don't print, too noisy.
                    // System.out.println("No task available to send back in heart beat response.");
                }
            } catch (Exception e) {
                System.out.println("Problem creating heart beat response?? " + e.getMessage());
                e.printStackTrace();
            }

        } catch (Exception e) {
            System.out.println("Problem parsing heart beat request?? " + e.getMessage());
            e.printStackTrace();
        }
        return heartBeatResponseBuilder.build().toByteArray();
    }

    public static void main(String[] args) {
        JobTracker me = null;
        try {
            me = new JobTracker(configFile);
            Naming.rebind("HDFSMRJobTracker", me);
        } catch (Exception e) {
            System.out.println("Job Tracker binding to rmi problem?? " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("Job Tracker bound to RMI Registry!! ");
    }

    public class TaskData {
        int blockNumber;
        String mapper;
        String reducer;
        String input;
        String output;
        int jobID;
        int taskID;

        public TaskData(int BlockNum,
                String Mapper,
                String Reducer,
                String Input,
                String Output,
                int Jid,
                int Tid){
            this.blockNumber = BlockNum;
            this.mapper = Mapper;
            this.reducer = Reducer;
            this.input = Input;
            this.output = Output;
            this.jobID = Jid;
            this.taskID = Tid;
        }
    }

    public class JobRunnerThread extends Thread {

        private JobTracker parentJT;
        private String mapperName = "";
        private String reducerName = "";
        private String inputFile = "";
        private String outputFile = "";
        private int numberOfReducers = 0;
        private String rendezvousIdentifier = "";

        private int JID = 0;
        private int TID = 0;
        private int numMapTasks = 0;
        private int numReduceTasks = 0;

        public JobRunnerThread(JobTracker parent, String mapper, String reducer, String input, String output, int numberReducers) {
            // A new Job has been started with the following parameters
            // A thread has been spawned to handle this job
            // Updates to this job can be monitored using a function getStatus()
            this.parentJT = parent;
            this.mapperName = mapper;
            this.reducerName = reducer;
            this.inputFile = input;
            this.outputFile = output;
            this.numberOfReducers = numberReducers;
            this.JID = this.parentJT.getAndIncrementJobID();
        }

        public int getJID() {
            return this.JID;
        }

        public String getStatus() {
            return "";
        }

        public JobTracker getParent() {
            return this.parentJT;
        }

        public String getMapper() {
            return this.mapperName;
        }

        public String getReducer() {
            return this.reducerName;
        }

        public String getInput() {
            return this.inputFile;
        }

        public String getOutput() {
            return this.outputFile;
        }

        public int getNumberOfReducers() {
            return this.numberOfReducers;
        }

        public int getNumMapTasks() {
            return this.numMapTasks;
        }

        public int getNumReduceTasks() {
            return this.numMapTasks;
        }

        public void addMapTasks(int numTasks) {
            this.numMapTasks += numTasks;
        }

        public void addReduceTasks(int numTasks) {
            this.numReduceTasks += numTasks;
        }

        public int assignTID() {
            this.TID += 1;
            return this.TID;
        }

        public void run() {
            this.open(this.inputFile, true);
            try { Thread.sleep(1000); } catch (Exception e){} // Waiting for NN to spawn the rendezvousThread
            NameNodeBlockDataNodeMappingsResponse mappings = this.getBlocks(this.inputFile);
            // Block:IPs obtained as above protobuf object
            // Using the hash map to now store blocks that it needs to process in map phase
            // Trying to fins the most even distribution for optimization
            HashMap<String, Integer> ipMapTaskCount = new HashMap<String, Integer>();
            ArrayList<String> taskIPs = new ArrayList<String>(); // Extra book keeping to make a parent check faster.
            for(int i=0; i < mappings.getMappingsCount(); i++) {
                int blockNumber  = mappings.getMappings(i).getBlockNumber();
                int IPsLength = mappings.getMappings(i).getDataNodeIPsCount();
                String IPToQueueTo = "";
                int mapTasksMin = -1;
                // Parse all IPs that have the block, get the one with the least map tasks and queue the job there.
                for(int j=0; j < IPsLength; j++) {
                    String thisIP = mappings.getMappings(i).getDataNodeIPs(j);
                    if(ipMapTaskCount.containsKey(thisIP)) {
                        int mapTasks = ipMapTaskCount.get(thisIP);
                        if(mapTasksMin == -1 || mapTasks < mapTasksMin) {
                            mapTasksMin = mapTasks;
                            IPToQueueTo = thisIP;
                        }
                    }
                    else {
                        IPToQueueTo = thisIP;
                        break;
                    }
                }
                // Got the IP that needs to be queued, add entry into our mappings
                if(ipMapTaskCount.containsKey(IPToQueueTo)) {
                    int newCount = ipMapTaskCount.get(IPToQueueTo) + 1;
                    ipMapTaskCount.put(IPToQueueTo, newCount);
                }
                else {
                    ipMapTaskCount.put(IPToQueueTo, 1);
                    taskIPs.add(IPToQueueTo);
                }
                System.out.println("Queuing: " + IPToQueueTo +" with map task, on block number: " + blockNumber);

                //add the map task to the toProcessQueue
                synchronized(queueLock) {
                    this.parentJT.addToProcessQueue(IPToQueueTo, //String
                            new TaskData(blockNumber, //int
                                this.getMapper(), //String
                                this.getReducer(), //String
                                this.getInput(), //String
                                this.getOutput(), //String
                                this.getJID(), //int
                                this.assignTID()), 1); // int
                }

                //increment the num of map tasks by 1
                this.addMapTasks(1);
            }
            
            // Looping until there are no more tasks pertaining to this JID in the ToProcessing or Processing Queue.
            boolean toProcessFlag = true;
            boolean processingFlag = true;
            while(toProcessFlag || processingFlag) {
                try { Thread.sleep(5000); } catch (Exception e){}
                if(toProcessFlag) {
                    toProcessFlag = this.parentJT.inToProcessQueue(this.getJID(), taskIPs);
                }
                else if(processingFlag) {
                    processingFlag = this.parentJT.inProcessingQueue(this.getJID(), taskIPs);
                }
            }
            this.close();
            this.parentJT.removeJobRunnerFromJRList(this.JID);
        }

        public NameNodeBlockDataNodeMappingsResponse getBlocks(String fileName) {
            byte[] responseEncoded = null;
            NameNodeBlockDataNodeMappingsResponse nameNodeBlockDataNodeMappingsResponse = null;
            NameNodeBlockDataNodeMappingsRequest.Builder nameNodeBlockDataNodeMappingsRequest = NameNodeBlockDataNodeMappingsRequest.newBuilder();
            nameNodeBlockDataNodeMappingsRequest.setFileName(fileName);
            try {
                RendezvousRunnableInterface rendezvous = (RendezvousRunnableInterface) Naming.lookup("//" +
                        this.parentJT.nameNodeIP + "/" + this.rendezvousIdentifier);
                responseEncoded = rendezvous.getNameNodeBlockDataNodeMappings(
                        nameNodeBlockDataNodeMappingsRequest.build().toByteArray());

            } catch (Exception e) {
                System.out.println("Connecting to NN for get file problem?? " + e.getMessage());
                e.printStackTrace();
            }
            try {
                nameNodeBlockDataNodeMappingsResponse = NameNodeBlockDataNodeMappingsResponse.parseFrom(responseEncoded);
                System.out.println("Got response " + nameNodeBlockDataNodeMappingsResponse.toString());
            } catch (Exception e) {
                System.out.println("Parsing get response problem?? " + e.getMessage());
                e.printStackTrace();
            }
            return nameNodeBlockDataNodeMappingsResponse;
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
                        this.parentJT.nameNodeIP + "/HDFSNameNode"); // This name node request location is hard coded.
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

            return true;
        }

        public boolean close() {
            try {
                RendezvousRunnableInterface rendezvous = (RendezvousRunnableInterface) Naming.lookup("//" +
                        this.parentJT.nameNodeIP + "/" + this.rendezvousIdentifier);
                rendezvous.closeFile();

            } catch (Exception e) {
                System.out.println("Connecting to HDFS for close file problem?? " + e.getMessage());
                e.printStackTrace();
            }
            return true;
        }
    }
}
