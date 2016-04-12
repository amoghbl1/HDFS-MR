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
import java.io.BufferedReader;
import java.io.FileReader;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;

public class JobTracker extends UnicastRemoteObject implements JobTrackerInterface {

    // Using this default config file
    private static String configFile = "hdfs_mr_job_tracker.conf";

    private static String nameNodeIP = "127.0.0.1";

    private static int currentJobID = 0;

    private static HashMap<Integer, JobRunnerThread> currentJobThreads = new HashMap<Integer, JobRunnerThread>();


    private final Object CJTLock = new Object();

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
                    jobStatusResponseBuilder.setTotalMapTasks(0);
                    jobStatusResponseBuilder.setNumMapTasksStarted(0);
                    jobStatusResponseBuilder.setTotalReduceTasks(0);
                    jobStatusResponseBuilder.setNumReduceTasksStarted(0);
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
        System.out.println("Received heart beat!!");
        HeartBeatRequest heartBeatRequest = null;
        try {
            heartBeatRequest = HeartBeatRequest.parseFrom(encodedRequest);
            System.out.println(heartBeatRequest.toString());
        } catch (Exception e) {
            System.out.println("Problem parsing heart beat request??" + e.getMessage());
            e.printStackTrace();
        }
        return null;
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


    public class JobRunnerThread extends Thread {

        private JobTracker parentJT;
        private String mapperName = "";
        private String reducerName = "";
        private String inputFile = "";
        private String outputFile = "";
        private int numberOfReducers = 0;
        private String rendezvousIdentifier = "";

        private int JID = 0;

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

        public String getStatus() {
            return "";
        }

        public int getJID() {
            return this.JID;
        }

        public void run() {
            this.open(this.inputFile, true);
            try { Thread.sleep(1000); } catch (Exception e){} // Waiting for NN to spawn the rendezvousThread
            NameNodeBlockDataNodeMappingsResponse mappings = this.getBlocks(this.inputFile);
            // Block:IPs obtained as above protobuf object
            // Using the hash map to now store blocks that it needs to process in map phase
            // Trying to fins the most even distribution for optimization
            HashMap<String, Integer> ipMapTaskCount = new HashMap<String, Integer>();
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
                }
                System.out.println("Queuing: " + IPToQueueTo +" with map task, on block number: " + blockNumber);
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
