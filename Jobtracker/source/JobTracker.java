import com.distributed.systems.MRProtos.HeartBeatRequest;
import com.distributed.systems.MRProtos.HeartBeatResponse;
import com.distributed.systems.MRProtos.JobStatusRequest;
import com.distributed.systems.MRProtos.JobStatusResponse;
import com.distributed.systems.MRProtos.JobSubmitRequest;
import com.distributed.systems.MRProtos.JobSubmitResponse;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

public class JobTracker extends UnicastRemoteObject implements JobTrackerInterface {

    // Using this default config file
    private static String configFile = "hdfs_mr_job_tracker.conf";

    private static int currentJobID = 0;

    private static HashMap<Integer, JobRunnerThread> currentJobThreads = new HashMap<Integer, JobRunnerThread>();


    private final Object CJTLock = new Object();

    public JobTracker(String conf) throws RemoteException {
        this.configFile = conf;
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
            try { Thread.sleep(10000); } catch (Exception e){}
            this.parentJT.removeJobRunnerFromJRList(this.JID);
        }
    }
}
