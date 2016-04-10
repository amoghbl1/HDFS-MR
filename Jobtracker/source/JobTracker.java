public class JobTracker extends UnicastRemoteObject implements JobTrackerInterface {

    // Using this default config file
    private static String configFile = "hdfs_mr_job_tracker.conf";


    public JobTracker(String conf) throws RemoteException {
        this.configFile = conf;
    }

    /* JobSubmitResponse jobSubmit(JobSubmitRequest) */
    byte[] jobSubmit(byte[] encodedRequest) throws RemoteException {
        return null;
    }

    /* JobStatusResponse getJobStatus(JobStatusRequest) */
    byte[] getJobStatus(byte[] encodedRequest) throws RemoteException {
        return null;
    }

    /* HeartBeatResponse heartBeat(HeartBeatRequest) */
    byte[] heartBeat(byte[] encodedRequest) throws RemoteException {
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
}
