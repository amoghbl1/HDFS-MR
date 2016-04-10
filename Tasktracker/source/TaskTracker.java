import com.distributed.systems.MRProtos.HeartBeatRequest;
import com.distributed.systems.MRProtos.HeartBeatResponse;
import com.distributed.systems.MRProtos.MapTaskStatus;
import com.distributed.systems.MRProtos.ReduceTaskStatus;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;

public class TaskTracker {

    private int BLOCK_SIZE_IN_BYTES = 32000000;

    // Using hdfs_data_node.conf as the default config file.
    private static String configFile = "hdfs_mr_task_tracker.conf";

    // This is parsed from the config file on object creation
    private static String jobTrackerIP;
    private static String myIP;
    private static int myNumMapSlotsFree;
    private static int myNumReduceSlotsFree;
    private static int myID;
    private static MapTaskStatus myMapTaskStatus;
    private static ReduceTaskStatus myReduceTaskStatus;
    private static int myIndex;

    private boolean dirtyBit = false;

    public boolean getDirtyBit() {
        return this.dirtyBit;
    }

    public MapTaskStatus getMapTaskStatus() {
    	return this.myMapTaskStatus;
    }

    public ReduceTaskStatus getReduceTaskStatus() {
    	return this.myReduceTaskStatus;
    }

    public void setDirtyBit(boolean bit) {
        this.dirtyBit = bit;
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

    public int getMyIndex() {
    	return this.myIndex;
    }

    public TaskTracker(String conf) throws RemoteException{
	this.myNumMapSlotsFree = 1;//change
	this.myNumReduceSlotsFree = 1;//change
	this.myIndex = myIndex;//what is myIndex?
	//this.myMapTaskStatus = getMapStatus();
	//this.myReduceTaskStatus = getReduceStatus();
        this.configFile = conf;
        // Lets just upload once on startup.
        this.dirtyBit = true;

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
            if(configLine.startsWith("myIP")) {
                this.myIP = configLine.split(" ")[1];
            }
            if(configLine.startsWith("myID")) {
                this.myID = Integer.parseInt(configLine.split(" ")[1]);
            }
        } 
    }

    static class HeartBeatThread extends Thread {

        private static TaskTracker parentNode;
        private static int HEART_BEAT_TIME = 1000;

        public HeartBeatThread(TaskTracker parent) {
            this.parentNode = parent;
        }

        public void run() {
            byte[] responseEncoded = null;
            while(true) {
                try {
                    String jobTrackerIP = parentNode.getJobTrackerIP();
                    HeartBeatRequest.Builder heartBeatRequestBuilder = HeartBeatRequest.newBuilder();
                    heartBeatRequestBuilder.setTaskTrackerId(parentNode.getMyID());
                    heartBeatRequestBuilder.setNumMapSlotsFree(parentNode.getNumMapSlotsFree());
                    heartBeatRequestBuilder.setNumReduceSlotsFree(parentNode.getNumReduceSlotsFree());
                    heartBeatRequestBuilder.setMapStatus(parentNode.getMyIndex(), parentNode.getMapTaskStatus());
                    heartBeatRequestBuilder.setReduceStatus(parentNode.getMyIndex(), parentNode.getReduceTaskStatus());

                    JobTrackerInterface jobtracker = (JobTrackerInterface) Naming.lookup("//" +
                            jobTrackerIP + "/HDFSMRJobTracker");

                    responseEncoded = jobtracker.heartBeat(
                            heartBeatRequestBuilder.build().toByteArray());

                    // Do some stuff with the response maybe.
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

}
