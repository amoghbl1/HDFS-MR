import com.distributed.systems.MRProtos.BlockLocations;
import com.distributed.systems.MRProtos.DataNodeLocation;
import com.distributed.systems.MRProtos.HeartBeatRequest;
import com.distributed.systems.MRProtos.HeartBeatResponse;
import com.distributed.systems.MRProtos.MapTaskInfo;
import com.distributed.systems.MRProtos.MapTaskStatus;
import com.distributed.systems.MRProtos.ReducerTaskInfo;
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
import java.util.HashMap;

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
    private static HashMap<Integer, Thread> mapHashMap = new HashMap<Integer, Thread>();
    private static HashMap<Integer, Thread> reduceHashMap = new HashMap<Integer, Thread>();

    public void addToHashMap(int taskID, Thread thread, int type) {
	if(type == 1) {//map task
	    mapHashMap.put(taskID, thread);
	}
	else if(type == 2) {//reduce task
	    reduceHashMap.put(taskID, thread);
	}
    }

    public void removeFromHashMap(int taskID, int type) {
	if(type == 1) {//map task
	    mapHashMap.remove(taskID);
	}
	else if(type == 2) {//reduce task
	    reduceHashMap.remove(taskID);
	}
    }

    public Thread getFromHashMap(int taskID, int type) {
	if(type == 1) {//map task
	   if(mapHashMap.containsKey(taskID)) {
	       return  mapHashMap.get(taskID);
	   }
	   else {
	       return null;
	   }
	}
	//else it is a reduce task
	if(!reduceHashMap.containsKey(taskID)) {
            return  reduceHashMap.get(taskID);
	}
	return null;
    }

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
                    //heartBeatRequestBuilder.setMapStatus(parentTT.getMyIndex(), parentTT.getMapTaskStatus());
                    //heartBeatRequestBuilder.setReduceStatus(parentTT.getMyIndex(), parentTT.getReduceTaskStatus());

                    JobTrackerInterface jobtracker = (JobTrackerInterface) Naming.lookup("//" +
                            jobTrackerIP + "/HDFSMRJobTracker");

                    responseEncoded = jobtracker.heartBeat(
                            heartBeatRequestBuilder.build().toByteArray());
		    try{

			// Parse the Heart Beat Response and spawn a new thread with that data
			heartBeatResponse = HeartBeatResponse.parseFrom(responseEncoded);

			if(heartBeatResponse.getStatus() != 0) {
			    System.out.println(heartBeatResponse.toString());
			    if(true){//testing-if(heartBeatResponse.getMapTasksList().size() != 0) {
			        System.out.println("Map Task(s) Received");

				int jobID;
			        int taskID;
			        int port;
			        int blockNumber;
				String mapperName;
				String ip;
				MapTaskInfo mapTask;
				BlockLocations inputBlock;
				DataNodeLocation location;

			        for(int i = 0; i < heartBeatResponse.getMapTasksList().size(); i++) {
				    mapTask = heartBeatResponse.getMapTasks(i);
				    jobID = mapTask.getJobId();
				    taskID = mapTask.getTaskId();
				    mapperName = mapTask.getMapperName();
				    inputBlock = mapTask.getInputBlock();
				    blockNumber = inputBlock.getBlockNumber();
				    for(int j = 0; j < inputBlock.getLocationsList().size(); j++) {
					location = inputBlock.getLocations(j);
					ip = location.getIp();
					port = location.getPort();

					System.out.println("Starting a map thread!! ");

					Runnable r  = new MapThreadRunnable(jobID, //int
									taskID, //int
									mapperName, //String
									blockNumber, //int
									ip, //String
									port);//int
					Thread th = new Thread(r);
					th.start();
					parentTT.addToHashMap(taskID, th, 1);
					//decrease the num of free map slots
					parentTT.myNumMapSlotsFree--;
				    }
				}
			    }

			    else if(heartBeatResponse.getReduceTasksList().size() != 0) {
			        System.out.println("Reduce Task(s) Received");

				int jobID;
				int taskID;
				String reducerName;
			        String mapOutputFile;
			        String outputFile;
				ReducerTaskInfo reduceTaskInfo;

			        for(int i = 0; i < heartBeatResponse.getReduceTasksList().size(); i++) {
				    reduceTaskInfo = heartBeatResponse.getReduceTasks(i);
				    jobID = reduceTaskInfo.getJobId();
				    taskID = reduceTaskInfo.getTaskId();
				    reducerName = reduceTaskInfo.getReducerName();
				    outputFile = reduceTaskInfo.getOutputFile();
				    for(int j = 0; j < reduceTaskInfo.getMapOutputFilesList().size(); j++) {
					mapOutputFile = reduceTaskInfo.getMapOutputFiles(j);

					System.out.println("Starting a reduce thread!! ");
					System.out.println("With Values: ");
					System.out.println("jobid: " + jobID 
							+ "taskid: " + taskID 
							+ "reducername: " + reducerName 
							+ "mapOutputFile: " + mapOutputFile 
							+ "outputFile: " + outputFile); 

					Runnable r  = new ReduceThreadRunnable(jobID, //int
									taskID, //int
									reducerName, //String
									mapOutputFile, //String
									outputFile); //String
					Thread th = new Thread(r);
					th.start();
					parentTT.addToHashMap(taskID, th, 2);
					//decrease the num of free reduce slots
					parentTT.myNumReduceSlotsFree--;
				    }
				}
				    }
			    else {
			        System.out.println("No Task Received");
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
	System.out.println("Hear Beat Thread Started.");
    }

    public static class MapThreadRunnable implements Runnable {
        
	int jobID;
	int taskID;
	int port;
	int blockNumber;
	String mapperName;
	String ip;

        public MapThreadRunnable(int jobId, int taskId, String mapperNam, 
                int blockNo, String ipAddr, int portNo) throws RemoteException{
            this.jobID = jobId;
            this.taskID = taskId;
            this.mapperName = mapperNam;
            this.blockNumber = blockNo;
            this.ip = ipAddr;
            this.port = portNo;
        }
        public void run() {
		System.out.println("Map Thread Runnable printing hello... ");
	}
    }

    public static class ReduceThreadRunnable implements Runnable {
        
	int jobID;
	int taskID;
	String reducerName;
	String mapOutputFile;
	String outputFile;

        public ReduceThreadRunnable(int jobId, int taskId, String reducerNam, 
                String mapOpFile, String opFile) throws RemoteException{
            this.jobID = jobId;
            this.taskID = taskId;
            this.reducerName = reducerNam;
            this.mapOutputFile = mapOpFile;
            this.outputFile = opFile;
        }
        public void run() {
		System.out.println("Reduce Thread Runnable printing hello... ");
	}
    }

}
