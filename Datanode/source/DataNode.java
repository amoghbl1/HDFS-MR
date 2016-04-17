import com.distributed.systems.HDFSProtos.DataNodeBlockMapping;
import com.distributed.systems.HDFSProtos.DataNodeBlockMappings;
import com.distributed.systems.HDFSProtos.DataNodeReportMapping;
import com.distributed.systems.HDFSProtos.DataNodeReportMappings;
import com.distributed.systems.HDFSProtos.HeartBeatRequest;
import com.distributed.systems.HDFSProtos.HeartBeatResponse;
import com.distributed.systems.HDFSProtos.ReadBlockRequest;
import com.distributed.systems.HDFSProtos.ReadBlockResponse;
import com.distributed.systems.HDFSProtos.WriteBlockRequest;
import com.distributed.systems.HDFSProtos.WriteBlockResponse;
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

public class DataNode extends UnicastRemoteObject implements DataNodeInterface {


    private int BLOCK_SIZE_IN_BYTES = 32000000;

    // Using hdfs_data_node.conf as the default config file.
    private static String configFile = "hdfs_data_node.conf";
    private static String blocksPbuf = "data_node_blocks.pbuf";

    // This is parsed from the config file on object creation
    private static String nameNodeIP;
    private static String myIP;
    private static int myID;

    private boolean dirtyBit = false;

    public boolean getDirtyBit() {
        return this.dirtyBit;
    }

    public void setDirtyBit(boolean bit) {
        this.dirtyBit = bit;
    }

    public String getNameNodeIP() {
        return this.nameNodeIP;
    }

    public String getMyIP() {
        return this.myIP;
    }

    public int getMyID() {
        return this.myID;
    }

    public DataNode(String conf) throws RemoteException{
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

    public synchronized void writeToBlockPbuf(int blockNumber, String blockFilePath) {
        System.out.println("writing to block pbuf!!");
        DataNodeBlockMappings.Builder updatedMappingsBuilder = DataNodeBlockMappings.newBuilder();
        try {
            updatedMappingsBuilder = DataNodeBlockMappings.newBuilder(
                    DataNodeBlockMappings.parseFrom(
                        new FileInputStream(this.blocksPbuf)));

        } catch (FileNotFoundException e) {
            // Looks like this is the first run, and there is no metadata
            // handling that case
            updatedMappingsBuilder = DataNodeBlockMappings.newBuilder();
        } catch (IOException e) {
            // Might be able to handle the IOException better.
            System.out.println("Problem parsing the data node block mappings?? " + e.getMessage());
            e.printStackTrace();
        }   
        try {
            DataNodeBlockMapping.Builder newMappingBuilder = DataNodeBlockMapping.newBuilder();
            newMappingBuilder.setBlockNumber(blockNumber);
            newMappingBuilder.setLocalPath(blockFilePath);
            updatedMappingsBuilder.addMappings(newMappingBuilder);
            updatedMappingsBuilder.build().writeTo(
                    new FileOutputStream(this.blocksPbuf));
        } catch (Exception e) {
            System.out.println("Problem parsing the data node block mappings?? " + e.getMessage());
            e.printStackTrace();
        }
    }

    public synchronized ArrayList<Integer> getAllStoredBlocks() {
        DataNodeBlockMappings dataNodeBlockMappings = null;
        ArrayList<Integer> blocks = new ArrayList<Integer>();
        try {       
            dataNodeBlockMappings = DataNodeBlockMappings.parseFrom(
                    new FileInputStream(this.blocksPbuf));
        } catch (Exception e) {
            System.out.println("Problem parsing the data node block mappings?? " + e.getMessage());
            e.printStackTrace();
            return blocks;
        }
        for(int i=0; i < dataNodeBlockMappings.getMappingsCount(); i++) {
            blocks.add(new Integer(
                        dataNodeBlockMappings.getMappings(i).getBlockNumber()));
        }
        return blocks;
    }

    /* ReadBlockResponse readBlock(ReadBlockRequest)) */
    /* Method to read data from any block given block-number */
    public synchronized byte[] readBlock(byte[] request) throws RemoteException {
        int blockNumber = -1;
        byte[] blockBytes = new byte[ this.BLOCK_SIZE_IN_BYTES ];
        byte[] blockToSend = null;
        int readSize;
        String blockPath = "/dev/null";
        ReadBlockResponse.Builder readBlockResponseBuilder = ReadBlockResponse.newBuilder();
        try {
            ReadBlockRequest readBlockRequest = ReadBlockRequest.parseFrom(request);
            blockNumber = readBlockRequest.getBlockNumber();
            System.out.println("Read block called for:" + blockNumber);
        } catch (Exception e) {
            System.out.println("Problem in read block request parsing?? " + e.getMessage());
            e.printStackTrace();
        }
        // Parse my local file to see if I have the block and then return it

        DataNodeBlockMappings dataNodeBlockMappings = null;
        try {
            dataNodeBlockMappings = DataNodeBlockMappings.parseFrom(
                    new FileInputStream(this.blocksPbuf));
        } catch (Exception e) {
            // Might be able to handle the IOException better.
            System.out.println("Problem parsing the data node block mappings?? " + e.getMessage());
            e.printStackTrace();
        }
        for(int i=0; i < dataNodeBlockMappings.getMappingsCount() ; i++) {
            if(dataNodeBlockMappings.getMappings(i).getBlockNumber() == blockNumber) {
                blockPath = dataNodeBlockMappings.getMappings(i).getLocalPath();
            }
        }

        try {
            FileInputStream fileInputStream = new FileInputStream(blockPath);
            readSize = fileInputStream.read(blockBytes);
            fileInputStream.close();
            if(readSize != BLOCK_SIZE_IN_BYTES) {
                blockToSend = new byte[readSize];
                for(int i=0; i < readSize; i++)
                    blockToSend[i] = blockBytes[i];
            } else {
                blockToSend = blockBytes;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return blockToSend;
    }

    /* WriteBlockResponse writeBlock(WriteBlockRequest) */
    /* Method to write data to a specific block */
    public synchronized byte[] writeBlock(byte[] protobuf, byte[] bytes) throws RemoteException {
        System.out.println("Write block called.");
        WriteBlockRequest writeBlockRequest;
        WriteBlockResponse.Builder writeBlockResponseBuilder = WriteBlockResponse.newBuilder();

        int blockNumber = -1;
        ArrayList<String> remainingDataNodeIPs = null;
        boolean startThreadFlag = false;

        // This is the location where the block is written.
        // Might wanna make this absolute path
        String blockNumberFile = "blocks/";

        try {
            writeBlockRequest = WriteBlockRequest.parseFrom(protobuf);
            blockNumber = writeBlockRequest.getBlockNumber();

            blockNumberFile = blockNumberFile + "" + blockNumber;

            remainingDataNodeIPs = new ArrayList<String>(writeBlockRequest.getRemainingDataNodeIPsCount());
            for(int i=0; i < writeBlockRequest.getRemainingDataNodeIPsCount(); i++) {
                startThreadFlag = true;
                remainingDataNodeIPs.add(writeBlockRequest.getRemainingDataNodeIPs(i));
            }
        } catch (Exception e) {
            System.out.println("Problem parsing write block request?? " + e.getMessage());
            e.printStackTrace();
        }
        try {
            System.out.println("Writing bloc number " + blockNumber + " to: " + blockNumberFile);
            FileOutputStream fileOutputStream = new FileOutputStream(blockNumberFile); // Using file name as block number as of now.
            fileOutputStream.write(bytes);
            fileOutputStream.close();
        } catch (Exception e) {
            System.out.println("Problem writing block to file?? " + e.getMessage());
            e.printStackTrace();
        }

        // Done with writing bytes locally
        // Handle writing on the remaining blocks in another thread

        if(startThreadFlag) {
            System.out.println("Starting another thread!! ");
            new Thread(new RedundancyWriterRunnable(
                        blockNumber,
                        bytes,
                        remainingDataNodeIPs)).start();
        }

        // Last, we update our local block <--> filename mappings

        writeToBlockPbuf(blockNumber, blockNumberFile);

        // Setting dirtyBit for DNReport purposes
        this.dirtyBit = true;

        return writeBlockResponseBuilder.setStatus(1)
            .build().toByteArray();
    }

    public static void main(String[] args) {
        DataNode me = null;
        try {
            me = new DataNode(configFile);
            Naming.rebind("HDFSDataNode", me);
        } catch (Exception e) {
            System.out.println("Data Node binding to rmi problem?? " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("Data Node bound to RMI Registry!! ");

        new DNReportLooperThread(me).start();
        System.out.println("DN Report Looper started.");
        new HeartBeatThread(me).start();
        System.out.println("Hear Beat Thread started.");
    }

    public class RedundancyWriterRunnable implements Runnable {

        // The request this class is going to send is going to look exactly like the one it receives but to the 
        // top of the list of IPs to send to, and remove that IP from the list that it sends along

        private byte[] blockToSend;
        private ArrayList<String> IPsRemaining;
        private int blockNumber;

        public RedundancyWriterRunnable(int blocknumber, byte[] data, ArrayList<String> ips) { 
            this.blockNumber = blocknumber;
            this.blockToSend = data;
            this.IPsRemaining = ips;
        }

        public void run() {
            WriteBlockResponse writeBlockResponse = null;
            byte[] responseEncoded = null;
            try {
                WriteBlockRequest.Builder writeBlockRequestBuilder = WriteBlockRequest.newBuilder();
                writeBlockRequestBuilder.setBlockNumber(this.blockNumber);

                // Append IPs from 1 to the end;
                String ipToSend = IPsRemaining.get(0);

                for(int i=1; i < IPsRemaining.size(); i++)
                    writeBlockRequestBuilder.addRemainingDataNodeIPs(IPsRemaining.get(i));

                DataNodeInterface datanode = (DataNodeInterface) Naming.lookup("//" +
                        ipToSend + "/HDFSDataNode");

                responseEncoded = datanode.writeBlock(writeBlockRequestBuilder.build().toByteArray(), this.blockToSend);
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

        }
    }
    static class DNReportLooperThread extends Thread {
        
        DataNode parentNode = null;

        int DN_REPORT_SLEEP_TIME = 5000;
        int MAX_DN_REPORT_SLEEP_TIME = 10000;

        public DNReportLooperThread(DataNode parent) {
            this.parentNode = parent;
        }

        public void run() {
            int sleepTime = DN_REPORT_SLEEP_TIME;
            byte[] responseEncoded;
            while(true) {
                if(parentNode.getDirtyBit()) {
                    String nameNodeIP = parentNode.getNameNodeIP();
                    ArrayList<Integer> blocks = parentNode.getAllStoredBlocks();
                    DataNodeReportMappings.Builder dataNodeReportMappingsBuilder = DataNodeReportMappings.newBuilder();

                    for(Integer i : blocks) {
                        DataNodeReportMapping.Builder dataNodeReportMappingBuilder = DataNodeReportMapping.newBuilder();
                        dataNodeReportMappingBuilder.setBlockNumber(i);
                        dataNodeReportMappingBuilder.setDataNodeIP(parentNode.getMyIP());

                        dataNodeReportMappingsBuilder.addMappings(dataNodeReportMappingBuilder);
                    }

                    System.out.println("Uploading DN Report to " + nameNodeIP);

                    try {
                        NameNodeInterface namenode = (NameNodeInterface) Naming.lookup("//" +
                                nameNodeIP + "/HDFSNameNode");

                        responseEncoded = namenode.blockReport(
                                dataNodeReportMappingsBuilder.build().toByteArray());

                        // Do some stuff with the response maybe.
                    } catch (Exception e) {
                        System.out.println("Problem uploading DN Report?? " + e.getMessage());
                        e.printStackTrace();
                    }

                    sleepTime = DN_REPORT_SLEEP_TIME;
                    System.out.println("Setting sleep time back to default! ");
                    if(parentNode.getDirtyBit()) {
                        parentNode.setDirtyBit(false);
                    }
                } else {
                    if(sleepTime < MAX_DN_REPORT_SLEEP_TIME)
                        sleepTime = sleepTime * 2;
                    System.out.println("This DN hasn't been used, increasing thread sleep to: " + sleepTime);
                    // Delay sleep time optimization.
                    // If this node isn't being used, theres no point in rechecking every 5 seconds!
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (Exception e) {
                    System.out.println("Thread Interrupted?? " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }

    static class HeartBeatThread extends Thread {

        private static DataNode parentNode;
        private static int HEART_BEAT_TIME = 1000;

        public HeartBeatThread(DataNode parent) {
            this.parentNode = parent;
        }

        public void run() {
            byte[] responseEncoded = null;
            while(true) {
                try {
                    String nameNodeIP = parentNode.getNameNodeIP();
                    HeartBeatRequest.Builder heartBeatRequestBuilder = HeartBeatRequest.newBuilder();
                    heartBeatRequestBuilder.setId(parentNode.getMyID());
                    heartBeatRequestBuilder.setIp(parentNode.getMyIP());

                    NameNodeInterface namenode = (NameNodeInterface) Naming.lookup("//" +
                            nameNodeIP + "/HDFSNameNode");

                    responseEncoded = namenode.heartBeat(
                            heartBeatRequestBuilder.build().toByteArray());

                    // Do some stuff with the response maybe.
                } catch (Exception e) {
                    // Uploading DN Report if my NN comes back up.
                    this.parentNode.setDirtyBit(true);
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
