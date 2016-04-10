import com.distributed.systems.HDFSProtos.AssignBlockResponse;
import com.distributed.systems.HDFSProtos.HeartBeatRequest;
import com.distributed.systems.HDFSProtos.HeartBeatResponse;
import com.distributed.systems.HDFSProtos.ListFilesRequest;
import com.distributed.systems.HDFSProtos.ListFilesResponse;
import com.distributed.systems.HDFSProtos.NameNodeBlockDataNodeMapping;
import com.distributed.systems.HDFSProtos.NameNodeBlockDataNodeMappings;
import com.distributed.systems.HDFSProtos.NameNodeBlockDataNodeMappingsRequest;
import com.distributed.systems.HDFSProtos.NameNodeBlockDataNodeMappingsResponse;
import com.distributed.systems.HDFSProtos.NameNodeFileMapping;
import com.distributed.systems.HDFSProtos.NameNodeFileMappings;
import com.distributed.systems.HDFSProtos.OpenFileRequest;
import com.distributed.systems.HDFSProtos.OpenFileResponse;
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
import java.util.List;

public class NameNode extends UnicastRemoteObject implements NameNodeInterface {

    // Using hdfs_name_node.conf as the default config file.
    private static String configFile = "hdfs_name_node.conf";
    private static String filesPbuf = "name_node_files.pbuf";
    private final Object filesPbufLock = new Object();
    private static String blocksPbuf = "name_node_blocks_to_data_nodes.pbuf";
    private final Object blocksPbufLock = new Object();

    private List<String> openFilesList = null;
    // Note: We use pbuf as a protobuf data file, name node has a few of these required for its
    // functionality and it can be configured with the cofig file

    private List<String> dataNodeList = new ArrayList<String>();

    private int currentBlockNumber = 0;

    public NameNode(String configFile) throws RemoteException {
        this.configFile = configFile;
        // Based on this config file, we do a bunch of stuff and set our variables for this object
        readMetaData();
        // Based on all the meta data, we set some values, like:
        //  currentBlockNumber

        // Parsing config file here
        BufferedReader fileReader = null;
        String configLine;
        // dataNodeList = new ArrayList<String>();
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
            } // You can either configure nodes like this, or when the heart beat is heart, they get added to the list!
            if(configLine.startsWith("dataNodeIP")) {
                dataNodeList.add(new String (configLine.split(" ")[1]));
            }
        }
    }

    private void readMetaData() {
        // Set some vals here


    }

    /* ListFilesResponse list(ListFilesRequest) */
    /* List the file names (no directories needed for current implementation */
    public byte[] list(byte[] encodedRequest) throws RemoteException{
        
        ListFilesRequest listFilesRequest;
        ListFilesResponse.Builder listFilesResponseBuilder = ListFilesResponse.newBuilder();
        NameNodeFileMappings.Builder updatedMappingsBuilder = NameNodeFileMappings.newBuilder();;

        NameNodeFileMappings nameNodeFileMappings = null;
        try {
            listFilesRequest = ListFilesRequest.parseFrom(encodedRequest);
        } catch (Exception e) {
            System.out.println("Problem parsing list request?? " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("List working!");

        // Right now, we just return null, we need to build a response list, based on the files we have
        // We do this by first parsing the Files.pbuf file and getting all the file names.

        try {
            nameNodeFileMappings = NameNodeFileMappings.parseFrom(
                    new FileInputStream(this.filesPbuf));

        } catch (FileNotFoundException e) {
            // Looks like this is the first run, and there is no metadata
            // handling that case
            updatedMappingsBuilder = NameNodeFileMappings.newBuilder();
        } catch (IOException e) {
            // Might be able to handle the IOException better.
            System.out.println("Problem parsing the name node file mappings?? " + e.getMessage());
            e.printStackTrace();
        }

        if(nameNodeFileMappings != null) {
            for(int i=0; i < nameNodeFileMappings.getMappingsCount(); i++) {
                listFilesResponseBuilder.addFileNames(
                        (nameNodeFileMappings.getMappings(i)).getFileName());
            }
        }
        return listFilesResponseBuilder.setStatus(1)
            .build().toByteArray();


    }

    /* OpenFileResponse openFile(OpenFileRequest) */
    /* Method to open a file given file name with read-write flag*/
    public byte[] openFile(byte[] encodedRequest) throws RemoteException {
        // Open file is basically starting a rendezvous for the file and returning details for it.
        // A list of already open files is maintained to make sure that two people can't open the same file at the same time.

        OpenFileRequest openFileRequest;
        OpenFileResponse.Builder openFileResponseBuilder = OpenFileResponse.newBuilder();;

        String fileName = "";
        boolean forRead; // true, for read; false for write

        try {
            openFileRequest = OpenFileRequest.parseFrom(encodedRequest);
            fileName = openFileRequest.getFileName();
            forRead = openFileRequest.getForRead();
        } catch (Exception e) {
            System.out.println("Problem parsing open file request?? " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("Open File called for " + fileName);
        // After getting all the data from the request, we make the object and repond with the 
        // pointer to the rendezvous node.
        if(openFilesList == null) {
            openFilesList = new ArrayList<String>();
        } else {
            for(String s : openFilesList) {
                if(s.equals(fileName)) {
                    System.out.println("Trying to open already opened file!");
                    openFileResponseBuilder.setRendezvousIndentifier("");
                    openFileResponseBuilder.setStatus(1);
                    return null;
                }
            }
        } // This checks if we have that file already opened, maybe we can handle this differently, we return null for now.
        openFilesList.add(fileName);

        System.out.println("Added file to list.");
        new Thread(new RendezvousRunnable(
                    fileName, // String filename, 
                    this, // NameNode node, 
                    "RendezVous" + fileName, // String identifier,
                    true, // boolean readperm, 
                    true)).start(); // boolean writeperm));
        System.out.println("got thread object");
        openFileResponseBuilder.setRendezvousIndentifier("RendezVous" + fileName);
        openFileResponseBuilder.setStatus(1);
        System.out.println("Started thread for rendezvous");
        return openFileResponseBuilder.build().toByteArray();
    }

    public synchronized void removeFileFromFilesList(String name) {
        for(int i=0; i < this.openFilesList.size(); i++) {
            String fileName = this.openFilesList.get(i);
            if(fileName.equals(name)) {
                this.openFilesList.remove(i);
            }
        }
    }

    public synchronized int getAndIncrementBlockNumber() {
        this.currentBlockNumber += 1;
        return this.currentBlockNumber - 1;
    }

    public void writeToBlocksPbuf(int blockNumber, ArrayList<String> listOfDataNodes) {
        NameNodeBlockDataNodeMappings.Builder updatedMappingsBuilder = NameNodeBlockDataNodeMappings.newBuilder();
        NameNodeBlockDataNodeMappings oldMappings = this.getNameNodeBlockDataNodeMappings();
        try {
            NameNodeBlockDataNodeMapping.Builder newMappingBuilder = NameNodeBlockDataNodeMapping.newBuilder();
            newMappingBuilder.setBlockNumber(blockNumber);
            for(String s : listOfDataNodes) {
                newMappingBuilder.addDataNodeIPs(s);
            }
            updatedMappingsBuilder.addMappings(newMappingBuilder);
            if(oldMappings != null) {
                int mappingsCount = oldMappings.getMappingsCount();
                for(int i=0; i < mappingsCount; i++) {
                    int mappingBlockNumber = oldMappings.getMappings(i).getBlockNumber();
                    if(mappingBlockNumber == blockNumber) {
                        continue;
                    } else {
                        updatedMappingsBuilder.addMappings(oldMappings.getMappings(i));
                    }
                }
            }
            synchronized(blocksPbufLock){
                updatedMappingsBuilder.build().writeTo(
                        new FileOutputStream(this.blocksPbuf));
            }
        } catch (Exception e) {
            System.out.println("Problem parsing the name node file mappings?? " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Gets the mappings from the default file
    public NameNodeBlockDataNodeMappings getNameNodeBlockDataNodeMappings(){ 
        NameNodeBlockDataNodeMappings returnMappings = null;
        synchronized(blocksPbufLock){
            try {
                returnMappings = NameNodeBlockDataNodeMappings.parseFrom(
                        new FileInputStream(this.blocksPbuf));
            }
            catch (FileNotFoundException e) {
                // Looks like this is the first run, and there is no metadata
                // handling that case
            } catch (IOException e) {
                // Might be able to handle the IOException better.
                System.out.println("Problem parsing the blocks to data nodes file mappings?? " + e.getMessage());
                e.printStackTrace();
            }
        }
        return returnMappings;
    }

    public void writeToFilesPbuf(String filename, ArrayList<Integer> listOfBlocks) {
        NameNodeFileMappings.Builder updatedMappingsBuilder = NameNodeFileMappings.newBuilder(this.getNameNodeFileMappings());
        try {
            NameNodeFileMapping.Builder newMappingBuilder = NameNodeFileMapping.newBuilder();
            newMappingBuilder.setFileName(filename);
            for(Integer i : listOfBlocks) {
                newMappingBuilder.addBlockNumbers(i);
            }
            updatedMappingsBuilder.addMappings(newMappingBuilder);
            synchronized(filesPbufLock) {
                updatedMappingsBuilder.build().writeTo(
                        new FileOutputStream(this.filesPbuf));
            }
        } catch (Exception e) {
            System.out.println("Problem parsing the name node file mappings?? " + e.getMessage());
            e.printStackTrace();
        }
    }

    public NameNodeFileMappings getNameNodeFileMappings() {
        NameNodeFileMappings returnMappings = null;
        try {
            synchronized(filesPbufLock) {
                returnMappings = NameNodeFileMappings.parseFrom(
                        new FileInputStream(this.filesPbuf));
            }
        } catch (FileNotFoundException e) {
            // Looks like this is the first run, and there is no metadata
            // handling that case
        } catch (IOException e) {
            // Might be able to handle the IOException better.
            System.out.println("Problem parsing the name nodes file mappings?? " + e.getMessage());
            e.printStackTrace();
        }
        return returnMappings;
    }

    /*
     *      Datanode <-> Namenode interaction methods
    */

    /* BlockReportResponse blockReport(BlockReportRequest) */
    /* Get the status for blocks */
    public synchronized byte[] blockReport(byte[] request) throws RemoteException {
        System.out.println("Got a DN Report!! ");
        return null;
    }

    /* HeartBeatResponse heartBeat(HeartBeatRequest) */
    /* Heartbeat messages between NameNode and DataNode */
    public synchronized byte[] heartBeat(byte[] request) throws RemoteException {
        HeartBeatRequest heartBeatRequest = null;
        String heartBeatIP = "";
        int heartBeatId = -1;
        try {
            boolean print;
            heartBeatRequest = HeartBeatRequest.parseFrom(request);
            heartBeatIP = heartBeatRequest.getIp();
            heartBeatId = heartBeatRequest.getId();
            print = true;
            for(int i=0; i < dataNodeList.size(); i++) {
                if(dataNodeList.get(i).trim().equals(heartBeatIP.trim())) {
                    dataNodeList.remove(i);
                    print = false;
                }
            }
            // Adding the heart beat node to the top of our list
            dataNodeList.add(0, new String(heartBeatIP));
            if(print) {
                System.out.println("Received new heart beat from " + heartBeatIP + " " + heartBeatId);
                System.out.println("Number of active DataNodes = " + dataNodeList.size() );
            }
        } catch (Exception e) {
            System.out.println("Parsing heart beat request problem?? " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    public void println(String s) { System.out.println(s); }

    public static void main(String[] args) {
        // Register my Name Node on my rmi and just chill
        try {
            NameNode me = new NameNode(configFile);
            Naming.rebind("HDFSNameNode", me);
        } catch (Exception e) {
            System.out.println("Name Node binding to rmi problem?? " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("Name Node bound to RMI Registry!! ");
    }

    public class RendezvousRunnable extends UnicastRemoteObject implements RendezvousRunnableInterface, Runnable {
        
        String fileName;
        NameNode parentNameNode;
        String bindingIdentifier;
        boolean readPermission;
        boolean writePermission;

        ArrayList<Integer> fileBlocks;

        public RendezvousRunnable(String filename, NameNode node, String identifier, 
                boolean readperm, boolean writeperm) throws RemoteException{
            this.fileName = filename;
            this.parentNameNode = node;
            this.bindingIdentifier = identifier;
            this.readPermission = readperm;
            this.writePermission = writeperm;
            this.fileBlocks = new ArrayList<Integer>();
        }
        public void run() {
            try {
                Naming.rebind(bindingIdentifier, this);
            } catch (Exception e) {
                System.out.println("Rendezvous binding to rmi problem?? " + e.getMessage());
                e.printStackTrace();
            }
        }

        /* AssignBlockResponse assignBlock() */
        /* Method to assign a block which will return the replicated block locations */
        public synchronized byte[] assignBlock() throws RemoteException {
            // Right now, use stupid assignment for starters.
            // We pick the first 2 DNs and block numbers starting from 0.
            // This block number should be written on file on the NN.

            int blockNumber = this.parentNameNode.getAndIncrementBlockNumber();
            AssignBlockResponse.Builder assignBlockResponseBuilder = AssignBlockResponse.newBuilder();

            assignBlockResponseBuilder.setStatus(1);
            assignBlockResponseBuilder.setBlockNumber(blockNumber);
            // Trying to add up to 2 data nodes to the 
            if(dataNodeList.size() >= 2) {
                assignBlockResponseBuilder.addDataNodeIPs(dataNodeList.get(0));
                assignBlockResponseBuilder.addDataNodeIPs(dataNodeList.get(1));
            } else if(dataNodeList.size() == 1) {
                assignBlockResponseBuilder.addDataNodeIPs(dataNodeList.get(0));
            }
            // assignBlockResponseBuilder.addDataNodeIPs("127.0.0.1");

            this.fileBlocks.add(new Integer(blockNumber));

            return assignBlockResponseBuilder.build().toByteArray();
        }

        /* CloseFileResponse closeFile(CloseFileRequest) */
        public synchronized void closeFile() throws RemoteException {
            if(this.writePermission)
            this.parentNameNode.writeToFilesPbuf(this.fileName, this.fileBlocks);
            this.parentNameNode.removeFileFromFilesList(this.fileName);
            System.out.println("Closing file");
            // Maybe I should kill this thread :P
        }

        /* NameNodeBlockDataNodeMappings getNameNodeBlockDataNodeMappings(nameNodeBlockDataNodeMappingsRequest) */
        /* To respond with the block mappings needed to download a file */
        public byte[] getNameNodeBlockDataNodeMappings(byte[] requestEncoded) throws RemoteException {
            parentNameNode.println("Get name node block data node mappings called!!");

            NameNodeFileMappings fileToBlocksMappings = parentNameNode.getNameNodeFileMappings();
            NameNodeBlockDataNodeMappings blockToDNsMappings = parentNameNode.getNameNodeBlockDataNodeMappings();


            NameNodeBlockDataNodeMappingsResponse.Builder responseBuilder = NameNodeBlockDataNodeMappingsResponse.newBuilder();

            NameNodeBlockDataNodeMappingsRequest request = null;
            ArrayList<Integer> blockNumbers = new ArrayList<Integer>();
            String fileToGet = "";
            try {
                request = NameNodeBlockDataNodeMappingsRequest.parseFrom(requestEncoded);
            } catch (Exception e) {
                parentNameNode.println("Problem parsing getNameNodeBlockDataNodeMappings request?? " + e.getMessage());
                e.printStackTrace();
            }

            fileToGet = request.getFileName();
            
            for(int i=0; i < fileToBlocksMappings.getMappingsCount(); i++) {
                NameNodeFileMapping thisMapping = fileToBlocksMappings.getMappings(i);
                parentNameNode.println("Here maybe?");
                if(fileToGet.equals(thisMapping.getFileName())) {
                    parentNameNode.println("Found File??");
                    for(int j=0; j < thisMapping.getBlockNumbersCount(); j++) {
                        blockNumbers.add(new Integer(thisMapping.getBlockNumbers(j)));
                        parentNameNode.println("this is j:" + j);
                    }
                    break;
                }
            }
            parentNameNode.println("Got " + blockNumbers.size() + " block(s) for " + fileToGet);
            
            return null;

/*
            if(mappings != null) {
                int mappingsCount = mappings.getMappingsCount();
                for(int i=0; i < mappingsCount; i++) {
                    int mappingBlockNumber = mappings.getMappings(i).getBlockNumber();
                    for(Integer j : blockNumbers) {
                        NameNodeBlockDataNodeMapping.Builder nameNodeBlockDataNodeMappingBuilder = NameNodeBlockDataNodeMapping.newBuilder();
                        if(mappingBlockNumber == j.intValue()) {
                            nameNodeBlockDataNodeMappingBuilder.setBlockNumber(mappingBlockNumber);
                            for(int k=0; k < mappings.getMappingsCount(); k++) {
                                nameNodeBlockDataNodeMappingBuilder.addDataNodeIPs(mappings.getMappings(i).getDataNodeIPs(k));
                            }
                            nameNodeBlockDataNodeMappingsResponseBuilder.addMappings(nameNodeBlockDataNodeMappingBuilder);
                        } 
                    }
                }
            }
            return nameNodeBlockDataNodeMappingsResponseBuilder.build().toByteArray();*/
        }
    }
}
