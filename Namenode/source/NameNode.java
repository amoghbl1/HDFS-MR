import com.distributed.systems.HDFSProtos.AssignBlockResponse;
import com.distributed.systems.HDFSProtos.BlockReportRequest;
import com.distributed.systems.HDFSProtos.DataNodeReportMapping;
import com.distributed.systems.HDFSProtos.DataNodeReportMappings;
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
import java.util.HashMap;
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

    HashMap<Integer, ArrayList<String>> blockToDataNodeMappings = new HashMap<Integer, ArrayList<String>>(100);

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
        // Bringing out blockNumber up to speed, before we do anything.
        int blockNumber = 0;
        NameNodeFileMappings oldMappings = this.readNameNodeFileMappings();
        if(oldMappings != null) {
            for(int i=0; i < oldMappings.getMappingsCount(); i++) {
                NameNodeFileMapping thisMapping = oldMappings.getMappings(i);
                for(int j=0; j < thisMapping.getBlockNumbersCount(); j++) {
                    if(thisMapping.getBlockNumbers(j) >= blockNumber)
                        blockNumber = thisMapping.getBlockNumbers(j);
                }
            }
            blockNumber += 10;
            System.out.println("Resuming Name Node, Block Number set to " + blockNumber);
            this.currentBlockNumber = blockNumber;
        } else {
            System.out.println("Block Number started from default! No history found!!");
        }
    }

    public ArrayList<String> getDNsFromBlock(int i) {
        return blockToDataNodeMappings.get(i);
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
            FileInputStream fis = new FileInputStream(this.filesPbuf);
            nameNodeFileMappings = NameNodeFileMappings.parseFrom(fis);
            fis.close();

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
        boolean forRead = false; // true, for read; false for write

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
                    forRead, // boolean readperm, 
                    !forRead)).start(); // boolean writeperm));
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

    public void writeToBlocksPbuf(NameNodeBlockDataNodeMappings.Builder updatedMappingsBuilder) {
        System.out.println("Writing to Blocks Pbuf!!");
        try {
            synchronized(blocksPbufLock){
                FileOutputStream fos = new FileOutputStream(this.blocksPbuf);
                updatedMappingsBuilder.build().writeTo(fos);
                fos.close();
            }
        } catch (Exception e) {
            System.out.println("Problem writing to name node file mappings?? " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Gets the mappings from the default file
    public synchronized NameNodeBlockDataNodeMappings readNameNodeBlockDataNodeMappings(){ 
        NameNodeBlockDataNodeMappings returnMappings = null;
        synchronized(blocksPbufLock){
            try {
                FileInputStream fis = new FileInputStream(this.blocksPbuf);
                returnMappings = NameNodeBlockDataNodeMappings.parseFrom(fis);
                fis.close();
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

        NameNodeFileMappings oldMappings = this.readNameNodeFileMappings();
        NameNodeFileMappings.Builder updatedMappingsBuilder = NameNodeFileMappings.newBuilder();
        if(oldMappings != null) {
            updatedMappingsBuilder = NameNodeFileMappings.newBuilder(oldMappings);
        }
        try {
            NameNodeFileMapping.Builder newMappingBuilder = NameNodeFileMapping.newBuilder();
            newMappingBuilder.setFileName(filename);
            for(Integer i : listOfBlocks) {
                newMappingBuilder.addBlockNumbers(i);
            }
            updatedMappingsBuilder.addMappings(newMappingBuilder);
            synchronized(filesPbufLock) {
                FileOutputStream fos = new FileOutputStream(this.filesPbuf);
                updatedMappingsBuilder.build().writeTo(fos);
                fos.close();
            }
        } catch (Exception e) {
            System.out.println("Problem parsing the name node file mappings?? " + e.getMessage());
            e.printStackTrace();
        }
    }

    public synchronized NameNodeFileMappings readNameNodeFileMappings() {
        NameNodeFileMappings returnMappings = null;
        try {
            synchronized(filesPbufLock) {
                FileInputStream fis = new FileInputStream(this.filesPbuf);
                returnMappings = NameNodeFileMappings.parseFrom(fis);
                fis.close();
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
        DataNodeReportMappings dataNodeReportMappings = null;
        try { 
            dataNodeReportMappings = DataNodeReportMappings.parseFrom(request); 
        } catch (Exception e) {
            System.out.println("Problem parsing DN Report request?? " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("Parsing mappings. Count:" + dataNodeReportMappings.getMappingsCount());
        
        for(int i=0; i < dataNodeReportMappings.getMappingsCount(); i++) {
            DataNodeReportMapping thisMapping = dataNodeReportMappings.getMappings(i);
            ArrayList<String> blockToDNList = blockToDataNodeMappings.get(thisMapping.getBlockNumber());
            if(blockToDNList == null)
                blockToDNList = new ArrayList<String>();
            if(blockToDNList.contains(thisMapping.getDataNodeIP())) {
                blockToDNList.remove(thisMapping.getDataNodeIP());
                blockToDNList.add(0, thisMapping.getDataNodeIP());
            } else {
                blockToDNList.add(0, thisMapping.getDataNodeIP());
            }
            blockToDataNodeMappings.put(thisMapping.getBlockNumber(), blockToDNList);
        }


        System.out.println("New State, after DN REPORT" + blockToDataNodeMappings );
        
        return null;
        /*
        NameNodeBlockDataNodeMappings oldMappings = this.readNameNodeBlockDataNodeMappings();
        NameNodeBlockDataNodeMappings.Builder updatedMappingsBuilder = NameNodeBlockDataNodeMappings.newBuilder();

        if(oldMappings != null && oldMappings.getMappingsCount() > 0) {
            System.out.println("Old mappings count: " + oldMappings.getMappingsCount());
            if( oldMappings.getMappingsCount() > dataNodeReportMappings.getMappingsCount()) {
                for(int i=0; i < oldMappings.getMappingsCount(); i++) {
                    NameNodeBlockDataNodeMapping thisMapping = oldMappings.getMappings(i);
                    NameNodeBlockDataNodeMapping.Builder newMappingBuilder = NameNodeBlockDataNodeMapping.newBuilder();
                    newMappingBuilder.setBlockNumber(thisMapping.getBlockNumber());
                    for(int j=0; j < dataNodeReportMappings.getMappingsCount(); j++) {
                        DataNodeReportMapping DNMapping = dataNodeReportMappings.getMappings(j);
                        if(DNMapping.getBlockNumber() == thisMapping.getBlockNumber()) {
                            newMappingBuilder.addDataNodeIPs(new String(DNMapping.getDataNodeIP().trim()));
                            for(int k=0; k < thisMapping.getDataNodeIPsCount(); k++) {
                                if(DNMapping.getDataNodeIP().trim().equals(thisMapping.getDataNodeIPs(k).trim())) {
                                    continue;
                                } else {
                                    newMappingBuilder.addDataNodeIPs(new String(thisMapping.getDataNodeIPs(k)));
                                }
                            }
                        } else {
                            for(int k=0; k < thisMapping.getDataNodeIPsCount(); k++) {
                                newMappingBuilder.addDataNodeIPs(new String(thisMapping.getDataNodeIPs(k)));
                            }
                        }
                    }
                    updatedMappingsBuilder.addMappings(newMappingBuilder);
                }
            } else {
                for(int i=0; i < dataNodeReportMappings.getMappingsCount(); i++) {
                    DataNodeReportMapping DNMapping = dataNodeReportMappings.getMappings(i);
                    NameNodeBlockDataNodeMapping.Builder newMappingBuilder = NameNodeBlockDataNodeMapping.newBuilder();
                    newMappingBuilder.setBlockNumber(DNMapping.getBlockNumber());
                    for(int j=0; j < oldMappings.getMappingsCount(); j++) {
                        NameNodeBlockDataNodeMapping thisMapping = oldMappings.getMappings(j);
                        if(DNMapping.getBlockNumber() == thisMapping.getBlockNumber()) {
                            newMappingBuilder.addDataNodeIPs(new String(DNMapping.getDataNodeIP().trim()));
                            for(int k=0; k < thisMapping.getDataNodeIPsCount(); k++) {
                                if(DNMapping.getDataNodeIP().trim().equals(thisMapping.getDataNodeIPs(k).trim())) {
                                    continue;
                                } else {
                                    newMappingBuilder.addDataNodeIPs(new String(thisMapping.getDataNodeIPs(k)));
                                }
                            }
                        } else {
                            for(int k=0; k < thisMapping.getDataNodeIPsCount(); k++) {
                                newMappingBuilder.addDataNodeIPs(new String(thisMapping.getDataNodeIPs(k)));
                            }
                        }
                    }
                    updatedMappingsBuilder.addMappings(newMappingBuilder);
                }

            }
        } else {
            System.out.println("No old entries found!!");
            for(int i=0; i < dataNodeReportMappings.getMappingsCount(); i++) {
                NameNodeBlockDataNodeMapping.Builder newMappingBuilder = NameNodeBlockDataNodeMapping.newBuilder();
                DataNodeReportMapping DNMapping = dataNodeReportMappings.getMappings(i);
                newMappingBuilder.setBlockNumber(DNMapping.getBlockNumber());
                newMappingBuilder.addDataNodeIPs(new String(DNMapping.getDataNodeIP().trim()));
                updatedMappingsBuilder.addMappings(newMappingBuilder);
            }
        }
        System.out.println("Writing " + updatedMappingsBuilder.getMappingsCount() + " mappings to blocks pbuf");
        this.writeToBlocksPbuf(updatedMappingsBuilder);
        return null;
        */
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
            if(this.writePermission) {
                System.out.println("Writing to pbuf before close");
                this.parentNameNode.writeToFilesPbuf(this.fileName, this.fileBlocks);
            }
            this.parentNameNode.removeFileFromFilesList(this.fileName);
            this.parentNameNode.println("Closing file");
            // Maybe I should kill this thread :P
        }

        /* NameNodeBlockDataNodeMappings getNameNodeBlockDataNodeMappings(nameNodeBlockDataNodeMappingsRequest) */
        /* To respond with the block mappings needed to download a file */
        public byte[] getNameNodeBlockDataNodeMappings(byte[] request) throws RemoteException {
            parentNameNode.println("Get name node block data node mappings called!!");
            NameNodeBlockDataNodeMappingsResponse.Builder nameNodeBlockDataNodeMappingsResponseBuilder = NameNodeBlockDataNodeMappingsResponse.newBuilder();
            NameNodeBlockDataNodeMappingsRequest nameNodeBlockDataNodeMappingsRequest = null;
            ArrayList<Integer> blockNumbers = new ArrayList<Integer>();
            try {
                nameNodeBlockDataNodeMappingsRequest = NameNodeBlockDataNodeMappingsRequest.parseFrom(request);
            } catch (Exception e) {
                parentNameNode.println("Problem parsing getNameNodeBlockDataNodeMappings request?? " + e.getMessage());
                e.printStackTrace();
            }
 
            NameNodeFileMappings nameNodeFileMappings = parentNameNode.readNameNodeFileMappings();
            for(int i=0; i < nameNodeFileMappings.getMappingsCount(); i++) {
                if(nameNodeBlockDataNodeMappingsRequest.getFileName().equals(
                        nameNodeFileMappings.getMappings(i).getFileName())) {
                    for(int j=0; j < nameNodeFileMappings.getMappings(i).getBlockNumbersCount(); j++) {
                        blockNumbers.add(new Integer(nameNodeFileMappings.getMappings(i).getBlockNumbers(j)));
                    }
                }
            }

            parentNameNode.println("Got " + blockNumbers.size() +" blocks");
            for(Integer i : blockNumbers) {
                NameNodeBlockDataNodeMapping.Builder blockToDNsBuilder = NameNodeBlockDataNodeMapping.newBuilder();
                blockToDNsBuilder.setBlockNumber(i.intValue());
                // HashMap<Integer, ArrayList<String>> hashMap = parentNameNode.getBlockToDNMappings();
                ArrayList<String> blockToDNs = parentNameNode.getDNsFromBlock(i.intValue());
                if(blockToDNs != null) {
                    parentNameNode.println("Block " + i.intValue() +" is mapped to " + blockToDNs.size() +" IPs.");
                    for(String s : blockToDNs) {
                        blockToDNsBuilder.addDataNodeIPs(s);
                    }
                } else {
                    parentNameNode.println("Null DNs From Block??");
                }
                nameNodeBlockDataNodeMappingsResponseBuilder.addMappings(blockToDNsBuilder);
            }
            /*
            NameNodeBlockDataNodeMappings mappings = parentNameNode.readNameNodeBlockDataNodeMappings(); 
            if(mappings != null) {
                int mappingsCount = mappings.getMappingsCount();
                System.out.println("Mappings we have " + mappingsCount);
                for(Integer i : blockNumbers) {
                    System.out.println("Number: " + i);
                    for(int j=0; j < mappingsCount; j++) {
                    }
                }
            } else {
                System.out.println("Looks like theres some problem getting block to DN mappings for file get :/");
            }

            
            
            if(mappings != null) {
                int mappingsCount = mappings.getMappingsCount();
                for(int i=0; i < mappingsCount; i++) {
                    int mappingBlockNumber = mappings.getMappings(i).getBlockNumber();
                    for(Integer j : blockNumbers) {
                        NameNodeBlockDataNodeMapping.Builder nameNodeBlockDataNodeMappingBuilder = NameNodeBlockDataNodeMapping.newBuilder();
                        if(mappingBlockNumber == j.intValue()) {
                            nameNodeBlockDataNodeMappingBuilder.setBlockNumber(mappingBlockNumber);
                            for(int k=0; k < mappings.getMappings(i).getDataNodeIPsCount(); k++) {
                                nameNodeBlockDataNodeMappingBuilder.addDataNodeIPs(mappings.getMappings(i).getDataNodeIPs(k));
                            }
                            nameNodeBlockDataNodeMappingsResponseBuilder.addMappings(nameNodeBlockDataNodeMappingBuilder);
                            System.out.println("Added mapping " + i);
                        } 
                    }
                }
            }
            */
            return nameNodeBlockDataNodeMappingsResponseBuilder.build().toByteArray();
        }
    }
}
