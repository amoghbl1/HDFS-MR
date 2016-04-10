import com.distributed.systems.HDFSProtos.AssignBlockResponse;
import com.distributed.systems.HDFSProtos.ListFilesRequest;
import com.distributed.systems.HDFSProtos.ListFilesResponse;
import com.distributed.systems.HDFSProtos.NameNodeBlockDataNodeMapping;
import com.distributed.systems.HDFSProtos.NameNodeBlockDataNodeMappingsRequest;
import com.distributed.systems.HDFSProtos.NameNodeBlockDataNodeMappingsResponse;
import com.distributed.systems.HDFSProtos.OpenFileRequest;
import com.distributed.systems.HDFSProtos.OpenFileResponse;
import com.distributed.systems.HDFSProtos.ReadBlockRequest;
import com.distributed.systems.HDFSProtos.ReadBlockResponse;
import com.distributed.systems.HDFSProtos.WriteBlockRequest;
import com.distributed.systems.HDFSProtos.WriteBlockResponse;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.Naming;
import java.util.ArrayList;


public class Client {

    private int BLOCK_SIZE_IN_BYTES = 32000000;

    // Using hdfs_mr_client.conf as the default config file.
    private static String configFile = "hdfs_mr_client.conf"; 

    // Using these defaults in case there is nothing in the config file.
    private static String nameNodeIP = "127.0.0.1";
    private static String jobTrackerIP = "127.0.0.1";

    private static String rendezvousIdentifier;
    
    private static String DOWNLOADS_PREFIX = "Downloads/";

    public Client(String configFile) {
        this.configFile = configFile;
    }

    public Client(String configFile, String rendezvousIdentifier) {
        this.configFile = configFile;
        this.rendezvousIdentifier = rendezvousIdentifier;
    }

    /**
     * Opens a file, that is, gets the rendezvous identifier
     * and sets it as this objects rendezvous.
     * 
     * @param filename the file to open
     * @param forRead set if file is being read
     * @return boolean Success of the request
     */
    public boolean open(String fileName, boolean forRead) {
        // We should add more checks to see if another file is already open, and close it auto
        // or maybe throw an error

        OpenFileRequest.Builder openFileRequestBuilder = OpenFileRequest.newBuilder();
        openFileRequestBuilder.setFileName(fileName);
        openFileRequestBuilder.setForRead(forRead);
        OpenFileResponse openFileResponse = null;

        byte[] requestEncoded = openFileRequestBuilder.build().toByteArray();
        byte[] responseEncoded = null;

        try {

            NameNodeInterface nameNode = (NameNodeInterface) Naming.lookup("//" +
                    this.nameNodeIP + "/HDFSNameNode"); // This name node request location is hard coded.
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

    /**
     * Close file function closes the file open and bound to this object;
     * Basically, asks Rendezvous to shutdown.
     *
     * @return boolean Value indicates success of request
     */
    public boolean close() {
        try {
            RendezvousRunnableInterface rendezvous = (RendezvousRunnableInterface) Naming.lookup("//" +
                    this.nameNodeIP + "/" + this.rendezvousIdentifier);
            rendezvous.closeFile();

        } catch (Exception e) {
            System.out.println("Connecting to HDFS for close file problem?? " + e.getMessage());
            e.printStackTrace();
        }
        return true;
    }

    /** 
     * The get function is used to download a file
     * stored on the HDFS, using the HDFS config file
     *
     * @param fileName This is the filename to be downloaded from the HDFS
     * @return boolean Value indicating the success of the get request
     */ 
    public boolean get(String fileName) {
        byte[] responseEncoded = null;
        byte[] requestEncoded = null;
        NameNodeBlockDataNodeMappingsResponse nameNodeBlockDataNodeMappingsResponse = null;
        NameNodeBlockDataNodeMappingsRequest.Builder nameNodeBlockDataNodeMappingsRequest = NameNodeBlockDataNodeMappingsRequest.newBuilder();
        nameNodeBlockDataNodeMappingsRequest.setFileName(fileName);
        requestEncoded = nameNodeBlockDataNodeMappingsRequest.build().toByteArray();
        try {
            RendezvousRunnableInterface rendezvous = (RendezvousRunnableInterface) Naming.lookup("//" +
                    this.nameNodeIP + "/" + this.rendezvousIdentifier);
            responseEncoded = rendezvous.getNameNodeBlockDataNodeMappings(requestEncoded);

        } catch (Exception e) {
            System.out.println("Connecting to NN for get file problem?? " + e.getMessage());
            e.printStackTrace();
        }
        try {
            nameNodeBlockDataNodeMappingsResponse = NameNodeBlockDataNodeMappingsResponse.parseFrom(responseEncoded);
            System.out.println("Response: " + nameNodeBlockDataNodeMappingsResponse.getMappingsCount());
            for(int i=0; i < nameNodeBlockDataNodeMappingsResponse.getMappingsCount(); i++) {
                if(nameNodeBlockDataNodeMappingsResponse.getMappings(i).getDataNodeIPsCount() > 0) {
                    appendBlockToFile(fileName, 
                            nameNodeBlockDataNodeMappingsResponse.getMappings(i).getBlockNumber(),
                            nameNodeBlockDataNodeMappingsResponse.getMappings(i).getDataNodeIPs(0));
                } else {
                    System.out.println("Sorry, looks like none of the nodes with the blocks are up!!");
                }
            } 
        }catch (Exception e) {
            System.out.println("Parsing get response problem?? " + e.getMessage());
            e.printStackTrace();
        }

        return true;
    }

    /** 
    * Helper for file download
    * Appends a block to a given filename
    *
    * @param filename the file to append to
    * @param blockNumber is the block to download
    * @param dataNodeIP is the Data Node to download from
    *
    * @return boolean status of the function
    */
    public boolean appendBlockToFile(String filename, int blockNumber, String dataNodeIP) {
        System.out.println("Getting block " + blockNumber + " from " + dataNodeIP);
        byte[] blockBytes = getBlockFromServer(dataNodeIP, blockNumber);
        try {
        FileOutputStream fileOutputStream = new FileOutputStream(DOWNLOADS_PREFIX + filename, true);
        fileOutputStream.write(blockBytes);
        fileOutputStream.close();
        } catch (Exception e) {
            System.out.println("Problem appending to file??" + e.getMessage());
            e.printStackTrace();
        }
        return true;
    }

    /** 
     * Helper function for file download
     * gets the block number on a particular server.
     *
     * @param server server IP or name
     * @param blockNumber Block number to fetch
     * @return byte[] The byte array that the server responds with
     */
    public byte[] getBlockFromServer(String server, int blocknumber) {
        ReadBlockRequest.Builder readBlockRequestBuilder = ReadBlockRequest.newBuilder();
        byte[] recievedBytes = null;
        try {
            readBlockRequestBuilder.setBlockNumber(blocknumber);
            DataNodeInterface datanode = (DataNodeInterface) Naming.lookup("//" +
                    server + "/HDFSDataNode");
            recievedBytes = datanode.readBlock(
                    readBlockRequestBuilder.build().toByteArray());
        } catch (Exception e){
            System.out.println("Problem sending read block request?? " + e.getMessage());
            e.printStackTrace();
        }
        return recievedBytes;
    }

    /**
     * Makes call to NN for the file we currently have open
     * and responds with what NN gives it
     *
     * @return AssignBlockResponse reponse object on calling assignBlock on NN
     */
    public AssignBlockResponse getBlock() {
        AssignBlockResponse assignBlockResponse = null;
        byte[] responseEncoded = null;
        try {
            RendezvousRunnableInterface rendezvous = (RendezvousRunnableInterface) Naming.lookup("//" +
                    this.nameNodeIP + "/" + this.rendezvousIdentifier);
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


    /** 
     * The get function is used to upload a file
     * to the HDFS, using the HDFS config file
     *
     * @param filename This is the filename to be uploaded to the HDFS
     * @return boolean Value indicating the success of the put request
     */
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

                AssignBlockResponse assignBlockResponse = getBlock();
                writeBlockRequestBuilder.setBlockNumber(assignBlockResponse.getBlockNumber());


                // for(int i=0; i < readSize; i++)
                //     writeBlockRequestBuilder.addData(block[i]);

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

    /** 
     * The list function is used to list all files
     * stored on the HDFS, using the HDFS config file
     *
     * @return String[] List of all the files
     */
    public ArrayList<String> list() {

        ListFilesResponse listFilesResponse = null;
        ListFilesRequest.Builder listFilesRequestBuilder = ListFilesRequest.newBuilder();

        ArrayList<String> filesList = new ArrayList<String>();
        // This dir Name feature is currently unused and will be used in future implementations
        listFilesRequestBuilder.setDirName("");
        byte[] requestEncoded = listFilesRequestBuilder.build().toByteArray();
        byte[] responseEncoded = null; // This is the response that we get from the Name Node
        try {
            // Using a new instance of connection every request.
            // Can this be globalized or do I have to make the lookup every single time?
            NameNodeInterface nameNode = (NameNodeInterface) Naming.lookup("//" +
                    this.nameNodeIP + "/HDFSNameNode"); // This name node request location is hard coded.
            responseEncoded = nameNode.list(requestEncoded);

        } catch (Exception e) {
            System.out.println("Connecting to HDFS for list problem?? " + e.getMessage());
            e.printStackTrace();
        }

        try {
            listFilesResponse = ListFilesResponse.parseFrom(responseEncoded);
            for(int i=0; i < listFilesResponse.getFileNamesCount(); i++) {
                filesList.add(new String(
                            listFilesResponse.getFileNames(i)));
            }
        } catch (Exception e) {
            System.out.println("Problem parsing list response?? " + e.getMessage());
            e.printStackTrace();
        }
        return filesList;
    }

    /**
     * Runs a job given all the right parameters
     *  
     * @param mapClassName The class name that map task should load
     * @param reduceClassName The class name that reduce task should load
     * @param inputFile the input file on HDFS to run job on
     * @param outputFile the output file to write on HDFS
     * @param numberOfReducers the number of reducers to be used
     */
    public void runJob(String mapClassName, String reduceClassName, String inputFile, String outputFile, int numberOfReducers) {
        
    }

    /**
     * Parse all command line args and figure out what exactly to do
     * Also parse the config file to figure out the HDFS environment
     *
     */
    public static void main(String[] args) {

        // Maybe set config file, based on args.

        if(args.length <= 0) {
            System.out.println("Please enter a command:");
            System.out.println("list <filename>");
            System.out.println("put <filename>");
            System.out.println("get <filename>");
            return;
        }


        BufferedReader fileReader = null;
        String configLine;
        ArrayList<String> listFiles;
        try {
            fileReader = new BufferedReader(new FileReader(configFile));
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
                nameNodeIP = configLine.split(" ")[1];
            }
            else if(configLine.startsWith("jobTrackerIP")) {
                jobTrackerIP = configLine.split(" ")[1];
            }
        }

        // This is done so that we can set the conf file with the constructor.
        // Also, using this kind of a medium makes it easy for other classes to use this,
        // in case it is needed in the future
        Client me = new Client(configFile);
        // me.open("newtest", true);
        // me.put("testFile");
        // me.close();

        if(args[0].equals("list")) {
            listFiles = me.list();
            System.out.println("Filelist:");
            for(String s : listFiles) {
                System.out.println(s);
            }
        }
        else if(args[0].equals("put")) {

            // Lets just do some kind of file already there stuff here.
            listFiles = me.list();
            for(String s : listFiles) {
                if(args[1].equals(s)) {
                    System.out.println("Sorry! File already exists.");
                    return;
                }
            }

            me.open(args[1], false);
            me.put(args[1]);
            me.close();
        }
        else if(args[0].equals("get")) {
            me.open(args[1], true);
            me.get(args[1]);
            me.close();
        }
        else if(args[0].equals("job")) {
            if(args.length < 6) {
                System.out.println("Not enough arguments for job.");
                System.out.println("Usage:");
                System.out.println("job mapClassName reduceClassName inputFile outputFile numOfReducers");
                return;
            }
            me.runJob(args[1], // Map Class Name
                    args[2], // Reduce Class Name
                    args[3], // Input File Name (File present on HDFS)
                    args[4], // Output File Name (File will be written to HDFS)
                    Integer.parseInt(args[5])); // Number of reducers
        }
    }
}
