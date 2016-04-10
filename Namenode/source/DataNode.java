import com.distributed.systems.HDFSProtos.DataNodeBlockMapping;
import com.distributed.systems.HDFSProtos.DataNodeBlockMappings;
import com.distributed.systems.HDFSProtos.ReadBlockRequest;
import com.distributed.systems.HDFSProtos.ReadBlockResponse;
import com.distributed.systems.HDFSProtos.WriteBlockRequest;
import com.distributed.systems.HDFSProtos.WriteBlockResponse;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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


    public DataNode(String conf) throws RemoteException{
        this.configFile = conf;
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
            //             // handling that case
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

    /* ReadBlockResponse readBlock(ReadBlockRequest)) */
    /* Method to read data from any block given block-number */
    public byte[] readBlock(byte[] request) throws RemoteException {
        int blockNumber = -1;
        byte[] blockBytes = new byte[ this.BLOCK_SIZE_IN_BYTES ];
        String blockPath = "/dev/null";
        ReadBlockResponse.Builder readBlockResponseBuilder = ReadBlockResponse.newBuilder();
        try {
            ReadBlockRequest readBlockRequest = ReadBlockRequest.parseFrom(request);
            blockNumber = readBlockRequest.getBlockNumber();
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
        fileInputStream.read(blockBytes);
        fileInputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return blockBytes;
    }

    /* WriteBlockResponse writeBlock(WriteBlockRequest) */
    /* Method to write data to a specific block */
    public byte[] writeBlock(byte[] protobuf, byte[] bytes) throws RemoteException {
        System.out.println("Write block called.");
        WriteBlockRequest writeBlockRequest;
        WriteBlockResponse.Builder writeBlockResponseBuilder = WriteBlockResponse.newBuilder();

        int blockNumber = -1;
        ArrayList<String> remainingDataNodeIPs = null;
        boolean startThreadFlag = false;

        try {
            writeBlockRequest = WriteBlockRequest.parseFrom(protobuf);
            blockNumber = writeBlockRequest.getBlockNumber();
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
            System.out.println("Writing bloc number " + blockNumber);
            FileOutputStream fileOutputStream = new FileOutputStream("" + blockNumber); // Using file name as block number as of now.
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

        writeToBlockPbuf(blockNumber, ""+blockNumber);

        return writeBlockResponseBuilder.setStatus(1)
            .build().toByteArray();
    }

    public static void main(String[] args) {
        try {
            DataNode me = new DataNode(configFile);
            Naming.rebind("HDFSDataNode", me);
        } catch (Exception e) {
            System.out.println("Data Node binding to rmi problem?? " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("Data Node bound to RMI Registry!! ");
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
}
