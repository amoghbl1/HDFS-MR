import java.rmi.Remote;
import java.rmi.RemoteException;

// This thread handles some of the Name Node stuff, in order to get better synchronization.

public interface RendezvousRunnableInterface extends Remote {

    /* AssignBlockResponse assignBlock(AssignBlockRequest) */
    /* Method to assign a block which will return the replicated block locations */
    byte[] assignBlock() throws RemoteException;

    /* CloseFileResponse closeFile(CloseFileRequest) */
    // byte[] closeFile(byte[] inp ) throws RemoteException;
    void closeFile() throws RemoteException;

    /* NameNodeBlockDataNodeMappings getNameNodeBlockDataNodeMappings(nameNodeBlockDataNodeMappingsRequest) */
    /* To respond with the block mappings needed to download a file */
    byte[] getNameNodeBlockDataNodeMappings(byte[] request) throws RemoteException;

}
