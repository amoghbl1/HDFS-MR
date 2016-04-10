public interface JobTrackerInterface {
	
	/* JobSubmitResponse jobSubmit(JobSubmitRequest) */
	byte[] jobSubmit(byte[] request) throws RemoteException;

	/* JobStatusResponse getJobStatus(JobStatusRequest) */
	byte[] getJobStatus(byte[] request) throws RemoteException;
	
	/* HeartBeatResponse heartBeat(HeartBeatRequest) */
	byte[] heartBeat(byte[] request) throws RemoteException;
}
