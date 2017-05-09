package njit.bd.application;

public class JobIndexPair {
	int mi_wfID = 0;
	int mi_jobID = 0;
	
	public JobIndexPair() { }
	public JobIndexPair(int wfID, int jobID) {
		mi_wfID = wfID;
		mi_jobID = jobID;
	}
	
	public void set(JobIndexPair jip) {
		mi_wfID = jip.mi_wfID;
		mi_jobID = jip.mi_jobID;
	}
	
	public int getWfID() {
		return mi_wfID;
	}
	
	public int getJobID() {
		return mi_jobID;
	}
}
