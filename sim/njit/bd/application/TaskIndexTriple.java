package njit.bd.application;

public class TaskIndexTriple {
	int mi_wfID = 0;
	int mi_jobID = 0;
	int mi_taskID = 0;
	
	public TaskIndexTriple() {}
	public TaskIndexTriple(int wfID, int jobID, int taskID) {
		mi_wfID = wfID;
		mi_jobID = jobID;
		mi_taskID = taskID;
	}
	
	public void set(TaskIndexTriple tit) {
		mi_wfID = tit.mi_wfID;
		mi_jobID = tit.mi_jobID;
		mi_taskID = tit.mi_taskID;
	}
	
	public int getWfID() {
		return mi_wfID;
	}
	
	public int getJobID() {
		return mi_jobID;
	}
	
	public int getTaskID() {
		return mi_taskID;
	}
}