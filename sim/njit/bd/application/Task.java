package njit.bd.application;

import njit.bd.common.Constant;

public class Task extends TaskIndexTriple {	
	double md_startTime = Constant.uncertainDouble;
	double md_endTime = Constant.uncertainDouble;
	int mi_machID = Constant.uncertainInt;
	double md_freedom = 0.0;
	
	public Task(int wfID, int jobID, int taskID) {
		super(wfID, jobID, taskID);
	}
	
	public void mapping(double startTime, double endTime, int machID) {
		md_startTime = startTime;
		md_endTime = endTime;
		mi_machID = machID;
	}
	
	public void withdrawMapping() {
		md_startTime = Constant.uncertainDouble;
		md_endTime = Constant.uncertainDouble;
		mi_machID = Constant.uncertainInt;
		md_freedom = 0.0;
	}
	
	public boolean isMapped() {
		if (md_startTime >= 0.0 && md_endTime >= 0.0 && mi_machID >= 0) {
			return true;
		}
		return false;
	}
	
	public double getStartTime() {
		return md_startTime;
	}
	
	public double getEndTime() {
		return md_endTime;
	}
	
	public int getMachID() {
		return mi_machID;
	}
	
	public double getFreedom() {
		return md_freedom;
	}
	
	public void setFreedom(double freedom) {
		md_freedom = freedom;
	}
}
