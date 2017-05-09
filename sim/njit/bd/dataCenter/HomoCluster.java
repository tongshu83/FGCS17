package njit.bd.dataCenter;

import njit.bd.application.MoldableJob;

public class HomoCluster {
	int mi_clusterID = 0;
	int mi_machNum = 0;
	int mi_machStartID = 0;
	int mi_coreNum = 12;
	double md_procSpeed = 2.4;
	int mi_memSize = 65536;    // in MB
	double md_dynPower = 40;    // in W
	double md_staPower = 100;    // in W
	
	double md_memUtilRatio = 0.0;    // used by task consolidation
	
	public HomoCluster() { }
	public HomoCluster(int clusterID, int machNum, int machStartID) {
		mi_clusterID = clusterID;
		mi_machNum = machNum;
		mi_machStartID = machStartID;
	}
	
    // used by MapperBAWMEE
	public int getMaxTaskNum(MoldableJob job) {
		int taskNumProc = (int) Math.floor((double) mi_coreNum / job.getTaskProcUtil(mi_clusterID));
		int taskNumMem = mi_memSize / job.getTaskMemSize();
		int taskNum;
		if (taskNumProc < taskNumMem) {
			taskNum = taskNumProc;
		} else {
			taskNum = taskNumMem;
		}
		taskNum *= mi_machNum;
		if (taskNum > job.getMaxTaskNum()) {
			taskNum = job.getMaxTaskNum();
		}
		return taskNum;
	}
	
	public int getClusterID() {
		return mi_clusterID;
	}
	
	public int getMachNum() {
		return mi_machNum;
	}
	
	public int getMachStartID() {
		return mi_machStartID;
	}
	
	public int getCoreNum() {
		return mi_coreNum;
	}
	
	public double getProcSpeed() {
		return md_procSpeed;
	}
	
	public int getMemSize() {
		return mi_memSize;
	}
	
	public double getDynPower() {
		return md_dynPower;
	}
	
	public double getStaPower() {
		return md_staPower;
	}
	
	public double getMemUtilRatio() {
		return md_memUtilRatio;
	}
	
	public void setMemUtilRatio(double memUtilRatio) {
		md_memUtilRatio = memUtilRatio;
	}
}
