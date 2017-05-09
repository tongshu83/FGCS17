package njit.bd.dataCenter;

public class Machine {
	int mi_clusterID = 0;
	int mi_machID = 0;
	double md_utilization = 0.0;    // used by task consolidation
	
	public Machine() { }
	public Machine(int machID) {
		mi_machID = machID;
	}
	public Machine(int clusterID, int machID) {
		mi_clusterID = clusterID;
		mi_machID = machID;
	}
	
	public int getClusterID() {
		return mi_clusterID;
	}
	
	public void setClusterID(int clusterID) {
		mi_clusterID = clusterID;
	}
	
	public int getMachID() {
		return mi_machID;
	}
	
	public void setMachID(int machID) {
		mi_machID = machID;
	}
	
	public double getUtilization() {
		return md_utilization;
	}
	
	public void setUtilization(double utilization) {
		md_utilization = utilization;
	}
}
