package njit.bd.dataCenter;

public class HomoCluster1 extends HomoCluster {

	public HomoCluster1(int clusterID, int machNum, int machStartID) {
		// Single Core Xeon
		super(clusterID, machNum, machStartID);
		mi_coreNum = 6;
		md_procSpeed = 3.2;    // in GHz
		mi_memSize = 65536;    // in MB
		md_dynPower = 92;    // in W
		md_staPower = 600;    // in W
	}
}
