package njit.bd.dataCenter;

public class HomoCluster2 extends HomoCluster {

	public HomoCluster2(int clusterID, int machNum, int machStartID) {
		// Dual Core Xeon 7150N
		super(clusterID, machNum, machStartID);
		mi_coreNum = 12;
		md_procSpeed = 3.5;    // in GHz
		mi_memSize = 65536;    // in MB
		md_dynPower = 150;    // in W
		md_staPower = 400;    // in W
	}
}
