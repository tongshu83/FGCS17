package njit.bd.dataCenter;

public class HomoCluster3 extends HomoCluster {

	public HomoCluster3(int clusterID, int machNum, int machStartID) {
		// Intel Itanium 2 9152M (2 cores)
		super(clusterID, machNum, machStartID);
		mi_coreNum = 8;
		md_procSpeed = 1.66;    // in GHz
		mi_memSize = 65536;    // in MB
		md_dynPower = 104;    // in W
		md_staPower = 450;    // in W
	}
}