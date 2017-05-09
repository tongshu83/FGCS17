package njit.bd.dataCenter;

public class HomoCluster0 extends HomoCluster {

	public HomoCluster0(int clusterID, int machNum, int machStartID) {
		// Xeon E7450 (Six Core, Core-based)
		super(clusterID, machNum, machStartID);
		mi_coreNum = 18;
		md_procSpeed = 2.4;    // in GHz
		mi_memSize = 65536;    // in MB
		md_dynPower = 90;    // in W
		md_staPower = 500;    // in W
	}
}
