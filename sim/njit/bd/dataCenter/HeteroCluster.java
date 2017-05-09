package njit.bd.dataCenter;

import njit.bd.application.MoldableJob;

public class HeteroCluster {
	/*
	 * Single Core Xeon	3.20	92W
	 * Dual Core Xeon 7150N	3.50 GHz	150 W 
	 * Xeon E7450 (Six Core, Core-based)	2.4 GHz	90 W
	 * Intel Itanium 2 9152M	1.66 GHz	104 W
	 */
	final public static int homoCluster = 0;
	final public static int heteroCluster = 1;
	final public static int clusterNum = 4;    // <= 4
	
	int mi_clusterNum = 0;
	HomoCluster[] maH_homoClusters;
	int mi_machNum = 0;
	Machine[] maM_machines;
	static double md_aveProcSpeed = 2.4;
	
	public HeteroCluster(int machNum, int clusterType) {
		if (clusterType == homoCluster) {
			mi_clusterNum = 1;
			maH_homoClusters = new HomoCluster[mi_clusterNum];
			maH_homoClusters[0] = new HomoCluster0(0, machNum, 0);
			mi_machNum = machNum;
			maM_machines = new Machine[mi_machNum];
			for (int i = 0; i < mi_machNum; i++) {
				maM_machines[i] = new Machine(0, i);
			}
			md_aveProcSpeed = maH_homoClusters[0].getProcSpeed();
		} else {    // clusterType == heteroCluster
			mi_clusterNum = clusterNum;
			maH_homoClusters = new HomoCluster[mi_clusterNum];
			
			mi_machNum = 0;
			md_aveProcSpeed = 0.0;
			
			// Cluster configuration
			if (mi_clusterNum >= 1) {    // Homogeneous Cluster 0
				int homoMachNum = machNum / mi_clusterNum;
				if (machNum % mi_clusterNum >= 1) {
					homoMachNum++;
				}
				maH_homoClusters[0] = new HomoCluster0(0, homoMachNum, mi_machNum);
				mi_machNum += homoMachNum;
				md_aveProcSpeed += maH_homoClusters[0].getProcSpeed() * homoMachNum;
			}
			
			if (mi_clusterNum >= 2) {    // Homogeneous Cluster 1
				int homoMachNum = machNum / mi_clusterNum;
				if (machNum % mi_clusterNum >= 2) {
					homoMachNum++;
				}
				maH_homoClusters[1] = new HomoCluster1(1, homoMachNum, mi_machNum);
				mi_machNum += homoMachNum;
				md_aveProcSpeed += maH_homoClusters[1].getProcSpeed() * homoMachNum;
			}
			
			if (mi_clusterNum >= 3) {    // Homogeneous Cluster 2
				int homoMachNum = machNum / mi_clusterNum;
				if (machNum % mi_clusterNum >= 3) {
					homoMachNum++;
				}
				maH_homoClusters[2] = new HomoCluster2(2, homoMachNum, mi_machNum);
				mi_machNum += homoMachNum;
				md_aveProcSpeed += maH_homoClusters[2].getProcSpeed() * homoMachNum;
			}
			
			if (mi_clusterNum >= 4) {    // Homogeneous Cluster 3
				int homoMachNum = machNum / mi_clusterNum;
				maH_homoClusters[3] = new HomoCluster3(3, homoMachNum, mi_machNum);
				mi_machNum += homoMachNum;
				md_aveProcSpeed += maH_homoClusters[3].getProcSpeed() * homoMachNum;
			}
			
			md_aveProcSpeed /= mi_machNum;
			
			maM_machines = new Machine[mi_machNum];
			for (int i = 0; i < mi_machNum; i++) {
				maM_machines[i] = new Machine(i);
			}
			for (int i = 0; i < mi_clusterNum; i++) {
				int machStartID = maH_homoClusters[i].getMachStartID();
				int homoMachNum = maH_homoClusters[i].getMachNum();
				for (int j = machStartID; j < machStartID + homoMachNum; j++) {
					maM_machines[j].setClusterID(i);
				}
			}
		}
    }
	
    // used by MapperATPBAWMEE
	public int getMaxTaskNum(MoldableJob job) {
		int taskNum = 0;
		for (int i = 0; i < mi_clusterNum; i++) {
			taskNum += maH_homoClusters[i].getMaxTaskNum(job);
		}
		if (taskNum > job.getMaxTaskNum()) {
			taskNum = job.getMaxTaskNum();
		}
		return taskNum;
	}
	
	public int getClusterNum() {
		return mi_clusterNum;
	}
	
	public HomoCluster getHomoCluster(int clusterID) {
		if (clusterID >= 0 && clusterID < mi_clusterNum) {
			return maH_homoClusters[clusterID];
		} else {
			return null;
		}
	}
	
	public int getMachNum() {
		return mi_machNum;
	}
	
	public Machine getMachine(int machID) {
		if (machID >= 0 && machID < mi_machNum) {
			return maM_machines[machID];
		} else {
			return null;
		}
	}
	
	public static double getAveProcSpeed() {
		return md_aveProcSpeed;
	}
}
