package njit.bd.dataCenter;

import njit.bd.common.Constant;

public class MachPriorityQueue {    // used by task consolidation
	int mi_machNum = 0;
	int mi_capacity = 0;
	int[] mai_machQueue;
	HeteroCluster mH_hc;
	int mi_clusterID;
	
	public MachPriorityQueue(HeteroCluster hc, int clusterID) {
		mi_machNum = 0;
		mi_capacity = hc.getHomoCluster(clusterID).getMachNum();
		mai_machQueue = new int[mi_capacity];
		for (int i = 0; i < mi_capacity; i++) {
			mai_machQueue[i] = Constant.uncertainInt;
		}
		mH_hc = hc;
		mi_clusterID = clusterID;
	}
	
	public void enqueue(int machID) {
		if (mi_clusterID != mH_hc.getMachine(machID).getClusterID()) {
			return;
		}
		mai_machQueue[mi_machNum] = machID;
		mi_machNum++;
	}
	
	public int dequeue() {
		int tailIndex = mi_machNum - 1;
		for (int i = 0; i < tailIndex; i++) {
			if (mH_hc.getMachine(mai_machQueue[i]).getUtilization() < mH_hc.getMachine(mai_machQueue[tailIndex]).getUtilization()) {
				int temp = mai_machQueue[i];
				mai_machQueue[i] = mai_machQueue[tailIndex];
				mai_machQueue[tailIndex] = temp;
			}
		}
		
		int machID = mai_machQueue[tailIndex];
		mai_machQueue[tailIndex] = Constant.uncertainInt;
		mi_machNum--;
		return machID;
	}
	
	public void sort() {     // Sorting the descreasing order
		mergeSort(0, mi_machNum - 1);
	}
	
	private void mergeSort(int startIndex, int endIndex) {
		if (startIndex >= endIndex) {
			return;
		}
		int middleIndex = (startIndex + endIndex) / 2;
		mergeSort(startIndex, middleIndex);
		mergeSort(middleIndex + 1, endIndex);
		int[] buffer = new int[endIndex - startIndex + 1];
		int i = startIndex;
		int j = middleIndex + 1;
		int k = 0;
		while (i <= middleIndex && j <= endIndex) {
			if (mH_hc.getMachine(mai_machQueue[i]).getUtilization() > mH_hc.getMachine(mai_machQueue[j]).getUtilization()) {
				buffer[k] = mai_machQueue[i];
				i++;
			} else {
				buffer[k] = mai_machQueue[j];
				j++;
			}
			k++;
		}
		if (i > middleIndex) {
			while (j <= endIndex) {
				buffer[k] = mai_machQueue[j];
				j++;
				k++;
			}
		} else {    // j > endIndex
			while (i <= middleIndex) {
				buffer[k] = mai_machQueue[i];
				i++;
				k++;
			}
		}
		for (int l = 0; l < endIndex - startIndex + 1; l++) {
			mai_machQueue[startIndex + l] = buffer[l];
		}
	}
	
	public int getMachID(int index) {
		if (index >= 0 && index < mi_machNum) {
			return mai_machQueue[index];
		} else {
			return Constant.uncertainInt;
		}
	}
	
	public int size() {
		return mi_machNum;
	}
}
