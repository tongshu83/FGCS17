package njit.bd.hadoopYARN;

import java.util.Vector;

import njit.bd.common.Constant;
import njit.bd.dataCenter.*;

public class TimeResourceTable {    // available time-resource table
	final static double maxTime = 31536000.0; // one year in seconds
	Vector<Resource> mVR_timeVector;
	
	public class Resource {
		double md_startTime = 0.0;
		double md_endTime = 0.0;
//		int mi_resUnit = 192;
		int mi_machNum = 0;
//		double[] mad_procUtils;
		int[] mai_coreNums;
		int[] mai_memSizes;
		
		public Resource() { }
		public Resource(double startTime, double endTime, HeteroCluster hc) {
			md_startTime = startTime;
			md_endTime = endTime;
			mi_machNum = hc.getMachNum();
//			mad_procUtils = new double[mi_machNum];
			mai_coreNums = new int[mi_machNum];
			mai_memSizes = new int[mi_machNum];
			for (int i = 0; i < mi_machNum; i++) {
				HomoCluster homoCluster = hc.getHomoCluster(hc.getMachine(i).getClusterID());
//				mad_procUtils[i] = (double) homoCluster.getCoreNum();
				mai_coreNums[i] = homoCluster.getCoreNum();
				mai_memSizes[i] = homoCluster.getMemSize();
			}
		}
		public Resource(Resource res) {
			md_startTime = res.md_startTime;
			md_endTime = res.md_endTime;
//			mi_resUnit = res.mi_resUnit;
			mi_machNum = res.mi_machNum;
//			mad_procUtils = new double[mi_machNum];
			mai_coreNums = new int[mi_machNum];
			mai_memSizes = new int[mi_machNum];
			for (int i = 0; i < mi_machNum; i++) {
//				mad_procUtils[i] = res.mad_procUtils[i];
				mai_coreNums[i] = res.mai_coreNums[i];
				mai_memSizes[i] = res.mai_memSizes[i];
			}
		}
		
		public double getStartTime() {
			return md_startTime;
		}
		
		public double getEndTime() {
			return md_endTime;
		}
		
//		public double getProcUtil(int machID) {
//			return mad_procUtils[machID];
//		}
		
		public int getCoreNum(int machID) {
			return mai_coreNums[machID];
		}
		
		public int getMemSize(int machID) {
			return mai_memSizes[machID];
		}
	}
	
	public TimeResourceTable(HeteroCluster hc) {
		mVR_timeVector = new Vector<Resource>();
		Resource ar = new Resource(0.0, maxTime, hc);
		mVR_timeVector.add(ar);
	}
	
	public boolean isAvailable(double startTime, double endTime, int machID, int coreNum, //double procUtil, 
			int memSize, int startTimeSlotIndex) {
		if (startTimeSlotIndex < 0) {
			return false;
		}
		for (int i = startTimeSlotIndex; i < mVR_timeVector.size(); i++) {
			if (startTime < mVR_timeVector.get(i).md_endTime) {
//				if (mVR_timeVector.get(i).mad_procUtils[machID] < procUtil 
				if (mVR_timeVector.get(i).mai_coreNums[machID] < coreNum 
						|| mVR_timeVector.get(i).mai_memSizes[machID] < memSize) {
					break;
				}
				if (endTime <= mVR_timeVector.get(i).md_endTime) {
					return true;
				}
			}
		}
		return false;
	}
	
	public int getAvailMachID(double startTime, double endTime, int coreNum, //double procUtil, 
			int memSize, HomoCluster hc, int startTimeSlotIndex) {
		if (startTimeSlotIndex < 0 || startTimeSlotIndex >= mVR_timeVector.size()) {
			return Constant.uncertainInt;
		}
		for (int i = hc.getMachStartID(); i < hc.getMachStartID() + hc.getMachNum(); i++) {
//			if (isAvailable(startTime, endTime, i, procUtil, memSize, startTimeSlotIndex)) {
			if (isAvailable(startTime, endTime, i, coreNum, memSize, startTimeSlotIndex)) {
				return i;
			}
		}
		return Constant.uncertainInt;
	}
	/*
	public int getAvailMachID(double startTime, double endTime, double procUtil, int memSize, HomoCluster hc) {
		for (int i = hc.getMachStartID(); i < hc.getMachStartID() + hc.getMachNum(); i++) {
			for (int j = 0; j < mVR_timeVector.size(); j++) {
				if (startTime < mVR_timeVector.get(j).md_endTime) {
					if (mVR_timeVector.get(j).mad_procUtil[i] < procUtil 
							|| mVR_timeVector.get(j).mai_memSize[i] < memSize) {
						break;
					}
					if (endTime <= mVR_timeVector.get(j).md_endTime) {
						return i;
					}
				}
			}
		}
		return Constant.uncertainInt;
	}
	*/
	public boolean reserve(double startTime, double endTime, int machID, int coreNum, //double procUtil, 
			int memSize, int startTimeSlotIndex) {
		if (startTimeSlotIndex < 0) {
			return false;
		}
//		if (procUtil == 0.0 && memSize == 0) {
		if (coreNum == 0 && memSize == 0) {
			return true;
		}
//		if (procUtil * memSize < 0.0) {
		if (coreNum * memSize < 0) {
			return false;
		}
		for (int i = startTimeSlotIndex; i < mVR_timeVector.size(); i++) {
			if (startTime < mVR_timeVector.get(i).md_endTime) {
				if (startTime > mVR_timeVector.get(i).md_startTime) {
					Resource ar = new Resource(mVR_timeVector.get(i));
					mVR_timeVector.insertElementAt(ar, i);
					mVR_timeVector.get(i).md_endTime = startTime;
					mVR_timeVector.get(i+1).md_startTime = startTime;
				} else if (endTime < mVR_timeVector.get(i).md_endTime) {
					Resource ar = new Resource(mVR_timeVector.get(i));
					mVR_timeVector.insertElementAt(ar, i);
					mVR_timeVector.get(i).md_endTime = endTime;
					mVR_timeVector.get(i+1).md_startTime = endTime;
//					mVR_timeVector.get(i).mad_procUtils[machID] -= procUtil;
					mVR_timeVector.get(i).mai_coreNums[machID] -= coreNum;
					mVR_timeVector.get(i).mai_memSizes[machID] -= memSize;
					if (i > 0) {
						mergeWithSucc(i - 1);
					}
					return true;
				} else {
					startTime = mVR_timeVector.get(i).md_endTime;
//					mVR_timeVector.get(i).mad_procUtils[machID] -= procUtil;
					mVR_timeVector.get(i).mai_coreNums[machID] -= coreNum;
					mVR_timeVector.get(i).mai_memSizes[machID] -= memSize;
					if (i > 0 && mergeWithSucc(i - 1)) {
						i--;
					}
					if (startTime == endTime) {
						if (i < mVR_timeVector.size() - 1) {
							mergeWithSucc(i);
						}
						return true;
					}
				}
			}
		}
		return false;
	}
	
	private boolean mergeWithSucc(int index) {
		boolean merge = true;
		for (int i = 0; i < mVR_timeVector.get(index).mi_machNum; i++) {
//			if (mVR_timeVector.get(index).mad_procUtils[i] != mVR_timeVector.get(index + 1).mad_procUtils[i] 
			if (mVR_timeVector.get(index).mai_coreNums[i] != mVR_timeVector.get(index + 1).mai_coreNums[i] 
					|| mVR_timeVector.get(index).mai_memSizes[i] != mVR_timeVector.get(index + 1).mai_memSizes[i]) {
				merge = false;
				break;
			}
		}
		if (merge) {
			mVR_timeVector.get(index).md_endTime = mVR_timeVector.get(index + 1).md_endTime;
			mVR_timeVector.removeElementAt(index + 1);
			return true;
		}
		return false;
	}
	
	public int getTimeSlotIndex(double time) {
		if (time < 0.0) {
			return Constant.uncertainInt;
		}
		for (int i = 0; i < mVR_timeVector.size(); i++) {
			if (time < mVR_timeVector.get(i).getEndTime()) {
				return i;
			}
		}
		return Constant.uncertainInt;
	}
	
	public int getTimeSlotIndex(double time, int startTimeSlotIndex) {
		if (time < mVR_timeVector.get(startTimeSlotIndex).getStartTime()) {
			return Constant.uncertainInt;
		}
		for (int i = startTimeSlotIndex; i < mVR_timeVector.size(); i++) {
			if (time < mVR_timeVector.get(i).getEndTime()) {
				return i;
			}
		}
		return Constant.uncertainInt;
	}
	
	public int getReverseTimeSlotIndex(double time) {
		if (time < 0.0) {
			return Constant.uncertainInt;
		}
		for (int i = 0; i < mVR_timeVector.size(); i++) {
			if (time <= mVR_timeVector.get(i).getEndTime()) {
				return i;
			}
		}
		return Constant.uncertainInt;
	}
	
	public int getReverseTimeSlotIndex(double time, int startTimeSlotIndex) {
		if (time < mVR_timeVector.get(startTimeSlotIndex).getStartTime()) {
			return Constant.uncertainInt;
		}
		for (int i = startTimeSlotIndex; i < mVR_timeVector.size(); i++) {
			if (time <= mVR_timeVector.get(i).getEndTime()) {
				return i;
			}
		}
		return Constant.uncertainInt;
	}
	
	public void print() {
		System.out.println("mVR_timeVector:");
		for (int i = 0; i < mVR_timeVector.size(); i++) {
			System.out.println("    " + i + ": [" + mVR_timeVector.get(i).md_startTime + ", " + mVR_timeVector.get(i).md_endTime + "] :");
			for (int j = 0; j < mVR_timeVector.get(i).mi_machNum; j++) {
				System.out.println("        " + j + " : " + mVR_timeVector.get(i).mai_coreNums[j] + ", " + mVR_timeVector.get(i).mai_memSizes[j]);
			}
		}
	}
	/*
	public static void main(String args[]) {
		HeteroCluster hc = new HeteroCluster(16, HeteroCluster.homoCluster);
		TimeResourceTable trt = new TimeResourceTable(hc);
		trt.print();
		int machID = trt.getAvailMachID(10.0, 15.0, 1.0, 4096, hc.getHomoCluster(0), 0);
		if (machID >= 0) {
			trt.reserve(10.0, 15.0, machID, 1.0, 4096, 0);
		}
		System.out.println("trt.reserve(10.0, 15.0, " + machID + ", 1.0, 4096)");
		trt.print();
		machID = trt.getAvailMachID(10.0, 15.0, 1.0, 4096, hc.getHomoCluster(0), 0);
		if (machID >= 0) {
			trt.reserve(10.0, 15.0, machID, 1.0, 4096, 0);
		}
		System.out.println("trt.reserve(10.0, 15.0, " + machID + ", 1.0, 4096)");
		trt.print();
		machID = trt.getAvailMachID(0.0, 5.0, 1.0, 4096, hc.getHomoCluster(0), 0);
		if (machID >= 0) {
			trt.reserve(0.0, 5.0, machID, 1.0, 4096, 0);
		}
		System.out.println("trt.reserve(0.0, 5.0, " + machID + ", 1.0, 4096)");
		trt.print();
		machID = trt.getAvailMachID(0.0, 3.0, 1.0, 4096, hc.getHomoCluster(0), 0);
		if (machID >= 0) {
			trt.reserve(0.0, 3.0, machID, 1.0, 4096, 0);
		}
		System.out.println("trt.reserve(0.0, 3.0, " + machID + ", 1.0, 4096)");
		trt.print();
		machID = trt.getAvailMachID(7.0, 10.0, 1.0, 4096, hc.getHomoCluster(0), 0);
		if (machID >= 0) {
			trt.reserve(7.0, 10.0, machID, 1.0, 4096, 0);
		}
		System.out.println("trt.reserve(7.0, 10.0, " + machID + ", 1.0, 4096)");
		trt.print();
		machID = trt.getAvailMachID(8.0, 12.0, 1.0, 4096, hc.getHomoCluster(0), 0);
		if (machID >= 0) {
			trt.reserve(8.0, 12.0, machID, 1.0, 4096, 0);
		}
		System.out.println("trt.reserve(8.0, 12.0, " + machID + ", 1.0, 4096)");
		trt.print();
		machID = trt.getAvailMachID(9.0, 20.0, 1.0, 4096, hc.getHomoCluster(0), 0);
		if (machID >= 0) {
			trt.reserve(9.0, 20.0, machID, 1.0, 4096, 0);
		}
		System.out.println("trt.reserve(9.0, 20.0, " + machID + ", 1.0, 4096)");
		trt.print();
		machID = trt.getAvailMachID(5.0, 7.0, 1.0, 4096, hc.getHomoCluster(0), 0);
		if (machID >= 0) {
			trt.reserve(5.0, 7.0, machID, 1.0, 4096, 0);
		}
		System.out.println("trt.reserve(5.0, 7.0, " + machID + ", 1.0, 4096)");
		trt.print();
		trt.reserve(10.0, 12.0, 0, -1.0, -4096, 0);
		System.out.println("trt.reserve(10.0, 12.0, 0, -1.0, -4096)");
		trt.print();
		machID = trt.getAvailMachID(8.0, 15.0, 13.0, 4096, hc.getHomoCluster(0), 0);
		if (machID >= 0) {
			trt.reserve(8.0, 15.0, machID, 13.0, 4096, 0);
		}
		System.out.println("trt.reserve(8.0, 15.0, " + machID + ", 13.0, 4096)");
		trt.print();
		machID = trt.getAvailMachID(8.0, 9.0, 12.0, 49152, hc.getHomoCluster(0), 0);
		if (machID >= 0) {
			trt.reserve(8.0, 9.0, machID, 12.0, 49152, 0);
		}
		System.out.println("trt.reserve(8.0, 9.0, " + machID + ", 12.0, 49152)");
		trt.print();
		machID = trt.getAvailMachID(8.0, 9.0, 1.0, 4096, hc.getHomoCluster(0), 0);
		if (machID >= 0) {
			trt.reserve(8.0, 9.0, machID, 1.0, 4096, 0);
		}
		System.out.println("trt.reserve(8.0, 9.0, " + machID + ", 1.0, 4096)");
		trt.print();
		machID = trt.getAvailMachID(9.0, 15.0, 12.0, 49152, hc.getHomoCluster(0), 0);
		if (machID >= 0) {
			trt.reserve(9.0, 15.0, machID, 12.0, 49152, 0);
		}
		System.out.println("trt.reserve(9.0, 15.0, " + machID + ", 12.0, 49152)");
		trt.print();
		trt.reserve(2.0, 3.0, 0, -1.0, -4096, 0);
		System.out.println("trt.reserve(2.0, 3.0, 0, -1.0, -4096)");
		trt.print();
	}
	
	public int isAvailable(double startTime, double endTime, double procUtil, int memSize) {
		for (int i = 0; i < mVR_timeVector.get(0).mi_machNum; i++) {
			for (int j = 0; j < mVR_timeVector.size(); j++) {
				if (startTime < mVR_timeVector.get(j).md_endTime) {
					if (mVR_timeVector.get(j).mad_procUtil[i] < procUtil 
							|| mVR_timeVector.get(j).mai_memSize[i] < memSize) {
						break;
					}
					if (endTime <= mVR_timeVector.get(j).md_endTime) {
						return i;
					}
				}
			}
		}
		return -1;
	}
	
	public TimeResourceTable() {
		mVR_timeVector = new Vector<Resource>();
		Resource ar = new Resource();
		ar.md_endTime = simulTime;
		mVR_timeVector.add(ar);
	}
	
	public boolean isAvailable(double startTime, double endTime, int resUnit) {
		for (int i = 0; i < mVR_timeVector.size(); i++) {
			if (startTime < mVR_timeVector.get(i).md_endTime) {
				if (mVR_timeVector.get(i).mi_resUnit < resUnit) {
					return false;
				}
				if (endTime <= mVR_timeVector.get(i).md_endTime) {
					return true;
				}
			}
		}
		return false;
	}
	
	public boolean reserve(double startTime, double endTime, int resUnit) {
		if (resUnit == 0) {
			return true;
		}
		for (int i = 0; i < mVR_timeVector.size(); i++) {
			if (startTime < mVR_timeVector.get(i).md_endTime) {
				if (startTime > mVR_timeVector.get(i).md_startTime) {
					Resource ar = new Resource(mVR_timeVector.get(i));
					mVR_timeVector.insertElementAt(ar, i);
					mVR_timeVector.get(i).md_endTime = startTime;
					mVR_timeVector.get(i+1).md_startTime = startTime;
				} else if (endTime < mVR_timeVector.get(i).md_endTime) {
					Resource ar = new Resource(mVR_timeVector.get(i));
					mVR_timeVector.insertElementAt(ar, i);
					mVR_timeVector.get(i).md_endTime = endTime;
					mVR_timeVector.get(i+1).md_startTime = endTime;
					mVR_timeVector.get(i).mi_resUnit -= resUnit;
					if (i > 0 && mVR_timeVector.get(i).mi_resUnit == mVR_timeVector.get(i-1).mi_resUnit) {
						mVR_timeVector.get(i-1).md_endTime = mVR_timeVector.get(i).md_endTime;
						mVR_timeVector.removeElementAt(i);
					}
					return true;
				} else {
					startTime = mVR_timeVector.get(i).md_endTime;
					mVR_timeVector.get(i).mi_resUnit -= resUnit;
					if (i > 0 && mVR_timeVector.get(i).mi_resUnit == mVR_timeVector.get(i-1).mi_resUnit) {
						mVR_timeVector.get(i-1).md_endTime = mVR_timeVector.get(i).md_endTime;
						mVR_timeVector.removeElementAt(i);
						i--;
					}
					if (startTime == endTime) {
						if (i < mVR_timeVector.size() - 1 && mVR_timeVector.get(i).mi_resUnit == mVR_timeVector.get(i+1).mi_resUnit) {
							mVR_timeVector.get(i).md_endTime = mVR_timeVector.get(i+1).md_endTime;
							mVR_timeVector.removeElementAt(i+1);
						}
						return true;
					}
				}
			}
		}
		return false;
	}
	
	public void print() {
		System.out.println("mVR_timeVector:");
		for (int i = 0; i < mVR_timeVector.size(); i++) {
			System.out.println("    " + i + ": [" + mVR_timeVector.get(i).md_startTime 
					+ ", " + mVR_timeVector.get(i).md_endTime 
					+ "] : " + mVR_timeVector.get(i).mi_resUnit);
		}
	}
	
	public static void main(String args[]) {
		TimeResourceTable trt = new TimeResourceTable();
		trt.print();
		if (trt.isAvailable(10.0, 15.0, 1)) {
			trt.reserve(10.0, 15.0, 1);
		}
		trt.print();
		if (trt.isAvailable(10.0, 15.0, 1)) {
			trt.reserve(10.0, 15.0, 1);
		}
		trt.print();
		if (trt.isAvailable(0.0, 5.0, 1)) {
			trt.reserve(0.0, 5.0, 1);
		}
		trt.print();
		if (trt.isAvailable(0.0, 3.0, 1)) {
			trt.reserve(0.0, 3.0, 1);
		}
		trt.print();
		if (trt.isAvailable(7.0, 10.0, 1)) {
			trt.reserve(7.0, 10.0, 1);
		}
		trt.print();
		if (trt.isAvailable(8.0, 12.0, 1)) {
			trt.reserve(8.0, 12.0, 1);
		}
		trt.print();
		if (trt.isAvailable(9.0, 20.0, 1)) {
			trt.reserve(9.0, 20.0, 1);
		}
		trt.print();
		if (trt.isAvailable(5.0, 7.0, 1)) {
			trt.reserve(5.0, 7.0, 1);
		}
		trt.print();
		if (trt.isAvailable(10.0, 12.0, -1)) {
			trt.reserve(10.0, 12.0, -1);
		}
		trt.print();
		if (trt.isAvailable(8.0, 15.0, 190)) {
			trt.reserve(8.0, 15.0, 190);
		}
		trt.print();
	}
	*/
}
