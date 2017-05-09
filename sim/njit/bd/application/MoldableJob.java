package njit.bd.application;

import java.util.Random;

import njit.bd.common.Constant;
import njit.bd.dataCenter.HeteroCluster;
import njit.bd.dataCenter.HomoCluster;

public class MoldableJob extends JobIndexPair{
	public final static int maxTaskNumUpperBound = 120;
	final static int maxTaskNumLowerBound = 30;
	final static double minUnsplitWorkload = 600.0;
	final static double maxUnsplitWorkload = 21600.0;
	final static double minProcUtil = 0.8;
	final static double maxProcUtil = 1.0;
	final static int minMemSize = 512;
	final static int maxMemSize = 4096;
	
	int mi_maxTaskNum = 0;
	double[] mai_jobWorkloads;
	double[] md_taskProcUtils;
	int mi_taskMemSize = 0;
	int mi_taskNum = 0;
	Task[] maT_tasks = null;
	
	double md_est = Constant.uncertainDouble;
	double md_lft = Constant.uncertainDouble;
	double md_ast = Constant.uncertainDouble;
	double md_aft = Constant.uncertainDouble;
	double md_tempEST = Constant.uncertainDouble;
	double md_tempLFT = Constant.uncertainDouble;
	double md_bestLFT = Constant.uncertainDouble;
	double md_virtDl = Constant.uncertainDouble;
	
	int md_currTaskID = 0;
	double md_schedTime = Constant.uncertainDouble;
	double md_taskPriority = 0.0;
	
	public MoldableJob(int wfID, int jobID) {
		mi_wfID = wfID;
		mi_jobID = jobID;
		
		Random rand = new Random();
		mi_maxTaskNum = maxTaskNumLowerBound + rand.nextInt(maxTaskNumUpperBound - maxTaskNumLowerBound + 1);
		
		mai_jobWorkloads = new double[mi_maxTaskNum];
		mai_jobWorkloads[0] = minUnsplitWorkload + (maxUnsplitWorkload - minUnsplitWorkload) * rand.nextDouble();
		double slope = 0.009 + 0.004 * rand.nextDouble();
		for (int i = 1; i < mi_maxTaskNum; i++) {
//			double minWorkload = mai_jobWorkloads[i - 1];
//			double maxWorkload = mai_jobWorkloads[i - 1] * (i + 1) / i;
//			double diff = maxWorkload - minWorkload;
//			minWorkload = minWorkload + diff * 0.1;
//			maxWorkload = maxWorkload - diff * 0.6;
			
			double minWorkload = mai_jobWorkloads[0] * ((i - 0.4) * slope + 1);
			double maxWorkload = mai_jobWorkloads[0] * ((i + 0.4) * slope + 1);
			mai_jobWorkloads[i] = minWorkload + (maxWorkload - minWorkload) * rand.nextDouble();
		}
		
		md_taskProcUtils = new double[HeteroCluster.clusterNum];
		for (int i = 0; i < HeteroCluster.clusterNum; i++) {
			md_taskProcUtils[i] = minProcUtil + 0.1 * rand.nextInt((int) Math.floor((maxProcUtil - minProcUtil) / 0.1) + 1);
		}
		mi_taskMemSize = minMemSize + minMemSize * rand.nextInt(maxMemSize / minMemSize);
	}
	
	public boolean isMapped() {
		if (md_ast >= 0.0 && md_aft >= 0.0 && mi_taskNum > 0) {
			return true;
		} else {
			return false;
		}
	}
	
	public void updateASFT() {
		for (int i = 0; i < mi_taskNum; i++) {
			if (maT_tasks[i].isMapped()) {
				if (md_ast > maT_tasks[i].getStartTime() || md_ast == Constant.uncertainDouble) {
					md_ast = maT_tasks[i].getStartTime();
				}
				if (md_aft < maT_tasks[i].getEndTime() || md_aft == Constant.uncertainDouble) {
					md_aft = maT_tasks[i].getEndTime();
				}
			} else {
				md_ast = Constant.uncertainDouble;
				md_aft = Constant.uncertainDouble;
				break;
			}
		}
	}
	
	public double getTaskExecTime(int taskNum, HomoCluster hc) {
		return mai_jobWorkloads[taskNum - 1] / (taskNum * hc.getProcSpeed() * md_taskProcUtils[hc.getClusterID()]);
	}
	
	public double getTaskExecTime(int taskNum, HeteroCluster hc) {
		double avgExecTime = 0.0;
		for (int i = 0; i < hc.getClusterNum(); i++) {
			double execTime = mai_jobWorkloads[taskNum - 1] / (taskNum * hc.getHomoCluster(i).getProcSpeed() * md_taskProcUtils[i]);
			avgExecTime += execTime * hc.getHomoCluster(i).getMachNum();
		}
		avgExecTime /= hc.getMachNum();
		return avgExecTime;
	}
	
	public double getTaskExecTime(int minTaskNum, int maxTaskNum, HeteroCluster hc) {
		double avgExecTime = 0.0;
		for (int i = minTaskNum; i <= maxTaskNum; i++) {
			avgExecTime += getTaskExecTime(i, hc);
		}
		avgExecTime /= maxTaskNum - minTaskNum + 1;
		return avgExecTime;
	}
	
	public double getMinTaskExecTime(int taskNum, HeteroCluster hc) {
		double minExecTime = Constant.uncertainDouble;
		for (int i = 0; i < hc.getClusterNum(); i++) {
			double execTime = mai_jobWorkloads[taskNum - 1] / (taskNum * hc.getHomoCluster(i).getProcSpeed() * md_taskProcUtils[i]);
			if (minExecTime == Constant.uncertainDouble || minExecTime > execTime) {
				minExecTime = execTime;
			}
		}
		return minExecTime;
	}
	
	public double getJobDynEnergy(int taskNum, HomoCluster hc) {
		return hc.getDynPower() * mai_jobWorkloads[taskNum - 1] / hc.getProcSpeed();
	}
	
	public double getJobDynEnergy(int taskNum, HeteroCluster hc) {
		double avgDynEnergy = 0.0;
		for (int i = 0; i < hc.getClusterNum(); i++) {
			HomoCluster homoC = hc.getHomoCluster(i);
			double dynEnergy = getJobDynEnergy(taskNum, homoC);
			avgDynEnergy += dynEnergy * hc.getHomoCluster(i).getMachNum();
		}
		avgDynEnergy /= hc.getMachNum();
		return avgDynEnergy;
	}
	
	public double estiJobStaEnergy(int taskNum, HomoCluster hc) {
		double procLimit = hc.getCoreNum();
		double memLimit = hc.getMemSize() / (double) mi_taskMemSize;
		double slotNum;
		if (procLimit < memLimit) {
			slotNum = procLimit;
		} else {
			slotNum = memLimit;
		}
		return hc.getStaPower() * mai_jobWorkloads[taskNum - 1] / (hc.getProcSpeed() * md_taskProcUtils[hc.getClusterID()] * slotNum);
	}
	
	public double estiJobStaEnergy(int taskNum, HeteroCluster hc) {
		double avgStaEnergy = 0.0;
		for (int i = 0; i < hc.getClusterNum(); i++) {
			HomoCluster homoC = hc.getHomoCluster(i);
			double staEnergy = estiJobStaEnergy(taskNum, homoC);
			avgStaEnergy += staEnergy * hc.getHomoCluster(i).getMachNum();
		}
		avgStaEnergy /= hc.getMachNum();
		return avgStaEnergy;
	}
	
	public boolean isLastTask(int taskID) {
		if (taskID < mi_taskNum - 1) {
			return false;
		}
		return true;
	}
	
	public double estiTaskLFT(int taskID, double deadline) {
		double taskLFT = md_est + (deadline - md_est) * (taskID + 1) / mi_taskNum;
		return taskLFT;
	}
	
	/* begin: used for MapperMinDEEDA */
	public void initProgress(double period, HeteroCluster hc) {
		md_currTaskID = 0;
		if (md_est >= 0.0) {
			md_schedTime = Math.ceil(md_est / period) * period;
		} else {
			md_schedTime = Constant.uncertainDouble;
		}
		md_taskPriority = md_virtDl - mi_taskNum * getTaskExecTime(mi_taskNum, hc);
	}
	
	public boolean nextTask(double period, double currTime, HeteroCluster hc) {
		if (md_currTaskID >= mi_taskNum - 1) {
			return false;
		}
		md_currTaskID++;
		
		if (md_schedTime < currTime) {
			md_schedTime = currTime;
		}
		
		md_taskPriority = md_virtDl - (mi_taskNum - md_currTaskID) * getTaskExecTime(mi_taskNum, hc);
		return true;
	}
	/* end: used for MapperMinDEEDA */
	
	public int getMaxTaskNum() {
		return mi_maxTaskNum;
	}
	
	public double getJobWorkload(int taskNum) {
		if (taskNum > 0 && taskNum <= mi_maxTaskNum) {
			return mai_jobWorkloads[taskNum - 1];
		} else {
			System.out.println("taskNum <= 0 || taskNum > mi_maxTaskNum !");
			return Constant.uncertainDouble;
		}
	}
	
	public double getTaskProcUtil(int homoClusterID) {
		return md_taskProcUtils[homoClusterID];
	}
	
	public int getTaskMemSize() {
		return mi_taskMemSize;
	}
	
	public int getTaskNum() {
		return mi_taskNum;
	}
	
	public void setTaskNum(int taskNum) {
		if (taskNum > 0) {
			mi_taskNum = taskNum;
			maT_tasks = new Task[mi_taskNum];
			for (int i = 0; i < mi_taskNum; i++) {
				maT_tasks[i] = new Task(mi_wfID, mi_jobID, i);
			}
		} else {
			mi_taskNum = 0;
			maT_tasks = null;
		}
	}
	
	public Task getTask(int index) {
		return maT_tasks[index];
	}
	
	public double getEST() {
		return md_est;
	}
	
	public void setEST(double est) {
		md_est = est;
	}
	
	public double getLFT() {
		return md_lft;
	}
	
	public void setLFT(double lft) {
		md_lft = lft;
	}
	
	public double getTempEST() {
		return md_tempEST;
	}
	
	public void setTempEST(double tempEST) {
		md_tempEST = tempEST;
	}
	
	public double getTempLFT() {
		return md_tempLFT;
	}
	
	public void setTempLFT(double tempLFT) {
		md_tempLFT = tempLFT;
	}
	
	public double getAST() {
		return md_ast;
	}
	
	public double getAFT() {
		return md_aft;
	}
	
	public void setASFT(double ast, double aft) {
		md_ast = ast;
		md_aft = aft;
	}
	
	public double getBestLFT() {
		return md_bestLFT;
	}
	
	public void setBestLFT(double bestLFT) {
		md_bestLFT = bestLFT;
	}
	
	public double getVirtDeadline() {
		return md_virtDl;
	}
	
	public void setVirtDeadline(double virtDl) {
		md_virtDl = virtDl;
	}
	
	public int getCurrTaskID() {
		return md_currTaskID;
	}
	
	public void setCurrTaskID(int currTaskID) {
		md_currTaskID = currTaskID;
	}
	
	public double getSchedTime() {
		return md_schedTime;
	}
	
	public void setSchedTime(double schedTime) {
		md_schedTime = schedTime;
	}
	
	public double getTaskPriority() {
		return md_taskPriority;
	}
	
	public void setTaskPriority(double taskPriority) {
		md_taskPriority = taskPriority;
	}
}
