package njit.bd.application;

import java.util.Random;
import java.util.Vector;

import njit.bd.common.Constant;
import njit.bd.dataCenter.HeteroCluster;

public class WorkflowGenerator {
	double md_simulTime = 86400.0; // one day // 604800.0; // one week // 2592000.0; // one month in seconds
	int mi_wfNum = 0;
	Workflow[] maW_workflows;
	
	public WorkflowGenerator(int minJobNum, int maxJobNum, double avgArrivalInterval, double simulTime, int wfShape, HeteroCluster hc) {
		
		md_simulTime = simulTime;
		Vector<Double> timeSeries = new Vector<Double>();
		Random rand = new Random();
		double currTime = 0.0;
		while (currTime < md_simulTime) {
			timeSeries.add(new Double(currTime));
			currTime += - avgArrivalInterval * Math.log(1 - rand.nextDouble());
		}
		mi_wfNum = timeSeries.size();
		maW_workflows = new Workflow[mi_wfNum];
		for (int i = 0; i < mi_wfNum; i++) {
			int jobNum = minJobNum + rand.nextInt(maxJobNum - minJobNum + 1);
			maW_workflows[i] = new Workflow(i, jobNum, wfShape, timeSeries.get(i).doubleValue(), hc);
		}
	}
	
	public void clear() {
		for (int i = 0; i < mi_wfNum; i++) {
			Workflow wf = maW_workflows[i];
			wf.setCurrTask(null);
			wf.setTaskRank(0.0);
			wf.setSchedTime(Constant.uncertainDouble);
			int jobNum = maW_workflows[i].getJobNum();
			for (int j = 0; j < jobNum; j++) {
				MoldableJob job = wf.getJob(j);
				job.setEST(Constant.uncertainDouble);
				job.setLFT(Constant.uncertainDouble);
				job.setTempEST(Constant.uncertainDouble);
				job.setTempLFT(Constant.uncertainDouble);
				job.setASFT(Constant.uncertainDouble, Constant.uncertainDouble);
				job.setTaskNum(0);
				job.setVirtDeadline(Constant.uncertainDouble);
			}
			wf.getJob(0).setEST(maW_workflows[i].getReadyTime());
			wf.getJob(jobNum - 1).setLFT(maW_workflows[i].getDeadline());
		}
	}
	
	public void scaleDeadline(double timeScale) {
		for (int i = 0; i < mi_wfNum; i++) {
			double readyTime = maW_workflows[i].getReadyTime();
			double deadline = readyTime + (maW_workflows[i].getDeadlineBaseline() - readyTime) * timeScale;
			maW_workflows[i].setDeadline(deadline);
		}
	}
	
	public double getMissedDeadlineRate() {
		int num = 0;
		for (int i = 0; i < mi_wfNum; i++) {
			if (maW_workflows[i].getJob(maW_workflows[i].getJobNum() - 1).getAFT() > maW_workflows[i].getDeadline()) {
				num++;
			}
		}
		return (double) num / (double) mi_wfNum;
	}
	
	public double getAvgDelay() {
		double delay = 0.0;
		for (int i = 0; i < mi_wfNum; i++) {
			double aft = maW_workflows[i].getJob(maW_workflows[i].getJobNum() - 1).getAFT();
			double deadline = maW_workflows[i].getDeadline();
			if (aft > deadline) {
				delay += aft - deadline;
			}
		}
		return delay / (double) mi_wfNum;
	}
	
	public double getAvgTaskNum() {
		int totalTaskNum = 0;
		int totalJobNum = 0;
		for (int i = 0; i < mi_wfNum; i++) {
			Workflow wf = maW_workflows[i];
			int jobNum = wf.getJobNum();
			for (int j = 0; j < jobNum; j++) {
				totalJobNum++;
				totalTaskNum += wf.getJob(j).getTaskNum();
			}
		}
		return totalTaskNum / (double) totalJobNum;
	}
	
	public double getWlRed() {
		double maxWorkload = 0.0;
		double actWorkload = 0.0;
		for (int i = 0; i < mi_wfNum; i++) {
			Workflow wf = maW_workflows[i];
			int jobNum = wf.getJobNum();
			for (int j = 0; j < jobNum; j++) {
				MoldableJob job = wf.getJob(j);
				maxWorkload += job.getJobWorkload(job.getMaxTaskNum());
				actWorkload += job.getJobWorkload(job.getTaskNum());
			}
		}
		return (maxWorkload - actWorkload) / maxWorkload;
	}
	
	public int getWfNum() {
		return mi_wfNum;
	}
	
	public Workflow getWorkflow(int wfID) {
		if (wfID < mi_wfNum && wfID >= 0) {
			return maW_workflows[wfID];
		} else {
			return null;
		}
	}
	
	public void setWorkflow(int wfID, Workflow wf) {
		if (wfID < mi_wfNum && wfID >= 0) {
			maW_workflows[wfID] = wf;
		} else {
			System.out.println("wfID = " + wfID + " !");
		}
	}
}
