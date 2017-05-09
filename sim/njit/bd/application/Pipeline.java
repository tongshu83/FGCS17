package njit.bd.application;

import njit.bd.common.Constant;
import njit.bd.dataCenter.HeteroCluster;
import njit.bd.dataCenter.HomoCluster;

public class Pipeline {
	int mi_jobNum = 0;
	int[] mai_jobs;
	
	double md_est = Constant.uncertainDouble;
	double md_lft = Constant.uncertainDouble;
	
	public Pipeline() { }
	public Pipeline(int jobNum) {
		mi_jobNum = jobNum;
		mai_jobs = new int[jobNum];
	}
	
	public Pipeline getSubPipeline(int startIndex, int endIndex) {
		if (startIndex > endIndex || startIndex < 0 || endIndex > mi_jobNum - 1) {
			return null;
		}
		Pipeline subPl = new Pipeline(endIndex - startIndex + 1);
		for (int i = 0; i < subPl.mi_jobNum; i++) {
			subPl.mai_jobs[i] =  mai_jobs[startIndex + i];
		}
		if (startIndex == 0) {
			subPl.md_est = md_est;
		}
		if (endIndex == mi_jobNum - 1) {
			subPl.md_lft = md_lft;
		}
		return subPl;
	}
	
	public double getExecTime(Workflow wf, int[] taskNums, HomoCluster hc) {
		double minExecTime = 0.0;
		for (int i = 0; i < mi_jobNum; i++) {
			minExecTime += wf.getJob(mai_jobs[i]).getTaskExecTime(taskNums[i], hc);
		}
		return minExecTime;
	}
	
	public double getExecTime(Workflow wf, int[] taskNums, HeteroCluster hc) {
		double minExecTime = 0.0;
		for (int i = 0; i < mi_jobNum; i++) {
			minExecTime += wf.getJob(mai_jobs[i]).getTaskExecTime(taskNums[i], hc);
		}
		return minExecTime;
	}
	
	public int getJobNum() {
		return mi_jobNum;
	}
	
	public int getJobID(int index) {
		return mai_jobs[index];
	}
	
	public void setJobID(int index, int jobID) {
		mai_jobs[index] = jobID;
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
	
	public void print() {
		System.out.println("Pipeline: jobNum = " + mi_jobNum);
		for (int i = 0; i < mi_jobNum; i++) {
			System.out.println("    mai_jobs[" + i + "] = " + mai_jobs[i]);
		}
		System.out.println("    md_est = " + md_est);
		System.out.println("    md_lft = " + md_lft);
	}
}
