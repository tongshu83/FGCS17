package njit.bd.application;

import java.util.Vector;

import njit.bd.common.Constant;

public class JobPriorityQueue {
	public final static int EST = 0;    // used by PSciMapper
	public final static int TaskPriority = 1;    // used by MapperMinDEEDA
	
	Vector<JobIndexPair> mVJ_jobQueue;
	WorkflowGenerator mW_wfg;
	
	public JobPriorityQueue(WorkflowGenerator wfg) {
		mVJ_jobQueue = new Vector<JobIndexPair>();
		mW_wfg = wfg;
	}
	
	public void enqueue(JobIndexPair jobIndexPair) {
		mVJ_jobQueue.add(jobIndexPair);
	}
	
	public JobIndexPair dequeue(int type) {
		int tailIndex = mVJ_jobQueue.size() - 1;
		for (int i = 0; i < tailIndex; i++) {
			JobIndexPair tailJobIndexPair = mVJ_jobQueue.get(tailIndex);
			JobIndexPair currJobIndexPair = mVJ_jobQueue.get(i);
			if (compare(currJobIndexPair, tailJobIndexPair, type)) {
				JobIndexPair temp = tailJobIndexPair;
				mVJ_jobQueue.set(tailIndex, currJobIndexPair);
				mVJ_jobQueue.set(i, temp);
			}
		}
		JobIndexPair jobIndexPair = mVJ_jobQueue.get(tailIndex);
		mVJ_jobQueue.remove(tailIndex);
		return jobIndexPair;
	}
	
	private boolean compare(JobIndexPair jobIdx1, JobIndexPair jobIdx2, int type) {
		if (type == EST) {
			double jobEST1 = mW_wfg.getWorkflow(jobIdx1.getWfID()).getJob(jobIdx1.getJobID()).getEST();
			double jobEST2 = mW_wfg.getWorkflow(jobIdx2.getWfID()).getJob(jobIdx2.getJobID()).getEST();
			if (jobEST1 != Constant.uncertainDouble && (jobEST2 == Constant.uncertainDouble || jobEST1 < jobEST2)) {
				return true;
			} else {
				return false;
			}
		}
		
		if (type == TaskPriority) {
			double jobSchedTime1 = mW_wfg.getWorkflow(jobIdx1.getWfID()).getJob(jobIdx1.getJobID()).getSchedTime();
			double taskPriority1 = mW_wfg.getWorkflow(jobIdx1.getWfID()).getJob(jobIdx1.getJobID()).getTaskPriority();
			double jobSchedTime2 = mW_wfg.getWorkflow(jobIdx2.getWfID()).getJob(jobIdx2.getJobID()).getSchedTime();
			double taskPriority2 = mW_wfg.getWorkflow(jobIdx2.getWfID()).getJob(jobIdx2.getJobID()).getTaskPriority();
			if (jobSchedTime1 != Constant.uncertainDouble && (jobSchedTime2 == Constant.uncertainDouble || jobSchedTime1 < jobSchedTime2)
					|| jobSchedTime1 == jobSchedTime2 && taskPriority1 < taskPriority2) {
				return true;
			} else {
				return false;
			}
		}
		
		return true;
	}
	
	public int size() {
		return mVJ_jobQueue.size();
	}
}
