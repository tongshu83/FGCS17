package njit.bd.hadoopYARN;

import njit.bd.application.MoldableJob;
import njit.bd.application.Task;
import njit.bd.application.TaskIndexTriple;
import njit.bd.application.Workflow;
import njit.bd.application.WorkflowGenerator;
import njit.bd.application.WorkflowPriorityQueue;
import njit.bd.common.Constant;
import njit.bd.common.DblDblIntIntQuad;
import njit.bd.dataCenter.HeteroCluster;
import njit.bd.dataCenter.HomoCluster;

public class MapperEEDAW extends ResourceManager {
	double md_period = 600.0;    // in seconds
	
	public MapperEEDAW(HeteroCluster hc, double period) {
		mH_hc = hc;
		mT_trt = new TimeResourceTable(hc);
		md_period = period;
	}
	
	public void schedule(WorkflowGenerator wfg) {
		for (int i = 0; i < wfg.getWfNum(); i++) {
			Workflow wf = wfg.getWorkflow(i);
			for (int j = 0; j < wf.getJobNum(); j++) {
				MoldableJob job = wf.getJob(j);
				job.setTaskNum(job.getMaxTaskNum());
			}
			wf.initProgress(md_period, mH_hc);
		}
		WorkflowPriorityQueue wfPriorityQueue = new WorkflowPriorityQueue(wfg);
		for (int i = 0; i < wfg.getWfNum(); i++) {
			wfPriorityQueue.enqueue(i, WorkflowPriorityQueue.TaskPriority);
		}
		double currTime = 0.0;
		int currTSI = 0;
		while (wfPriorityQueue.size() > 0) {
			int wfID = wfPriorityQueue.dequeue(WorkflowPriorityQueue.TaskPriority);
			Workflow wf = wfg.getWorkflow(wfID);
			TaskIndexTriple tit = wf.getCurrTask();
			int jobID = tit.getJobID();
			int taskID = tit.getTaskID();
			currTime = wf.getSchedTime();
			currTSI = mT_trt.getReverseTimeSlotIndex(currTime, currTSI);
			
			DblDblIntIntQuad mapScheme = new DblDblIntIntQuad(0.0, 0.0, 0, 0);
			if (!scheduleTaskEE(wf, jobID, taskID, currTime, currTSI, mapScheme)) {
				scheduleTaskMT(wf, jobID, taskID, currTime, currTSI, mapScheme);
			}
			MoldableJob job = wf.getJob(jobID);
			taskSched(job, taskID, mapScheme);
			if (job.isLastTask(taskID)) {
				job.updateASFT();
				wf.updateJobEST();
			}
			if (wf.nextTask(md_period, currTime, mH_hc)) {
				wfPriorityQueue.enqueue(wfID, WorkflowPriorityQueue.TaskPriority);
			} else {
				wf.updateJobLFT();
			}
		}
	}
	
	private boolean scheduleTaskEE(Workflow wf, int jobID, int taskID, double currTime, int currTSI, DblDblIntIntQuad mapScheme) {
		MoldableJob job = wf.getJob(jobID);
		double taskLFT = wf.estiJobLFT(jobID, mH_hc);    // This is critical for the adaption of this algorithm
		
		double jobEST = job.getEST();
		if (jobEST < currTime) {
			jobEST = currTime;
		}
		int startTimeSlotIndex = mT_trt.getTimeSlotIndex(jobEST, currTSI);
		double minEnergy = Constant.uncertainDouble;
		double startTimeMinEnergy = Constant.uncertainDouble;
		double endTimeMinEnergy = Constant.uncertainDouble;
		int machMinEnergy = Constant.uncertainInt;
		int startTimeSlotIndexMinEnergy = startTimeSlotIndex;
		for (int i = 0; i < mH_hc.getClusterNum(); i++) {
			HomoCluster hc = mH_hc.getHomoCluster(i);
			double energy = job.getJobDynEnergy(job.getTaskNum(), hc) / job.getTaskNum();
			double execTime = job.getTaskExecTime(job.getTaskNum(), hc);
			for (int j = startTimeSlotIndex; j < mT_trt.mVR_timeVector.size(); j++) {
				double startTime = mT_trt.mVR_timeVector.get(j).getStartTime();
				if (startTime < jobEST) {
					startTime = jobEST;
				}
				double endTime = startTime + execTime;
				if (endTime > taskLFT) {
					break;
				}
				int machID = mT_trt.getAvailMachID(startTime, endTime, 1, 
						job.getTaskMemSize(), hc, j);
				if (machID >= 0) {
					if (minEnergy == Constant.uncertainDouble || minEnergy > energy 
							|| minEnergy == energy && endTimeMinEnergy > endTime) {
						minEnergy = energy;
						startTimeMinEnergy = startTime;
						endTimeMinEnergy = endTime;
						machMinEnergy = machID;
						startTimeSlotIndexMinEnergy = j;
					}
					break;
				}
			}
		}
		if (minEnergy != Constant.uncertainDouble) {
			mapScheme.setFstDbl(startTimeMinEnergy);
			mapScheme.setSndDbl(endTimeMinEnergy);
			mapScheme.setFstInt(machMinEnergy);
			mapScheme.setSndInt(startTimeSlotIndexMinEnergy);
			return true;
		}
		return false;
	}
	
	private void scheduleTaskMT(Workflow wf, int jobID, int taskID, double currTime, int currTSI, DblDblIntIntQuad mapScheme) {
		MoldableJob job = wf.getJob(jobID);
		double taskLFT = wf.estiJobLFT(jobID, mH_hc);    // This is critical for the adaption of this algorithm
		
		double jobEST = job.getEST();
		if (jobEST < currTime) {
			jobEST = currTime;
		}
		int startTimeSlotIndex = mT_trt.getTimeSlotIndex(jobEST, currTSI);
		double minEndTime = Constant.uncertainDouble;
		double startTimeMinTime = Constant.uncertainDouble;
		int machMinTime = Constant.uncertainInt;
		int startTimeSlotIndexMinTime = startTimeSlotIndex;
		for (int i = 0; i < mH_hc.getClusterNum(); i++) {
			HomoCluster hc = mH_hc.getHomoCluster(i);
			double execTime = job.getTaskExecTime(job.getTaskNum(), hc);
			for (int j = startTimeSlotIndex; j < mT_trt.mVR_timeVector.size(); j++) {
				double startTime = mT_trt.mVR_timeVector.get(j).getStartTime();
				if (startTime < jobEST) {
					startTime = jobEST;
				}
				double endTime = startTime + execTime;
				if (endTime <= taskLFT) {
					continue;
				}
				int machID = mT_trt.getAvailMachID(startTime, endTime, 1, 
						job.getTaskMemSize(), hc, j);
				if (machID >= 0) {
					if (minEndTime == Constant.uncertainDouble || minEndTime > endTime) {
						minEndTime = endTime;
						startTimeMinTime = startTime;
						machMinTime = machID;
						startTimeSlotIndexMinTime = j;
					}
					break;
				}
			}
		}
		
		mapScheme.setFstDbl(startTimeMinTime);
		mapScheme.setSndDbl(minEndTime);
		mapScheme.setFstInt(machMinTime);
		mapScheme.setSndInt(startTimeSlotIndexMinTime);
	}
	
	private void taskSched(MoldableJob job, int taskID, DblDblIntIntQuad mapScheme) {
		double startTime = mapScheme.getFstDbl();
		double endTime = mapScheme.getSndDbl();
		int machID = mapScheme.getFstInt();
		int startTimeSlotIdx = mapScheme.getSndInt();
		mT_trt.reserve(startTime, endTime, machID, 1, 
				job.getTaskMemSize(), startTimeSlotIdx);
		Task task = job.getTask(taskID);
		task.mapping(startTime, endTime, machID);
	}
}
