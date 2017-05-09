package njit.bd.hadoopYARN;

import java.util.Vector;

import njit.bd.application.JobIndexPair;
import njit.bd.application.JobPriorityQueue;
import njit.bd.application.MoldableJob;
import njit.bd.application.Task;
import njit.bd.application.Workflow;
import njit.bd.application.WorkflowGenerator;
import njit.bd.common.Constant;
import njit.bd.dataCenter.HeteroCluster;
import njit.bd.dataCenter.HomoCluster;

public class MapperMinDED extends MapperTaskPartition {

	public MapperMinDED(HeteroCluster hc) {
		mH_hc = hc;
		mT_trt = new TimeResourceTable(hc);
	}
	
	public void schedule(WorkflowGenerator wfg, boolean taskPartition) {
		for (int i = 0; i < wfg.getWfNum(); i++) {
			Workflow wf = wfg.getWorkflow(i);
			compVirtDl(wf, taskPartition);
		}
		EDScheduling(wfg);
	}
	
	private void compVirtDl(Workflow wf, boolean taskPartition) {
		if (!taskPartition) {
			for (int i = 0; i < wf.getJobNum(); i++) {
				MoldableJob job = wf.getJob(i);
				job.setTaskNum(job.getMaxTaskNum());
			}
		}
		getJobClusterTable(wf, taskPartition);
		int jobNum = wf.getJobNum();
		int[] jobOptions = new int[jobNum];
		for (int i = 0; i < jobNum; i++) {
			jobOptions[i] = 0;
		}
		
		double[] priorities = compPriority(wf, taskPartition);
		int[] priorityRanks = getJobRanks(wf, priorities);
		
		double[] virtDeadlines = new double[jobNum];
		int jobIdx = 0;
		int count = 0;
		while (count < jobNum) {
			double[] tempVirtDls = new double[jobNum];
			jobOptions[priorityRanks[jobIdx]]++;
			if (isInTime(wf, jobOptions, tempVirtDls)) {
				for (int i = 0; i < jobNum; i++) {
					virtDeadlines[i] = tempVirtDls[i];
				}
				count = 0;
			} else {
				jobOptions[priorityRanks[jobIdx]]--;
				count++;
			}
			jobIdx = (jobIdx + 1) % jobNum;
		}
		
		double slackTime = virtDeadlines[0] - mVVD_jobClusterTbl.get(0).get(jobOptions[0]).getFstDbl();
		int[] virtDeadlineRanks = getJobRanks(wf, virtDeadlines);
		for (int i = 0; i < jobNum; i++) {
			int jobID = virtDeadlineRanks[i];
			MoldableJob job = wf.getJob(jobID);
			job.setVirtDeadline(virtDeadlines[jobID] - slackTime * i / jobNum);
			
			int taskNum = mVVD_jobClusterTbl.get(jobID).get(jobOptions[jobID]).getFstInt();
			job.setTaskNum(taskNum);
		}
	}
	
	private double[] compPriority(Workflow wf, boolean taskPartition) {
		int jobNum = wf.getJobNum();
		double[] priorities = new double[jobNum];
		for (int jobID = 0; jobID < jobNum; jobID++) {
			Vector<Integer> desc = new Vector<Integer>();
			for (int i = jobID + 1; i < jobNum; i++) {
				if (wf.existPrecedence(jobID, i)) {
					desc.add(new Integer(i));
				}
			}
			for (int i = 0; i < desc.size(); i++) {
				for (int j = desc.get(i).intValue() + 1; j < jobNum; j++) {
					if (wf.existPrecedence(desc.get(i).intValue(), j) && !desc.contains(new Integer(j))) {
						desc.add(new Integer(j));
					}
				}
			}
			double descET = 0.0;
			for (int i = 0; i < desc.size(); i++) {
				MoldableJob job = wf.getJob(desc.get(i).intValue());
				if (taskPartition)
					descET += job.getTaskExecTime(1, job.getMaxTaskNum(), mH_hc);
				else
					descET += job.getTaskExecTime(job.getMaxTaskNum(), mH_hc);
			}
			
			Vector<Integer> ance = new Vector<Integer>();
			for (int i = 0; i < jobID; i++) {
				if (wf.existPrecedence(i, jobID)) {
					ance.add(new Integer(i));
				}
			}
			for (int i = 0; i < ance.size(); i++) {
				for (int j = 0; j < ance.get(i).intValue(); j++) {
					if (wf.existPrecedence(j, ance.get(i).intValue()) && !ance.contains(new Integer(j))) {
						ance.add(new Integer(j));
					}
				}
			}
			double anceET = 0.0;
			for (int i = 0; i < ance.size(); i++) {
				MoldableJob job = wf.getJob(ance.get(i).intValue());
				if (taskPartition)
					anceET += job.getTaskExecTime(1, job.getMaxTaskNum(), mH_hc);
				else
					anceET += job.getTaskExecTime(job.getMaxTaskNum(), mH_hc);
			}
			
			priorities[jobID] = - anceET - descET;
		}
		return priorities;
	}
	
	private int[] getJobRanks(Workflow wf, double[] rankProporties) {	// from large to small
		int jobNum = wf.getJobNum();
		int[] ranks = new int[jobNum];
		for (int i = 0; i < jobNum; i++) {
			ranks[i] = i;
		}
		for (int i = 0; i < jobNum; i++) {
			for (int j = i + 1; j < jobNum; j++) {
				if (rankProporties[ranks[i]] < rankProporties[ranks[j]]) {
					int temp = ranks[i];
					ranks[i] = ranks[j];
					ranks[j] = temp;
				}
			}
		}
		return ranks;
	}
	
	private boolean isInTime(Workflow wf, int[] jobOptions, double[] virtDeadlines) {
		int jobNum = wf.getJobNum();
		double[] execTimes = new double[jobNum];
		for (int i = 0; i < jobNum; i++) {
			if (jobOptions[i] >= mVVD_jobClusterTbl.get(i).size())
				return false;
			execTimes[i] = mVVD_jobClusterTbl.get(i).get(jobOptions[i]).getFstDbl();
		}
		
		// Dynamic programming
		for (int i = jobNum - 1; i >= 0; i--) {
			virtDeadlines[i] = wf.getDeadline();
			for (int j = i + 1; j < jobNum; j++) {
				if (wf.existPrecedence(i, j) && virtDeadlines[i] > virtDeadlines[j] - execTimes[j]) {
					virtDeadlines[i] = virtDeadlines[j] - execTimes[j];
				}
			}
		}
		if (virtDeadlines[0] < execTimes[0])
			return false;
		return true;
	}

	private void EDScheduling(WorkflowGenerator wfg) {
		JobPriorityQueue jobPriorityQueue = new JobPriorityQueue(wfg);
		for (int i = 0; i < wfg.getWfNum(); i++) {
			Workflow wf = wfg.getWorkflow(i);
			for (int j = 0; j < wf.getJobNum(); j++) {
				jobPriorityQueue.enqueue(new JobIndexPair(i, j));
			}
		}
		int startTimeSlotIndex = 0;
		while (jobPriorityQueue.size() > 0) {
			JobIndexPair jobIndexPair = jobPriorityQueue.dequeue(JobPriorityQueue.EST);
			MoldableJob job = wfg.getWorkflow(jobIndexPair.getWfID()).getJob(jobIndexPair.getJobID());
			
			startTimeSlotIndex = mT_trt.getReverseTimeSlotIndex(job.getEST(), startTimeSlotIndex);
			for (int i = 0; i < job.getTaskNum(); i++) {
				if (!scheduleTaskEE(job, i, startTimeSlotIndex)) {
					scheduleTaskMT(job, i, startTimeSlotIndex);
				}
			}
			job.updateASFT();
			wfg.getWorkflow(jobIndexPair.getWfID()).updateJobEST();
			wfg.getWorkflow(jobIndexPair.getWfID()).updateJobLFT();
		}
	}
	
	private boolean scheduleTaskEE(MoldableJob job, int taskID, int stsi) {
		double taskLFT = job.getVirtDeadline();
		double jobEST = job.getEST();
		int startTimeSlotIndex = mT_trt.getTimeSlotIndex(jobEST, stsi);
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
			mT_trt.reserve(startTimeMinEnergy, endTimeMinEnergy, machMinEnergy, 1, 
					job.getTaskMemSize(), startTimeSlotIndexMinEnergy);
			Task task = job.getTask(taskID);
			task.mapping(startTimeMinEnergy, endTimeMinEnergy, machMinEnergy);
			return true;
		}
		return false;
	}
	
	private double scheduleTaskMT(MoldableJob job, int taskID, int stsi) {
		double taskLFT = job.getVirtDeadline();
		double jobEST = job.getEST();
		int startTimeSlotIndex = mT_trt.getTimeSlotIndex(jobEST, stsi);
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
		mT_trt.reserve(startTimeMinTime, minEndTime, machMinTime, 1, 
				job.getTaskMemSize(), startTimeSlotIndexMinTime);
		Task task = job.getTask(taskID);
		task.mapping(startTimeMinTime, minEndTime, machMinTime);
		return minEndTime;
	}
}
