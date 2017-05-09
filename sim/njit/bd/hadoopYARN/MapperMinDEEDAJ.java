package njit.bd.hadoopYARN;

import java.util.Vector;

import njit.bd.application.JobIndexPair;
import njit.bd.application.JobPriorityQueue;
import njit.bd.application.MoldableJob;
import njit.bd.application.Task;
import njit.bd.application.Workflow;
import njit.bd.application.WorkflowGenerator;
import njit.bd.common.Constant;
import njit.bd.common.DblDblIntIntQuad;
import njit.bd.dataCenter.HeteroCluster;
import njit.bd.dataCenter.HomoCluster;

public class MapperMinDEEDAJ extends MapperTaskPartition {
	double md_period = 600.0;    // in seconds

	public MapperMinDEEDAJ(HeteroCluster hc, double period) {
		mH_hc = hc;
		mT_trt = new TimeResourceTable(hc);
		md_period = period;
	}
	
	public void schedule(WorkflowGenerator wfg, boolean taskPartition) {
		for (int i = 0; i < wfg.getWfNum(); i++) {
			Workflow wf = wfg.getWorkflow(i);
			compVirtDl(wf, taskPartition);
		}
		heteroMapping(wfg);
	}
	
	private void compVirtDl(Workflow wf, boolean taskPartition) {
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

	private void heteroMapping(WorkflowGenerator wfg) {
		for (int i = 0; i < wfg.getWfNum(); i++) {
			Workflow wf = wfg.getWorkflow(i);
			for (int j = 0; j < wf.getJobNum(); j++) {
				MoldableJob job = wf.getJob(j);
				job.setTaskNum(job.getMaxTaskNum());
				job.initProgress(md_period, mH_hc);
			}
		}
		JobPriorityQueue jobPriorityQueue = new JobPriorityQueue(wfg);
		for (int i = 0; i < wfg.getWfNum(); i++) {
			Workflow wf = wfg.getWorkflow(i);
			for (int j = 0; j < wf.getJobNum(); j++) {
				jobPriorityQueue.enqueue(new JobIndexPair(i, j));
			}
		}
		double currTime = 0.0;
		int currTSI = 0;
		while (jobPriorityQueue.size() > 0) {
			JobIndexPair jobIdxPair = jobPriorityQueue.dequeue(JobPriorityQueue.TaskPriority);
			Workflow wf = wfg.getWorkflow(jobIdxPair.getWfID());
			int jobID = jobIdxPair.getJobID();
			MoldableJob job = wf.getJob(jobID);
			int taskID = job.getCurrTaskID();
			currTime = job.getSchedTime();
			currTSI = mT_trt.getReverseTimeSlotIndex(currTime, currTSI);
			DblDblIntIntQuad mapScheme = new DblDblIntIntQuad(0.0, 0.0, 0, 0);
			if (!scheduleTaskEE(job, taskID, currTime, currTSI, mapScheme)) {
				scheduleTaskMT(job, taskID, currTime, currTSI, mapScheme);
			}
			taskSched(job, taskID, mapScheme);
			if (job.nextTask(md_period, currTime, mH_hc)) {
				jobPriorityQueue.enqueue(jobIdxPair);
			} else {
				job.updateASFT();
				wf.updateJobEST();
				int jobNum = wf.getJobNum();
				for (int i = jobID + 1; i < jobNum; i++) {
					if (wf.existPrecedence(jobID, i)) {
						MoldableJob succJob = wf.getJob(i);
						double succJobEST = succJob.getEST();
						if (succJobEST >= 0.0) {
							succJob.setSchedTime(Math.ceil(succJobEST / md_period) * md_period);
						}
					}
				}
				if (jobID >= jobNum - 1)
					wf.updateJobLFT();
			}
		}
	}
	
	private boolean scheduleTaskEE(MoldableJob job, int taskID, double currTime, int currTSI, DblDblIntIntQuad mapScheme) {
		double taskLFT = job.estiTaskLFT(taskID, job.getVirtDeadline());
		
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
	
	private void scheduleTaskMT(MoldableJob job, int taskID, double currTime, int currTSI, DblDblIntIntQuad mapScheme) {
		double taskLFT = job.estiTaskLFT(taskID, job.getVirtDeadline());
		
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
