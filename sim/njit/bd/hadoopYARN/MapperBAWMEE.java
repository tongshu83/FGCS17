package njit.bd.hadoopYARN;

import java.util.Vector;

import njit.bd.algorithm.RestrictedShortestPath;
import njit.bd.application.*;
import njit.bd.common.Constant;
import njit.bd.common.DblDblIntIntQuad;
import njit.bd.common.IntPair;
import njit.bd.dataCenter.*;

public class MapperBAWMEE extends MapperTaskPartition {
	
	public MapperBAWMEE(HeteroCluster hc, double epsilon) {
		mH_hc = hc;
		mT_trt = new TimeResourceTable(hc);
		md_epsilon = epsilon;
	}
	
	public void mapping(WorkflowGenerator wfg) {
		int currTimeSlotIndex = 0;
		for (int wfID = 0; wfID < wfg.getWfNum(); wfID++) {
			Workflow wf = wfg.getWorkflow(wfID);
			getJobClusterTable(wf, true);
			double currTime = wf.getReadyTime();
			currTimeSlotIndex = mT_trt.getReverseTimeSlotIndex(currTime, currTimeSlotIndex);
			wfMapping(wf, currTimeSlotIndex);
		}
	}
	
	// heterogeneous mapping
	private void wfMapping(Workflow wf, int currTSI) {
		boolean feasibleMap = true;
		wf.updateJobTempEST();
		wf.updateJobTempLFT();
		double est = calcJobEST(wf.getJob(0), wf.getDeadline(), currTSI);
		if (est == Constant.uncertainDouble) {
			feasibleMap = false;
		} else {
			Pipeline cp = wf.getCriticalPath(mH_hc, Workflow.minExecTime);
			double minTime = 0.0;
			for (int i = 0; i < cp.getJobNum(); i++) {
				minTime += mVVD_jobClusterTbl.get(cp.getJobID(i)).get(0).getFstDbl();
			}		
			if (minTime > cp.getLFT() - est) {
				feasibleMap = false;
			}
		}
		if (!feasibleMap) {
			System.out.println(wf.getWfID() + ": infeasible mapping!");
			for (int i = 0; i < wf.getJobNum(); i++) {
				MoldableJob job = wf.getJob(i);
				jobMappingMT(job, job.getTempEST(), currTSI);
				wf.updateJobTempEST();
				wf.updateJobTempLFT();
			}
			wf.updateJobEST();
			wf.updateJobLFT();
			return;
		}
		
		while (wf.existUnmappedJob()) {    // if there exists an unmapped job in the workflow wf
			Pipeline pl = wf.getCriticalPath(mH_hc, Workflow.avgExecTime);

			int startTSI = mT_trt.getReverseTimeSlotIndex(pl.getEST(), currTSI);
			if (!pipelineMappingEE(wf, pl, startTSI)) {
				int jobIdx = pipelineMappingMT(wf, pl, startTSI);
				if (jobIdx >= 0) {
					withdrawDescMapping(wf, pl.getJobID(jobIdx), startTSI);
					int jobID = pl.getJobID(jobIdx);
					if (jobID == wf.getJobNum() - 1) {
						MoldableJob job = wf.getJob(jobID);
						jobMappingMT(job, job.getTempEST(), startTSI);
					}
				}
			}
		}
		wf.updateJobEST();
		wf.updateJobLFT();
	}
	
	private boolean pipelineMappingEE(Workflow wf, Pipeline pl, int startTSI) {
		// Calculate the earliest start time of the pipeline pl
		wf.updateJobTempEST();
		wf.updateJobTempLFT();
		if (pl.getEST() > pl.getLFT()) {
			return false;
		}
		
		double est = calcJobEST(wf.getJob(pl.getJobID(0)), pl.getLFT(), startTSI);
		if (est == Constant.uncertainDouble) {
			return false;
		}
		pl.setEST(est);
		
		double minTime = 0.0;
		for (int i = 0; i < pl.getJobNum(); i++) {
			minTime += mVVD_jobClusterTbl.get(pl.getJobID(i)).get(0).getFstDbl();
		}		
		if (minTime > pl.getLFT() - pl.getEST()) {
			return false;
		}
		
		IntPair[] taskNumClusterIDs = getIdealPipelineMapping(wf, pl, pl.getLFT() - pl.getEST());
		double jobAST = pl.getEST();
		for (int i = 0; i < pl.getJobNum(); i++) {
			MoldableJob job = wf.getJob(pl.getJobID(i));
			if (i > 0 && job.getTempEST() != Constant.uncertainDouble && jobAST < job.getTempEST()) {
				pl = pl.getSubPipeline(0, i - 1);
				if (pl.getLFT() > job.getTempEST()) {
					pl.setLFT(job.getTempEST());
				}
				MoldableJob endJob = wf.getJob(pl.getJobID(i - 1));
				if (pl.getLFT() > endJob.getTempLFT()) {
					pl.setLFT(endJob.getTempLFT());
				}
				return pipelineMappingEE(wf, pl, startTSI);
			}
			double execTime = job.getTaskExecTime(taskNumClusterIDs[i].getFst(), mH_hc.getHomoCluster(taskNumClusterIDs[i].getSnd()));
			double jobAFT = jobAST + execTime;
			if (job.getTempLFT() != Constant.uncertainDouble && jobAFT > job.getTempLFT()) {
				pl = pl.getSubPipeline(0, i);
				pl.setLFT(job.getTempLFT());
				return pipelineMappingEE(wf, pl, startTSI);
			}
			jobAST = jobAFT;
		}
		
		if (pipelineMapping(wf, pl, taskNumClusterIDs, startTSI)) {
			return true;
		}
		withdrawPipelineMapping(wf, pl, startTSI);
		return false;
	}
	
	private double calcJobEST(MoldableJob job, double lft, int startTSI) {
		double est = Constant.uncertainDouble;
		int sTSI = mT_trt.getTimeSlotIndex(job.getTempEST(), startTSI);
		for (int i = 0; i < mH_hc.getClusterNum(); i++) {
			HomoCluster hc = mH_hc.getHomoCluster(i);
			int maxTaskNum = hc.getMaxTaskNum(job);
			double execTime = job.getTaskExecTime(maxTaskNum, hc);
			for (int k = sTSI; k < mT_trt.mVR_timeVector.size(); k++) {
				double startTime = mT_trt.mVR_timeVector.get(k).getStartTime();
				if (startTime < job.getTempEST()) {
					startTime = job.getTempEST();
				}
				if (mT_trt.getAvailMachID(startTime, startTime + execTime, 1, job.getTaskMemSize(), hc, k) >= 0) {
					if (est == Constant.uncertainDouble || est > startTime) {
						est = startTime;
					}
					break;
				}
			}
		}
		return est;
	}
	
	private IntPair[] getIdealPipelineMapping(Workflow wf, Pipeline pl, double execTimeLimit) {
		Vector<Vector<Double>> taskTime = new Vector<Vector<Double>>();
		Vector<Vector<Double>> jobEnergy = new Vector<Vector<Double>>();
		double maxExecTime = 0.0;
		double minExecTime = 0.0;
		for (int i = 0; i < pl.getJobNum(); i++) {
			taskTime.add(new Vector<Double>());
			jobEnergy.add(new Vector<Double>());
			Vector<DblDblIntIntQuad> optList = mVVD_jobClusterTbl.get(pl.getJobID(i));
			maxExecTime += optList.get(optList.size() - 1).getFstDbl();
			minExecTime += optList.get(0).getFstDbl();
			for (int j = 0; j < optList.size(); j++) {
				double taskExecTime = optList.get(j).getFstDbl();
				taskTime.get(i).add(new Double(taskExecTime));
				double jobEC = optList.get(j).getSndDbl();
				jobEnergy.get(i).add(new Double(jobEC));
			}
		}
		
		IntPair[] taskNumClusterIDs = new IntPair[pl.getJobNum()];
		if (execTimeLimit >= maxExecTime) {
			for (int i = 0; i < pl.getJobNum(); i++) {
				Vector<DblDblIntIntQuad> optList = mVVD_jobClusterTbl.get(pl.getJobID(i));
				int taskNum = optList.get(optList.size() - 1).getFstInt();
				int clusterID = optList.get(optList.size() - 1).getSndInt();
				taskNumClusterIDs[i] = new IntPair(taskNum, clusterID);
			}
			return taskNumClusterIDs;
		}
		if (execTimeLimit <= minExecTime + Constant.tiny) {
			for (int i = 0; i < pl.getJobNum(); i++) {
				Vector<DblDblIntIntQuad> optList = mVVD_jobClusterTbl.get(pl.getJobID(i));
				int taskNum = optList.get(0).getFstInt();
				int clusterID = optList.get(0).getSndInt();
				taskNumClusterIDs[i] = new IntPair(taskNum, clusterID);
			}
			return taskNumClusterIDs;
		}
		
		RestrictedShortestPath rsp = new RestrictedShortestPath(pl.getJobNum(), taskTime, jobEnergy);
		rsp.minEnergy(execTimeLimit, md_epsilon);
		int[] optIndeces = rsp.getTaskNums();	// rsp.getTaskNums() i.e. rsp.getOptions() + 1
		for (int i = 0; i < pl.getJobNum(); i++) {
			Vector<DblDblIntIntQuad> optList = mVVD_jobClusterTbl.get(pl.getJobID(i));
			int taskNum = optList.get(optIndeces[i] - 1).getFstInt();
			int clusterID = optList.get(optIndeces[i] - 1).getSndInt();
			taskNumClusterIDs[i] = new IntPair(taskNum, clusterID);
		}
		return taskNumClusterIDs;
	}
	
	
	private boolean pipelineMapping(Workflow wf, Pipeline pl, IntPair[] taskNumClusterIDs, int startTSI) {
		double startTime = pl.getEST();
		double endTime = startTime;
		for (int i = 0; i < pl.getJobNum(); i++) {
			startTime = endTime;
			MoldableJob job = wf.getJob(pl.getJobID(i));
			int taskNum = taskNumClusterIDs[i].getFst();
			job.setTaskNum(taskNum);
			HomoCluster hc = mH_hc.getHomoCluster(taskNumClusterIDs[i].getSnd());
			endTime = startTime + job.getTaskExecTime(taskNum, hc);
			int[] machIDs = new int[taskNum];
			for (int j = 0; j < taskNum; j++) {
				machIDs[j] = mT_trt.getAvailMachID(startTime, endTime, 1, //job.getTaskProcUtil(hc.getClusterID()), 
						job.getTaskMemSize(), hc, startTSI);
				if (machIDs[j] >= 0) {
					mT_trt.reserve(startTime, endTime, machIDs[j], 1, //job.getTaskProcUtil(hc.getClusterID()), 
							job.getTaskMemSize(), startTSI);
					job.getTask(j).mapping(startTime, endTime, machIDs[j]);
				} else {
					for (int k = 0; k < j; k++) {
						mT_trt.reserve(startTime, endTime, machIDs[k], -1, //-job.getTaskProcUtil(hc.getClusterID()), 
								-job.getTaskMemSize(), startTSI);
					}
					job.setTaskNum(0);
					return false;
				}
			}
			job.setASFT(startTime, endTime);
		}
		return true;
	}
	
	private int pipelineMappingMT(Workflow wf, Pipeline pl, int startTSI) {
		// calculate the maximum number of map tasks in the entire pipeline
		int jobIdx = pl.getJobNum() - 1;
		wf.updateJobTempLFT();
		double jobEST = pl.getEST();
		double jobEFT = jobEST;
		for (int i = 0; i < pl.getJobNum(); i++) {
			wf.updateJobTempEST();
			MoldableJob job = wf.getJob(pl.getJobID(i));
			jobEST = jobEFT;
			if (jobEST < job.getTempEST()) {
				jobEST = job.getTempEST();
			}
			int sTSI = mT_trt.getReverseTimeSlotIndex(jobEST, startTSI);
			jobEFT = jobMappingMT(job, jobEST, sTSI);
			if (job.getTempLFT() != Constant.uncertainInt && jobEFT > job.getTempLFT()) {
				withdrawJobMapping(job, sTSI);
				jobIdx = i - 1;
				break;
			}
		}
		
		if (jobIdx == pl.getJobNum() - 1) {
			return Constant.uncertainInt;
		}
		return jobIdx + 1;
	}
	
	private double jobMappingMT(MoldableJob job, double jobEST, int startTSI) {
	    startTSI = mT_trt.getReverseTimeSlotIndex(jobEST, startTSI);
	    
		double minEndTime = Constant.uncertainDouble;
		double jobAST = Constant.uncertainDouble;
		int taskNum = Constant.uncertainInt;
		int maxTaskNum = mH_hc.getMaxTaskNum(job);
		for (int i = 1; i < maxTaskNum * 2; i *= 2) {
			if (i > maxTaskNum) {
				i = maxTaskNum;
			}
			double jobStartTime = Constant.uncertainDouble;
			double jobEndTime = Constant.uncertainDouble;
			job.setTaskNum(i);
			for (int j = 0; j < i; j++) {
				taskMappingMT(job, j, jobEST, startTSI);
				if (jobStartTime == Constant.uncertainDouble || jobStartTime > job.getTask(j).getStartTime()) {
					jobStartTime = job.getTask(j).getStartTime();
				}
				if (jobEndTime == Constant.uncertainDouble || jobEndTime < job.getTask(j).getEndTime()) {
					jobEndTime = job.getTask(j).getEndTime();
				}
			}
			job.setASFT(jobStartTime, jobEndTime);
			if (minEndTime == Constant.uncertainDouble || minEndTime > jobEndTime) {
				minEndTime = jobEndTime;
				jobAST = jobStartTime;
				taskNum = i;
			}
			withdrawJobMapping(job, startTSI);
		}
		
		job.setTaskNum(taskNum);
		for (int i = 0; i < taskNum; i++) {
			taskMappingMT(job, i, jobEST, startTSI);
		}
		job.setASFT(jobAST, minEndTime);
		return job.getAFT();
	}
	
	private double taskMappingMT(MoldableJob job, int taskID, double jobEST, int startTSI) {
		double minEndTime = Constant.uncertainDouble;
		double startTimeMET = Constant.uncertainDouble;
		int machIDMET = Constant.uncertainInt;
		int startTSIMET = startTSI;
		for (int i = 0; i < mH_hc.getClusterNum(); i++) {
			HomoCluster hc = mH_hc.getHomoCluster(i);
			double execTime = job.getTaskExecTime(job.getTaskNum(), hc);
			for (int j = startTSI; j < mT_trt.mVR_timeVector.size(); j++) {
				double startTime = mT_trt.mVR_timeVector.get(j).getStartTime();
				if (startTime < jobEST) {
					startTime = jobEST;
				}
				if (minEndTime != Constant.uncertainDouble && startTime > minEndTime) {
					break;
				}
				double endTime = startTime + execTime;
				int machID = mT_trt.getAvailMachID(startTime, endTime, 1, 
						job.getTaskMemSize(), hc, j);
				if (machID >= 0) {
					if (minEndTime == Constant.uncertainDouble || minEndTime > endTime) {
						minEndTime = endTime;
						startTimeMET = startTime;
						machIDMET = machID;
						startTSIMET = j;
					}
					break;
				}
			}
		}
		mT_trt.reserve(startTimeMET, minEndTime, machIDMET, 1, 
				job.getTaskMemSize(), startTSIMET);
		job.getTask(taskID).mapping(startTimeMET, minEndTime, machIDMET);
		return minEndTime;
	}

	// Withdraw the mappings of all the descendants of job jobID in the workflow
	private boolean withdrawDescMapping(Workflow wf, int jobID, int startTSI) {
		Vector<Integer> desc = new Vector<Integer>();
		for (int i = jobID + 1; i < wf.getJobNum(); i++) {
			if (wf.existPrecedence(jobID, i) && wf.getJob(i).isMapped()) {
				desc.add(new Integer(i));
			}
		}
		if (desc.size() <= 0) {
			return false;
		}
		for (int i = 0; i < desc.size(); i++) {
			for (int j = desc.get(i).intValue() + 1; j < wf.getJobNum(); j++) {
				if (wf.existPrecedence(desc.get(i).intValue(), j) && wf.getJob(j).isMapped()) {
					if (!desc.contains(new Integer(j))) {
						desc.add(new Integer(j));
					}
				}
			}
		}
		for (int i = 0; i < desc.size(); i++) {
			withdrawJobMapping(wf.getJob(desc.get(i).intValue()), startTSI);
		}
		return true;
	}
	
	private void withdrawPipelineMapping(Workflow wf, Pipeline pl, int startTSI) {
		if (pl != null) {
			for (int i = 0; i < pl.getJobNum(); i++) {
				withdrawJobMapping(wf.getJob(pl.getJobID(i)), startTSI);
			}
		}
	}
	
	private void withdrawJobMapping(MoldableJob job, int startTSI) {
		if (job.isMapped()) {
			for (int i = 0; i < job.getTaskNum(); i++) {
				Task task = job.getTask(i);
				if (task.isMapped()) {
					mT_trt.reserve(task.getStartTime(), task.getEndTime(), task.getMachID(), 
							-1, -job.getTaskMemSize(), startTSI);
				}
			}
		}
		job.setASFT(Constant.uncertainDouble, Constant.uncertainDouble);
		job.setTaskNum(0);
	}
}
