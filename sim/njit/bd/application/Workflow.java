package njit.bd.application;

import java.util.Random;
import java.util.Vector;

import njit.bd.common.Constant;
import njit.bd.dataCenter.HeteroCluster;
import njit.bd.dataCenter.HomoCluster;

public class Workflow {
	final public static int randomShape = 0;
	final public static int chain = 1;
	final public static int tree = 2;
	final public static int reverseTree = 3;
	final public static int diamond = 4;
	final public static int special = 5;
	
	final public static int avgExecTime = 0;
	final public static int minExecTime = 1;
	
	int mi_wfID = 0;
	int mi_jobNum = 0;
	MoldableJob[] maM_jobs;
	double[][] maad_precedences;
	double md_readyTime = 0.0;
	double md_deadline = 0.0;
	double md_deadlineBaseline = 0.0;
	
	TaskIndexTriple md_currTask = null;
	double md_schedTime = Constant.uncertainDouble;
	double md_taskRank = 0.0;
	
	public Workflow() {}
	public Workflow(int wfID, int jobNum, int wfShape, double readyTime, HeteroCluster hc) {
		if (wfShape == special) {
			jobNum = 10;
		}
		
		// Set workflow ID
		mi_wfID = wfID;
		
		// Set job number
		mi_jobNum = jobNum;
		
		// Generate jobs
		maM_jobs = new MoldableJob[mi_jobNum];
		for (int i = 0; i < mi_jobNum; i++) {
			maM_jobs[i] = new MoldableJob(wfID, i);
		}
		
		switch (wfShape) {
		case randomShape:
			generatePrecedence();
			break;
		case chain:
			generateChainPrecedence();
			break;
		case tree:
			generateTreePrecedence();
			break;
		case reverseTree:
			generateReverseTreePrecedence();
			break;
		case diamond:
			generateDiamondPrecedence();
			break;
		case special:
			generatesSpecialPrecedence();
			break;
		default:
			generatePrecedence();
			break;
		}
		
		md_readyTime = readyTime;
		generateDeadline(hc);
		
		/*
		maM_jobs[0].md_taskProcUtils[0] = 0.7;
		maM_jobs[1].md_taskProcUtils[0] = 0.9;
		maM_jobs[2].md_taskProcUtils[0] = 0.4;
		maM_jobs[3].md_taskProcUtils[0] = 0.6;
		maM_jobs[4].md_taskProcUtils[0] = 0.1;
		maM_jobs[5].md_taskProcUtils[0] = 0.8;
		maM_jobs[6].md_taskProcUtils[0] = 0.4;
		maM_jobs[7].md_taskProcUtils[0] = 1;
		maM_jobs[8].md_taskProcUtils[0] = 0.2;
		maM_jobs[9].md_taskProcUtils[0] = 0.4;
		md_deadline = 88561.0;
		*/
		
		maM_jobs[0].setEST(md_readyTime);
		maM_jobs[mi_jobNum - 1].setLFT(md_deadline);
		
		for (int i = 0; i < mi_jobNum; i++) {
			maM_jobs[i].setBestLFT(compJobLFT(i, hc));
		}
	}
	
	private void generatesSpecialPrecedence() {
		maad_precedences = new double[mi_jobNum][mi_jobNum];
		for (int i = 0; i < mi_jobNum; i++) {
			for (int j = 0; j < mi_jobNum; j++) {
				maad_precedences[i][j] = 0.0;
			}
		}
		maad_precedences[0][1] = 1.0;
		maad_precedences[0][2] = 1.0;
		maad_precedences[0][3] = 1.0;
		maad_precedences[0][4] = 1.0;
		maad_precedences[1][5] = 1.0;
		maad_precedences[2][5] = 1.0;
		maad_precedences[5][6] = 1.0;
		maad_precedences[1][7] = 1.0;
		maad_precedences[5][7] = 1.0;
		maad_precedences[5][8] = 1.0;
		maad_precedences[3][9] = 1.0;
		maad_precedences[4][9] = 1.0;
		maad_precedences[6][9] = 1.0;
		maad_precedences[7][9] = 1.0;
		maad_precedences[8][9] = 1.0;
	}
	
	// Generate precedence constraints among jobs
	private void generatePrecedence() {
		// Initializing
		maad_precedences = new double[mi_jobNum][mi_jobNum];
		for (int i = 0; i < mi_jobNum; i++) {
			for (int j = 0; j < mi_jobNum; j++) {
				maad_precedences[i][j] = 0.0;
			}
		}
		
		// Generate layers
		int minLayerNum = (mi_jobNum - 3) / 5 + 3;
		Random rand = new Random();
		int layerNum = minLayerNum + rand.nextInt(mi_jobNum - minLayerNum + 1);
		int[] layers = new int[layerNum];
		int[] indices = new int[layerNum];
		for (int i = 1; i < layerNum - 1; i++) {
			layers[i] = 1;
			indices[i] = 0;
		}
		layers[0] = 1;
		indices[0] = 0;
		layers[layerNum - 1] = 1;
		indices[layerNum - 1] = mi_jobNum - 1;
		for (int i = 0; i < mi_jobNum - layerNum; i++) {
			layers[1 + rand.nextInt(layerNum - 2)]++;
		}
		for (int i = 1; i < layerNum - 1; i++) {
			indices[i] = indices[i - 1] + layers[i - 1];
		}
		
		int count = 0;
		for (int i = 1; i < layerNum - 1; i++) {
			for (int j = 0; j < layers[i]; j++) {
				// Randomly pick a job in the previous adjacent layer to generate a precedence constraint for each job, except the end job
				int currJobID = indices[i] + j;
				int prevJobID, k;
				for (k = 0; k < layers[i - 1]; k++) {
					prevJobID = indices[i - 1] + k;
					if (maad_precedences[prevJobID][currJobID] == 1.0) {
						break;
					}
				}
				if (k >= layers[i - 1]) {
					prevJobID = indices[i - 1] + rand.nextInt(layers[i - 1]);
					maad_precedences[prevJobID][currJobID] = 1.0;
					count++;
				}
				// Randomly pick a job in a subsequent layer to generate a precedence constraint for each job, except the start and end jobs
				int succJobID = indices[i + 1] + rand.nextInt(mi_jobNum - indices[i + 1]);
				maad_precedences[currJobID][succJobID] = 1.0;
				count++;
			}
		}
		
		// Generate other precedence constraints across layers
		int precNum = mi_jobNum * 3 / 2 + mi_jobNum % 2;
		int loopNum = 0;
		while (count < precNum) {
			int prevJob = rand.nextInt(mi_jobNum - 1);
			int prevLayer = layerNum - 1;
			for (int i = 1; i < layerNum; i++) {
				if (prevJob < indices[i]) {
					prevLayer = i - 1;
					break;
				}
			}
			int succJob = indices[prevLayer + 1] + rand.nextInt(mi_jobNum - indices[prevLayer + 1]);
			if (maad_precedences[prevJob][succJob] == 0.0) {
				maad_precedences[prevJob][succJob] = 1.0;
				count++;
				loopNum = 0;
			} else {
				loopNum++;
			}
			if (loopNum >= mi_jobNum * mi_jobNum) {
				break;
			}
		}
		
		/*
		System.out.println("baseLayerNum = " + baseLayerNum);
		System.out.println("layerNum = " + layerNum);
		for (int i = 0; i < layerNum; i++) {
			System.out.println("indices[" + i + "] = " + indices[i]);
		}
		System.out.println("precNum = " + precNum);
		*/
	}
	
	private void generateChainPrecedence() {
		maad_precedences = new double[mi_jobNum][mi_jobNum];
		for (int i = 0; i < mi_jobNum; i++) {
			for (int j = 0; j < mi_jobNum; j++) {
				maad_precedences[i][j] = 0.0;
			}
		}
		for (int i = 0; i < mi_jobNum - 1; i++) {
			maad_precedences[i][i + 1] = 1.0;
		}
	}
	
	private void generateTreePrecedence() {
		maad_precedences = new double[mi_jobNum][mi_jobNum];
		for (int i = 0; i < mi_jobNum; i++) {
			for (int j = 0; j < mi_jobNum; j++) {
				maad_precedences[i][j] = 0.0;
			}
		}
		int layerNum = (int) Math.floor(Math.log(mi_jobNum - 1) / Math.log(2)) + 1;
		int leafNum = mi_jobNum - 1 - ((int) Math.pow(2.0, layerNum - 1) - 1);
		if (leafNum < (int) Math.pow(2.0, layerNum - 2)) {
			layerNum--;
		}
		layerNum--;
		int jobNum = (int) Math.pow(2.0, layerNum) - 1;
		for (int i = 1; i < jobNum; i++) {
			int parent = (i - 1) / 2;
			maad_precedences[parent][i] = 1.0;
		}
		leafNum = (int) Math.pow(2.0, layerNum - 1);
		int upperStartJobID = jobNum - leafNum;
		int middleStartJobID = jobNum;
		int middleJobNum = mi_jobNum - 1 - middleStartJobID;
		int middleJobID = middleStartJobID;
		for (int i = 0; i < middleJobNum % leafNum; i++) {
			int childNum = middleJobNum / leafNum + 1;
			for (int j = 0; j < childNum; j++) {
				maad_precedences[upperStartJobID + i][middleJobID] = 1.0;
				maad_precedences[middleJobID][mi_jobNum - 1] = 1.0;
				middleJobID++;
			}
		}
		for (int i = middleJobNum % leafNum; i < leafNum; i++) {
			int childNum = middleJobNum / leafNum;
			for (int j = 0; j < childNum; j++) {
				maad_precedences[upperStartJobID + i][middleJobID] = 1.0;
				maad_precedences[middleJobID][mi_jobNum - 1] = 1.0;
				middleJobID++;
			}
		}
	}
	
	private void generateReverseTreePrecedence() {
		if (mi_jobNum < 4) {
			generateChainPrecedence();
			return;
		}
		maad_precedences = new double[mi_jobNum][mi_jobNum];
		for (int i = 0; i < mi_jobNum; i++) {
			for (int j = 0; j < mi_jobNum; j++) {
				maad_precedences[i][j] = 0.0;
			}
		}
		
		int treeLayerNum = (int) Math.floor(Math.log(mi_jobNum - 1) / Math.log(2)) + 1;
		int treeLeafNum = mi_jobNum - 1 - ((int) Math.pow(2.0, treeLayerNum - 1) - 1);
		if (treeLeafNum < (int) Math.pow(2.0, treeLayerNum - 2)) {
			treeLayerNum--;
		}
		int cmplBiTreeLayerNum = treeLayerNum - 1;
		int cmplBiTreeJobNum = (int) Math.pow(2.0, cmplBiTreeLayerNum) - 1;
		for (int i = 1; i < cmplBiTreeJobNum; i++) {
			int parent = (i - 1) / 2;
			maad_precedences[mi_jobNum - 1 - i][mi_jobNum - 1 - parent] = 1.0;
		}
		
		int cmplBiTreeLeafNum = (int) Math.pow(2.0, cmplBiTreeLayerNum - 1);
		int middleStartJobID = 1;
		int middleJobNum = mi_jobNum - 1 - cmplBiTreeJobNum;
		int lowerupperStartJobID = middleStartJobID + middleJobNum;
		int middleJobID = middleStartJobID;
		for (int i = 0; i < middleJobNum % cmplBiTreeLeafNum; i++) {
			int childNum = middleJobNum / cmplBiTreeLeafNum + 1;
			for (int j = 0; j < childNum; j++) {
				maad_precedences[0][middleJobID] = 1.0;
				maad_precedences[middleJobID][lowerupperStartJobID + i] = 1.0;
				middleJobID++;
			}
		}
		for (int i = middleJobNum % cmplBiTreeLeafNum; i < cmplBiTreeLeafNum; i++) {
			int childNum = middleJobNum / cmplBiTreeLeafNum;
			for (int j = 0; j < childNum; j++) {
				maad_precedences[0][middleJobID] = 1.0;
				maad_precedences[middleJobID][lowerupperStartJobID + i] = 1.0;
				middleJobID++;
			}
		}
	}
	
	private void generateDiamondPrecedence() {
		maad_precedences = new double[mi_jobNum][mi_jobNum];
		for (int i = 0; i < mi_jobNum; i++) {
			for (int j = 0; j < mi_jobNum; j++) {
				maad_precedences[i][j] = 0.0;
			}
		}
		int layerNum = (int) Math.floor(Math.log(mi_jobNum / 2 + mi_jobNum % 2) / Math.log(2)) + 1;
		int leafNum = mi_jobNum / 2 - ((int) Math.pow(2.0, layerNum - 1) - 1);
		if (leafNum < (int) Math.pow(2.0, layerNum - 2)) {
			layerNum--;
		}
		layerNum--;
		int jobNum = (int) Math.pow(2.0, layerNum) - 1;
		for (int i = 1; i < jobNum; i++) {
			int parent = (i - 1) / 2;
			maad_precedences[parent][i] = 1.0;
			maad_precedences[mi_jobNum - i - 1][mi_jobNum - parent - 1] = 1.0;
		}
		leafNum = (int) Math.pow(2.0, layerNum - 1);
		int upperStartJobID = jobNum - leafNum;
		int middleStartJobID = jobNum;
		int lowerStartJobID = mi_jobNum - jobNum;
		int middleJobNum = lowerStartJobID - middleStartJobID;
		int middleJobID = middleStartJobID;
		for (int i = 0; i < middleJobNum % leafNum; i++) {
			int childNum = middleJobNum / leafNum + 1;
			for (int j = 0; j < childNum; j++) {
				maad_precedences[upperStartJobID + i][middleJobID] = 1.0;
				maad_precedences[middleJobID][lowerStartJobID + i] = 1.0;
				middleJobID++;
			}
		}
		for (int i = middleJobNum % leafNum; i < leafNum; i++) {
			int childNum = middleJobNum / leafNum;
			for (int j = 0; j < childNum; j++) {
				maad_precedences[upperStartJobID + i][middleJobID] = 1.0;
				maad_precedences[middleJobID][lowerStartJobID + i] = 1.0;
				middleJobID++;
			}
		}
	}
	/*
	private void generatesSpecialPrecedence() {
		maad_precedences = new double[mi_jobNum][mi_jobNum];
		for (int i = 0; i < mi_jobNum; i++) {
			for (int j = 0; j < mi_jobNum; j++) {
				maad_precedences[i][j] = 0.0;
			}
		}
		maad_precedences[0][1] = 1.0;
		maad_precedences[0][2] = 1.0;
		maad_precedences[0][3] = 1.0;
		maad_precedences[0][4] = 1.0;
		maad_precedences[1][5] = 1.0;
		maad_precedences[2][5] = 1.0;
		maad_precedences[5][6] = 1.0;
		maad_precedences[1][7] = 1.0;
		maad_precedences[5][7] = 1.0;
		maad_precedences[5][8] = 1.0;
		maad_precedences[3][9] = 1.0;
		maad_precedences[4][9] = 1.0;
		maad_precedences[6][9] = 1.0;
		maad_precedences[7][9] = 1.0;
		maad_precedences[8][9] = 1.0;
	}
	*/
	// Generate deadline
	private void generateDeadline(HeteroCluster hc) {
		double makespan = 0.0;
		Pipeline cp = getCriticalPath(mi_jobNum - 1, hc, avgExecTime);
		for (int i = 0; i < cp.getJobNum(); i++) {
			makespan += maM_jobs[cp.getJobID(i)].getTaskExecTime(1, hc);
		}
		md_deadlineBaseline = md_readyTime + makespan;
		md_deadline = md_deadlineBaseline;
	}
	
	public void updateJobTempEST() {
		maM_jobs[0].setTempEST(md_readyTime);
		for (int i = 1; i < mi_jobNum; i++) {
			double est = Constant.uncertainDouble;
			for (int j = 0; j < i; j++) {
				if (existPrecedence(j, i) && maM_jobs[j].isMapped()
						&& (est == Constant.uncertainDouble || est < maM_jobs[j].getAFT())) {
					est = maM_jobs[j].getAFT();
				}
			}
			maM_jobs[i].setTempEST(est);
		}
	}
	
	public void updateJobTempLFT() {
		maM_jobs[mi_jobNum - 1].setTempLFT(md_deadline);
		for (int i = 0; i < mi_jobNum - 1; i++) {
			double lft = Constant.uncertainDouble;
			for (int j = i + 1; j < mi_jobNum; j++) {
				if (existPrecedence(i, j) && maM_jobs[j].isMapped()
						&& (lft == Constant.uncertainDouble || lft > maM_jobs[j].getAST())) {
					lft = maM_jobs[j].getAST();
				}
			}
			maM_jobs[i].setTempLFT(lft);
		}
	}
	
	public void updateJobEST() {
		maM_jobs[0].setEST(md_readyTime);
		for (int i = 1; i < mi_jobNum; i++) {
			double est = Constant.uncertainDouble;
			for (int j = 0; j < i; j++) {
				if (existPrecedence(j, i)) {
					if (!maM_jobs[j].isMapped()) {
						est = Constant.uncertainDouble;
						break;
					} else if (est == Constant.uncertainDouble || est < maM_jobs[j].getAFT()) {
						est = maM_jobs[j].getAFT();
					}
				}
			}
			maM_jobs[i].setEST(est);
		}
	}
	
	public void updateJobLFT() {
		maM_jobs[mi_jobNum - 1].setLFT(md_deadline);
		for (int i = 0; i < mi_jobNum - 1; i++) {
			double lft = Constant.uncertainDouble;
			for (int j = i + 1; j < mi_jobNum; j++) {
				if (existPrecedence(i, j)) {
					if (!maM_jobs[j].isMapped()) {
						lft = Constant.uncertainDouble;
						break;
					} else if (lft == Constant.uncertainDouble || lft > maM_jobs[j].getAST()) {
						lft = maM_jobs[j].getAST();
					}
				}
			}
			maM_jobs[i].setLFT(lft);
		}
	}
	
	/* begin: used by MapperBAWMEE */
	public double compJobLFT(int jobID, HeteroCluster hc) {
		Vector<Integer> desc = new Vector<Integer>();
		for (int i = jobID + 1; i < mi_jobNum; i++) {
			if (existPrecedence(jobID, i)) {
				desc.add(new Integer(i));
			}
		}
		for (int i = 0; i < desc.size(); i++) {
			for (int j = desc.get(i).intValue() + 1; j < mi_jobNum; j++) {
				if (existPrecedence(desc.get(i).intValue(), j) && !desc.contains(new Integer(j))) {
					desc.add(new Integer(j));
				}
			}
		}
		double descET = 0.0;
		for (int i = 0; i < desc.size(); i++) {
			descET += maM_jobs[desc.get(i).intValue()].getTaskExecTime(1, hc);
		}
		
		Vector<Integer> ance = new Vector<Integer>();
		ance.add(new Integer(jobID));
		for (int i = 0; i < jobID; i++) {
			if (existPrecedence(i, jobID)) {
				ance.add(new Integer(i));
			}
		}
		for (int i = 0; i < ance.size(); i++) {
			for (int j = 0; j < ance.get(i).intValue(); j++) {
				if (existPrecedence(j, ance.get(i).intValue()) && !ance.contains(new Integer(j))) {
					ance.add(new Integer(j));
				}
			}
		}
		double anceET = 0.0;
		for (int i = 0; i < ance.size(); i++) {
			anceET += maM_jobs[ance.get(i).intValue()].getTaskExecTime(1, hc);
		}
		
		double totalET = anceET + descET;
		double jobLFT = md_readyTime + (md_deadline - md_readyTime) * anceET / totalET;
		return jobLFT;
	}
	
	// Find the longest unmapped path ending at a job with the earliest LFT from the workflow DAG
	public Pipeline getCriticalPath(HeteroCluster hc, int type) {
		updateJobEST();
		updateJobLFT();
		double minLFT = Constant.uncertainDouble;
		int jobMinLFT = Constant.uncertainInt;
		for (int i = 0; i < mi_jobNum; i++) {
			if (!maM_jobs[i].isMapped()) {
				if (maM_jobs[i].getLFT() != Constant.uncertainDouble && (minLFT > maM_jobs[i].getLFT() || minLFT == Constant.uncertainDouble)) {
					minLFT = maM_jobs[i].getLFT();
					jobMinLFT = i;
				}
			}
		}
		if (minLFT == Constant.uncertainDouble) {
			System.out.println("minLFT == Constant.uncertainDouble !");
			return null;
		}
		
		return getCriticalPath(jobMinLFT, hc, type);
	}
	
	// Find the longest unmapped path ending at the job endJobID from the workflow DAG
	private Pipeline getCriticalPath(int endJobID, HeteroCluster hc, int type) {
		if (maM_jobs[endJobID].isMapped()) {
			return null;
		}
		
		// Dynamic programming
		double[] execTimes = new double[endJobID + 1];
		int[] precJobIDs = new int[endJobID + 1];
		for (int i = 0; i <= endJobID; i++) {
			execTimes[i] = Constant.uncertainDouble;
			precJobIDs[i] = Constant.uncertainInt;
			if (maM_jobs[i].isMapped()) {
				continue;
			}
			switch (type) {
				case avgExecTime:
					execTimes[i] = maM_jobs[i].getTaskExecTime(1, hc);
					break;
				case minExecTime:
					execTimes[i] = maM_jobs[i].getMinTaskExecTime(maM_jobs[i].getMaxTaskNum(), hc);
					break;
				default:
					execTimes[i] = maM_jobs[i].getTaskExecTime(1, hc);
					break;
			}
			for (int j = 0; j < i; j++) {
				if (maad_precedences[j][i] > 0.0) {
					if (!maM_jobs[j].isMapped() && execTimes[i] < execTimes[j] + maM_jobs[i].getTaskExecTime(1, hc)) {
						execTimes[i] = execTimes[j] + maM_jobs[i].getTaskExecTime(1, hc);
						precJobIDs[i] = j;
					}
				}
			}
		}
		
		// Keep the track of the path
		Vector<Integer> path = new Vector<Integer>();
		path.add(new Integer(endJobID));
		while (precJobIDs[path.get(0).intValue()] >= 0) {
			int jobID = precJobIDs[path.get(0).intValue()];
			path.add(0, new Integer(jobID));
		}
		Pipeline cp = new Pipeline(path.size());
		for (int i = 0; i < cp.getJobNum(); i++) {
			cp.setJobID(i, path.get(i).intValue());
		}
		cp.setEST(maM_jobs[cp.getJobID(0)].getEST());
		cp.setLFT(maM_jobs[endJobID].getLFT());
		return cp;
	}
	
	public boolean existUnmappedJob() {
		for (int i = 0; i < mi_jobNum; i++) {
			if (!maM_jobs[i].isMapped()) {
				return true;
			}
		}
		return false;
	}
	
	public double calcDynEnergy(Pipeline pl, HomoCluster hc) {
		double dynEnergy = 0.0;
		for (int i = 0; i < pl.getJobNum(); i++) {
			MoldableJob job = maM_jobs[pl.getJobID(i)];
			if (job.isMapped()) {
				dynEnergy += job.getJobDynEnergy(job.getTaskNum(), hc);
			}
		}
		if (dynEnergy <= 0.0) {
			System.out.println("Error: dynEnergy <= 0.0 !");
		}
		return dynEnergy;
	}
	
	/* end: used by MapperBAWMEE */
	
	/* begin: used for MapperEEDAW */
	public void initProgress(double period, HeteroCluster hc) {
		md_currTask = new TaskIndexTriple(mi_wfID, 0, 0);
		md_schedTime = Math.ceil(md_readyTime / period) * period;
		double execTime = 0.0;
		for (int i = 0; i < mi_jobNum; i++) {
			MoldableJob job = maM_jobs[i];
			execTime += job.getTaskNum() * job.getTaskExecTime(job.getTaskNum(), hc);
		}
		md_taskRank = md_deadline - execTime;
	}
	
	public boolean nextTask(double period, double currTime, HeteroCluster hc) {
		int currJobID = md_currTask.getJobID();
		int currTaskID = md_currTask.getTaskID();
		MoldableJob currJob = maM_jobs[currJobID];
		if (currJobID >= mi_jobNum - 1 && currTaskID >= currJob.getTaskNum() - 1) {
			return false;
		}
		if (currTaskID < currJob.getTaskNum() - 1) {
			currTaskID++;
		} else {
			currJobID++;
			currTaskID = 0;
		}
		md_currTask = new TaskIndexTriple(mi_wfID, currJobID, currTaskID);
		
		currJob = maM_jobs[currJobID];
		double schedTime = Math.ceil(currJob.getEST() / period) * period;
		if (schedTime < currTime) {
			schedTime = currTime;
		}
		md_schedTime = schedTime;
		
		double execTime = (currJob.getTaskNum() - currTaskID) * currJob.getTaskExecTime(currJob.getTaskNum(), hc);
		for (int i = currJobID + 1; i < mi_jobNum; i++) {
			MoldableJob job = maM_jobs[i];
			execTime += job.getTaskNum() * job.getTaskExecTime(job.getTaskNum(), hc);
		}
		md_taskRank = md_deadline - execTime;
		return true;
	}
	
	public double estiJobLFT(int jobID, HeteroCluster hc) {
		Vector<Integer> desc = new Vector<Integer>();
		for (int i = jobID + 1; i < mi_jobNum; i++) {
			if (existPrecedence(jobID, i)) {
				desc.add(new Integer(i));
			}
		}
		for (int i = 0; i < desc.size(); i++) {
			for (int j = desc.get(i).intValue() + 1; j < mi_jobNum; j++) {
				if (existPrecedence(desc.get(i).intValue(), j) && !desc.contains(new Integer(j))) {
					desc.add(new Integer(j));
				}
			}
		}
		double descET = 0.0;
		for (int i = 0; i < desc.size(); i++) {
			MoldableJob job = maM_jobs[desc.get(i).intValue()];
			descET += job.getTaskExecTime(job.getTaskNum(), hc) * job.getTaskNum();
		}
		
		double currET = maM_jobs[jobID].getTaskExecTime(maM_jobs[jobID].getTaskNum(), hc) * maM_jobs[jobID].getTaskNum();
		
		double totalET = currET + descET;
		double jobEST = maM_jobs[jobID].getEST();
		double jobLFT = jobEST + (md_deadline - jobEST) * currET / totalET;
		return jobLFT;
	}
	/* end: used for MapperEEDAW */
	
	public int getWfID() {
		return mi_wfID;
	}
	
	public int getJobNum() {
		return mi_jobNum;
	}
	
	public MoldableJob getJob(int jobID) {
		return maM_jobs[jobID];
	}
	
	public boolean existPrecedence(int jobID1, int jobID2) {
		if (jobID1 < 0 || jobID1 > mi_jobNum) {
			return false;
		}
		if (jobID2 < 0 || jobID2 > mi_jobNum) {
			return false;
		}
		if (maad_precedences[jobID1][jobID2] > 0.0) {
			return true;
		}
		return false;
	}
	
	public double getReadyTime() {
		return md_readyTime;
	}
	
	public double getDeadline() {
		return md_deadline;
	}
	
	public void setDeadline(double deadline) {
		md_deadline = deadline;
	}
	
	public double getDeadlineBaseline() {
		return md_deadlineBaseline;
	}
	
	public TaskIndexTriple getCurrTask() {
		return md_currTask;
	}
	
	public void setCurrTask(TaskIndexTriple tit) {
		md_currTask = tit;
	}
	
	public double getTaskRank() {
		return md_taskRank;
	}
	
	public void setTaskRank(double taskRank) {
		md_taskRank = taskRank;
	}
	
	public double getSchedTime() {
		return md_schedTime;
	}
	
	public void setSchedTime(double schedTime) {
		md_schedTime = schedTime;
	}
	
	public void print(HeteroCluster hc) {
		System.out.println("Workflow wfID = " + mi_wfID);
		System.out.println("    jobNum = " + mi_jobNum);
		System.out.println("    readyTime = " + md_readyTime);
		System.out.println("    deadline = " + md_deadline);
		for (int i = 1; i < mi_jobNum; i++) {
			for (int j = 0; j < i; j++) {
				if (maad_precedences[j][i] == 1.0) {
					System.out.println("        " + j + " -> " + i);
				}
			}
		}
		System.out.println("    Critical Paths: ");
		for (int i = 0; i < mi_jobNum; i++) {
			Pipeline cp = getCriticalPath(i, hc, avgExecTime);
			if (cp != null) {
				System.out.print("        ");
				for (int j = 0; j < cp.getJobNum(); j++) {
					if (j < cp.getJobNum() - 1) {
						System.out.print(cp.getJobID(j) + " -> ");
					} else {
						System.out.println(cp.getJobID(j));
					}
				}
			}
		}

		for (int j = 0; j < mi_jobNum; j++) {
			if (maM_jobs[j].mi_wfID != mi_wfID) {
				System.out.println("    wfID = " + maM_jobs[j].mi_wfID);
			}
			System.out.println("    jobID = " + maM_jobs[j].mi_jobID);
			System.out.println("        md_est = " + maM_jobs[j].md_est);
			System.out.println("        md_lft = " + maM_jobs[j].md_lft);
			System.out.println("        md_ast = " + maM_jobs[j].md_ast);
			System.out.println("        md_aft = " + maM_jobs[j].md_aft);
			System.out.println("        maxTaskNum = " + maM_jobs[j].mi_maxTaskNum);
			System.out.println("        taskMemSize = " + maM_jobs[j].mi_taskMemSize);
			for (int k = 0; k < HeteroCluster.clusterNum; k++) {
				System.out.println("        taskProcUtils(" + k + ") = " + maM_jobs[j].md_taskProcUtils[k]);
			}
			System.out.println("        mapWorkload(1) = " + maM_jobs[j].mai_jobWorkloads[0]);
			for (int k = 0; k < maM_jobs[j].mi_maxTaskNum; k++) {
				System.out.println("        jobWorkload(" + (k+1) + ") = " + maM_jobs[j].mai_jobWorkloads[k]);
			}
			
			System.out.println("        taskNum = " + maM_jobs[j].mi_taskNum);
			for (int k = 0; k < maM_jobs[j].mi_taskNum; k++) {
				if (maM_jobs[j].maT_tasks[k].mi_wfID != mi_wfID) {
					System.out.println("        wfID = " + maM_jobs[j].maT_tasks[k].mi_wfID);
				}
				if (maM_jobs[j].maT_tasks[k].mi_jobID != maM_jobs[j].mi_jobID) {
					System.out.println("        jobID = " + maM_jobs[j].maT_tasks[k].mi_jobID);
				}
				System.out.println("        taskID = " + maM_jobs[j].maT_tasks[k].mi_taskID);
				System.out.println("            startTime = " + maM_jobs[j].maT_tasks[k].md_startTime);
				System.out.println("            endTime = " + maM_jobs[j].maT_tasks[k].md_endTime);
				System.out.println("            machID = " + maM_jobs[j].maT_tasks[k].mi_machID);
			}
		}
	}
}
