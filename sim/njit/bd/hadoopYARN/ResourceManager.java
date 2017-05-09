package njit.bd.hadoopYARN;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Scanner;

import njit.bd.application.*;
import njit.bd.common.Inputs;
import njit.bd.dataCenter.*;

public class ResourceManager {
	HeteroCluster mH_hc;
	TimeResourceTable mT_trt;
	double md_epsilon = 0.2;    // in seconds
	
	public ResourceManager() { }
	public ResourceManager(HeteroCluster hc) {
		mH_hc = hc;
		mT_trt = new TimeResourceTable(hc);
	}
	
	public double calcStaEnergy() {
		double staEnergy = 0.0;
		for (int i = 0; i < mH_hc.getClusterNum(); i++) {
			HomoCluster homoCluster = mH_hc.getHomoCluster(i);
			int machStartID = homoCluster.getMachStartID();
			int machNum = homoCluster.getMachNum();
			int coreNum = homoCluster.getCoreNum();
			double staMachTime = 0.0;
			for (int j = 0; j < mT_trt.mVR_timeVector.size(); j++) {
				double time = mT_trt.mVR_timeVector.get(j).getEndTime() - mT_trt.mVR_timeVector.get(j).getStartTime();
				for (int k = machStartID; k < machStartID + machNum; k++) {
					int usedCoreNum = coreNum - mT_trt.mVR_timeVector.get(j).getCoreNum(k);
					if (usedCoreNum > 0) {
						staMachTime += time;
					}
				}
			}
			staEnergy += homoCluster.getStaPower() * staMachTime;
		}
		if (staEnergy < 0.0) {
			System.out.println("Error: staEnergy < 0.0 !");
		}
		return staEnergy;
	}
	
	public double calcDynEnergy(WorkflowGenerator wfg) {
		double dynEnergy = 0.0;
		int wfNum = wfg.getWfNum();
		for (int i = 0; i < wfNum; i++) {
			Workflow wf = wfg.getWorkflow(i);
			int jobNum = wf.getJobNum();
			for (int j = 0; j < jobNum; j++) {
				MoldableJob job = wf.getJob(j);
				int taskNum = job.getTaskNum();
				for (int k = 0; k < taskNum; k++) {
					Task task = job.getTask(k);
					int homoClusterID = mH_hc.getMachine(task.getMachID()).getClusterID();
					HomoCluster homoC = mH_hc.getHomoCluster(homoClusterID);
					dynEnergy += (task.getEndTime() - task.getStartTime()) * homoC.getDynPower() * job.getTaskProcUtil(homoClusterID);
				}
			}
		}
		return dynEnergy;
	}
	
	public static void main(String args[]) {
		int iteration = 5;
		boolean defaultInput = true;
		int machNumLB = 128;//4;
		int machNumUB = 128;//4096;
		int jobNumLB = 50;//5;
		int jobNumUB = 50;//100;
		int jobNumDiff = 2;
		double avgArrIntervalLB = 1800.0;// 900.0;
		double avgArrIntervalUB = 1800.0;// 3600.0;
		double deadlineLB = 0.1;//0.05;
		double deadlineUB = 0.1;//1;
		double simulTime = 259200.0; // three days
		double epsilonLB = 0.2;//0.05;
		double epsilonUB = 0.2;//0.4;
		int wfShapeNum = 1;
		String fileName = "data";
		
		Scanner reader = new Scanner(System.in);
		System.out.print("iteration (1 - 20) = ");
		iteration = reader.nextInt();
		System.out.print("defaultInput (true or false) = ");
		defaultInput = reader.nextBoolean();
		if (defaultInput) {
			fileName += "_probSize.dat";
		} else {
			System.out.print("machNumLowerBound (64 - 256) = ");
			machNumLB = reader.nextInt();
			System.out.print("machNumUpperBound (64 - 256) = ");
			machNumUB = reader.nextInt();
			System.out.print("jobNumLowerBound (5 - 100) = ");
			jobNumLB = reader.nextInt();
			System.out.print("jobNumUpperBound (5 - 100) = ");
			jobNumUB = reader.nextInt();
			System.out.print("jobNumDiff (0 - 10) = ");
			jobNumDiff = reader.nextInt();
			System.out.print("avgArrIntervalLowerBound (900 - 3600) = ");
			avgArrIntervalLB = reader.nextDouble();
			System.out.print("avgArrIntervalUpperBound (900 - 3600) = ");
			avgArrIntervalUB = reader.nextDouble();
			System.out.print("deadlineLowerBound (0.05 - 1) = ");
			deadlineLB = reader.nextDouble();
			System.out.print("deadlineUpperBound (0.05 - 1) = ");
			deadlineUB = reader.nextDouble();
			System.out.print("simulationTime (86400 - 604800) = ");
			simulTime = reader.nextDouble();
			System.out.print("epsilonLowerBound (0.05 - 0.4) = ");
			epsilonLB = reader.nextDouble();
			System.out.print("epsilonUpperBound (0.05 - 0.4) = ");
			epsilonUB = reader.nextDouble();
			System.out.print("wfShapeNum (1 - 5) = ");
			wfShapeNum = reader.nextInt();
			
			if (machNumLB < machNumUB) {
				fileName += "_" + machNumLB + "-" + machNumUB;
			} else {
				fileName += "_" + machNumLB;
			}
			if (jobNumLB < jobNumUB) {
				fileName += "_" + jobNumLB + "-" + jobNumUB;
			} else {
				fileName += "_" + jobNumLB;
			}
			if (avgArrIntervalLB < avgArrIntervalUB) {
				fileName += "_" + (int) avgArrIntervalLB + "-" + (int) avgArrIntervalUB;
			} else {
				fileName += "_" + (int) avgArrIntervalLB;
			}
			if (deadlineLB < deadlineUB) {
				fileName += "_" + deadlineLB + "-" + deadlineUB;
			} else {
				fileName += "_" + deadlineLB;
			}
			fileName += "_" + (int) simulTime;
			if (epsilonLB < epsilonUB) {
				fileName += "_" + epsilonLB + "-" + epsilonUB;
			} else {
				fileName += "_" + epsilonLB;
			}
			fileName += "_" + wfShapeNum + ".dat";
		}
		
		reader.close();
		
		try {
			PrintWriter writer = new PrintWriter(fileName, "UTF-8");
			writer.print("Scheduler\t");
			writer.print("dynEnergy(J)\t");
			writer.print("missedDLR\t");
			writer.print("avgDelay(s)\t");
			writer.print("taskNumPerJob\t");
			writer.print("wlRedPrec\t");
			writer.print("execTime(ms)\t");
			writer.println("wfNum\t");
			
			for (int i = 0; i < iteration; i++) {
				System.out.println(i + ":");
				int loopNum = 1;
				Inputs inputs = new Inputs();
				if (defaultInput) {
					loopNum = inputs.mi_inputNum;
				}
				for (int j = 0; j < loopNum; j++) {
					if (defaultInput) {
						machNumLB = inputs.mI_inputs[j].mi_machNum;
						machNumUB = inputs.mI_inputs[j].mi_machNum;
						jobNumLB = inputs.mI_inputs[j].mi_jobNum;
						jobNumUB = inputs.mI_inputs[j].mi_jobNum;
						avgArrIntervalLB = inputs.mI_inputs[j].md_avgArrInt;
						avgArrIntervalUB = inputs.mI_inputs[j].md_avgArrInt;
						deadlineLB = inputs.mI_inputs[j].md_deadlineScale;
						deadlineUB = inputs.mI_inputs[j].md_deadlineScale;
						simulTime = inputs.mI_inputs[j].md_simulTime;
						epsilonLB = inputs.mI_inputs[j].md_epsilon;
						epsilonUB = inputs.mI_inputs[j].md_epsilon;
						wfShapeNum = inputs.mI_inputs[j].md_wfShapeNum;
					}
					
					for (int machNum = machNumLB; machNum <= machNumUB; machNum += 16) {
						HeteroCluster hc = new HeteroCluster(machNum, HeteroCluster.heteroCluster);
						for (int jobNum = jobNumLB; jobNum <= jobNumUB; jobNum += 5) {
							int minJobNum = jobNum - jobNumDiff;
							int maxJobNum = jobNum + jobNumDiff;
							for (double avgArrInterval = avgArrIntervalLB; avgArrInterval <= avgArrIntervalUB + 0.1; avgArrInterval += 300.0) {
								for (int wfShape = 0; wfShape < wfShapeNum; wfShape++) {
									WorkflowGenerator wfg = new WorkflowGenerator(minJobNum, maxJobNum, avgArrInterval, simulTime, wfShape, hc);
									for (double deadlineScale = deadlineLB; deadlineScale < deadlineUB + 0.01; deadlineScale += 0.05) {
										wfg.scaleDeadline(deadlineScale);
										
										double dynEnergy;
										double missedDeadlineRate;
										double avgDelay;
										double avgTaskNum;
										double wlRedPrec;
										long startTime;
										long execTime;
										
										for (double epsilon = epsilonLB; epsilon <= epsilonUB; epsilon += 0.05) {
											wfg.clear();
											MapperBAWMEE mapperBAWMEE = new MapperBAWMEE(hc, epsilon);
											startTime = new Date().getTime();
											mapperBAWMEE.mapping(wfg);
											execTime = new Date().getTime() - startTime;
											dynEnergy = mapperBAWMEE.calcDynEnergy(wfg);
											missedDeadlineRate = wfg.getMissedDeadlineRate();
											avgDelay = wfg.getAvgDelay();
											avgTaskNum = wfg.getAvgTaskNum();
											wlRedPrec = wfg.getWlRed();
											System.out.print("    BAWMEE (machNum=" + machNum + ", jobNum=" + minJobNum + "-" + maxJobNum + 
													", arrInt=" + avgArrInterval + ", deadline=" + deadlineScale + ", wfShape=" + wfShape + 
													", epsilon=" + epsilon + "): ");
											System.out.print("dynEnergy = " + dynEnergy);
											System.out.print(", missedDLR = " + missedDeadlineRate);
											System.out.print(", avgDelay = " + avgDelay);
											System.out.print(", avgTaskNum = " + avgTaskNum);
											System.out.print(", wlRedPrec = " + wlRedPrec);
											System.out.print(", execTime = " + execTime);
											System.out.println(", wfNum = " + wfg.getWfNum());
											if (minJobNum < maxJobNum) {
												writer.print("BAWMEE (machNum=" + machNum + ", jobNum=" + minJobNum + "-" + maxJobNum + 
														", arrInt=" + avgArrInterval + ", deadline=" + deadlineScale + ", wfShape=" + wfShape + 
														", epsilon=" + epsilon + ")\t");
											} else {
												writer.print("BAWMEE (machNum=" + machNum + ", jobNum=" + minJobNum + ", arrInt=" + avgArrInterval + 
														", deadline=" + deadlineScale + ", wfShape=" + wfShape + ", epsilon=" + epsilon + ")\t");
											}
											writer.print(dynEnergy + "\t");
											writer.print(missedDeadlineRate + "\t");
											writer.print(avgDelay + "\t");
											writer.print(avgTaskNum + "\t");
											writer.print(wlRedPrec + "\t");
											writer.print(execTime + "\t");
											writer.println(wfg.getWfNum() + "\t");
											writer.flush();
										}
										
										wfg.clear();
										MapperEEDAW mapperEEDAW = new MapperEEDAW(hc, 60.0);
										startTime = new Date().getTime();
										mapperEEDAW.schedule(wfg);
										execTime = new Date().getTime() - startTime;
										dynEnergy = mapperEEDAW.calcDynEnergy(wfg);
										missedDeadlineRate = wfg.getMissedDeadlineRate();
										avgDelay = wfg.getAvgDelay();
										avgTaskNum = wfg.getAvgTaskNum();
										wlRedPrec = wfg.getWlRed();
										System.out.print("    EEDAW (machNum=" + machNum + ", jobNum=" + minJobNum + "-" + maxJobNum + 
												", arrInt=" + avgArrInterval + ", deadline=" + deadlineScale + ", wfShape=" + wfShape + "): ");
										System.out.print("dynEnergy = " + dynEnergy);
										System.out.print(", missedDLR = " + missedDeadlineRate);
										System.out.print(", avgDelay = " + avgDelay);
										System.out.print(", avgTaskNum = " + avgTaskNum);
										System.out.print(", wlRedPrec = " + wlRedPrec);
										System.out.print(", execTime = " + execTime);
										System.out.println(", wfNum = " + wfg.getWfNum());
										if (minJobNum < maxJobNum) {
											writer.print("EEDAW (machNum=" + machNum + ", jobNum=" + minJobNum + "-" + maxJobNum + 
													", arrInt=" + avgArrInterval + ", deadline=" + deadlineScale + ", wfShape=" + wfShape + ")\t");
										} else {
											writer.print("EEDAW (machNum=" + machNum + ", jobNum=" + minJobNum + ", arrInt=" + avgArrInterval + 
													", deadline=" + deadlineScale + ", wfShape=" + wfShape + ")\t");
										}
										writer.print(dynEnergy + "\t");
										writer.print(missedDeadlineRate + "\t");
										writer.print(avgDelay + "\t");
										writer.print(avgTaskNum + "\t");
										writer.print(wlRedPrec + "\t");
										writer.print(execTime + "\t");
										writer.println(wfg.getWfNum() + "\t");
										writer.flush();
										
										wfg.clear();
										MapperMinDED mapperMinDED = new MapperMinDED(hc);
										startTime = new Date().getTime();
										mapperMinDED.schedule(wfg, false);
										execTime = new Date().getTime() - startTime;
										dynEnergy = mapperMinDED.calcDynEnergy(wfg);
										missedDeadlineRate = wfg.getMissedDeadlineRate();
										avgDelay = wfg.getAvgDelay();
										avgTaskNum = wfg.getAvgTaskNum();
										wlRedPrec = wfg.getWlRed();
										System.out.print("    MinD+ED (machNum=" + machNum + ", jobNum=" + minJobNum + "-" + maxJobNum + 
												", arrInt=" + avgArrInterval + ", deadline=" + deadlineScale + ", wfShape=" + wfShape + "): ");
										System.out.print("dynEnergy = " + dynEnergy);
										System.out.print(", missedDLR = " + missedDeadlineRate);
										System.out.print(", avgDelay = " + avgDelay);
										System.out.print(", avgTaskNum = " + avgTaskNum);
										System.out.print(", wlRedPrec = " + wlRedPrec);
										System.out.print(", execTime = " + execTime);
										System.out.println(", wfNum = " + wfg.getWfNum());
										if (minJobNum < maxJobNum) {
											writer.print("MinD+ED (machNum=" + machNum + ", jobNum=" + minJobNum + "-" + maxJobNum + 
													", arrInt=" + avgArrInterval + ", deadline=" + deadlineScale + ", wfShape=" + wfShape + ")\t");
										} else {
											writer.print("MinD+ED (machNum=" + machNum + ", jobNum=" + minJobNum + ", arrInt=" + avgArrInterval + 
													", deadline=" + deadlineScale + ", wfShape=" + wfShape + ")\t");
										}
										writer.print(dynEnergy + "\t");
										writer.print(missedDeadlineRate + "\t");
										writer.print(avgDelay + "\t");
										writer.print(avgTaskNum + "\t");
										writer.print(wlRedPrec + "\t");
										writer.print(execTime + "\t");
										writer.println(wfg.getWfNum() + "\t");
										writer.flush();
										
										wfg.clear();
										MapperMinDEEDAJ mapperMinDEEDAJ = new MapperMinDEEDAJ(hc, 6.0);
										startTime = new Date().getTime();
										mapperMinDEEDAJ.schedule(wfg, false);
										execTime = new Date().getTime() - startTime;
										dynEnergy = mapperMinDEEDAJ.calcDynEnergy(wfg);
										missedDeadlineRate = wfg.getMissedDeadlineRate();
										avgDelay = wfg.getAvgDelay();
										avgTaskNum = wfg.getAvgTaskNum();
										wlRedPrec = wfg.getWlRed();
										System.out.print("    MinD+EEDAJ (machNum=" + machNum + ", jobNum=" + minJobNum + "-" + maxJobNum + 
												", arrInt=" + avgArrInterval + ", deadline=" + deadlineScale + ", wfShape=" + wfShape + "): ");
										System.out.print("dynEnergy = " + dynEnergy);
										System.out.print(", missedDLR = " + missedDeadlineRate);
										System.out.print(", avgDelay = " + avgDelay);
										System.out.print(", avgTaskNum = " + avgTaskNum);
										System.out.print(", wlRedPrec = " + wlRedPrec);
										System.out.print(", execTime = " + execTime);
										System.out.println(", wfNum = " + wfg.getWfNum());
										if (minJobNum < maxJobNum) {
											writer.print("MinD+EEDAJ (machNum=" + machNum + ", jobNum=" + minJobNum + "-" + maxJobNum + 
													", arrInt=" + avgArrInterval + ", deadline=" + deadlineScale + ", wfShape=" + wfShape + ")\t");
										} else {
											writer.print("MinD+EEDAJ (machNum=" + machNum + ", jobNum=" + minJobNum + ", arrInt=" + avgArrInterval + 
													", deadline=" + deadlineScale + ", wfShape=" + wfShape + ")\t");
										}
										writer.print(dynEnergy + "\t");
										writer.print(missedDeadlineRate + "\t");
										writer.print(avgDelay + "\t");
										writer.print(avgTaskNum + "\t");
										writer.print(wlRedPrec + "\t");
										writer.print(execTime + "\t");
										writer.println(wfg.getWfNum() + "\t");
										writer.flush();
									}
								}
							}
						}
					}
				}
			}
			writer.close();
		} catch (UnsupportedEncodingException e) {
			System.out.println(e.getMessage());
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
		}
	}
}
