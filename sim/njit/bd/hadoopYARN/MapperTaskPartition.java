package njit.bd.hadoopYARN;

import java.util.Vector;

import njit.bd.application.MoldableJob;
import njit.bd.application.Workflow;
import njit.bd.common.DblDblIntIntQuad;
import njit.bd.dataCenter.HomoCluster;

public class MapperTaskPartition extends ResourceManager {
	Vector<Vector<DblDblIntIntQuad>> mVVD_jobClusterTbl = null;
	
	public MapperTaskPartition() { }
	
	public void getJobClusterTable(Workflow wf, boolean taskPartition) {	// from small to big on the first column
		mVVD_jobClusterTbl = new Vector<Vector<DblDblIntIntQuad>>();
		for (int i = 0; i < wf.getJobNum(); i++) {
			MoldableJob job = wf.getJob(i);
			Vector<DblDblIntIntQuad> optList = new Vector<DblDblIntIntQuad>();
			int minTaskNum = job.getTaskNum();
			int maxTaskNum = job.getTaskNum();
			if (taskPartition) {
				minTaskNum = 1;
				maxTaskNum = job.getMaxTaskNum();
			}
			for (int j = minTaskNum; j <= maxTaskNum; j++) {
				for (int k = 0; k < mH_hc.getClusterNum(); k++) {
					HomoCluster hc = mH_hc.getHomoCluster(k);
					double taskExecTime = job.getTaskExecTime(j, hc);
					double jobEnergy = job.getJobDynEnergy(j, hc);
					optList.add(new DblDblIntIntQuad(taskExecTime, jobEnergy, j, k));
				}
			}
			
			for (int j = 0; j < optList.size(); j++) {
				for (int k = j + 1; k < optList.size(); k++) {
					DblDblIntIntQuad option1 = optList.get(j);
					DblDblIntIntQuad option2 = optList.get(k);
					if (option1.getFstDbl() > option2.getFstDbl() 
							|| option1.getFstDbl() == option2.getFstDbl() && option1.getSndDbl() > option2.getSndDbl()
							|| option1.getFstDbl() == option2.getFstDbl() && option1.getSndDbl() == option2.getSndDbl() 
								&& (job.getTaskProcUtil(option1.getSndInt()) < job.getTaskProcUtil(option2.getSndInt()))) {
						optList.set(j, option2);
						optList.set(k, option1);
					}
				}
			}
			for (int j = 0; j < optList.size() - 1; j++) {
				DblDblIntIntQuad option1 = optList.get(j);
				DblDblIntIntQuad option2 = optList.get(j + 1);
				if (option1.getSndDbl() <= option2.getSndDbl()) {
					optList.remove(j + 1);
					j--;
				}
			}
			
			mVVD_jobClusterTbl.add(optList);
		}
	}
}
