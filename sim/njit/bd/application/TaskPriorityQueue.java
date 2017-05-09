package njit.bd.application;

import java.util.Vector;

public class TaskPriorityQueue {    // used by taskConsolidation
	Vector<TaskIndexTriple> mVT_taskQueue;
	Workflow mW_wf;
	
	public TaskPriorityQueue(Workflow wf) {
		mVT_taskQueue = new Vector<TaskIndexTriple>();
		mW_wf = wf;
	}
	
	public void enqueue(TaskIndexTriple tit) {
		if (mW_wf.getWfID() != tit.getWfID()) {
			return;
		}
		mVT_taskQueue.add(tit);
	}
	
	public TaskIndexTriple dequeue() {
		int tailIndex = mVT_taskQueue.size() - 1;
		for (int i = 0; i < tailIndex; i++) {
			double tailFree = mW_wf.getJob(mVT_taskQueue.get(tailIndex).getJobID()).getTask(mVT_taskQueue.get(tailIndex).getTaskID()).getFreedom();
			double curFree = mW_wf.getJob(mVT_taskQueue.get(i).getJobID()).getTask(mVT_taskQueue.get(i).getTaskID()).getFreedom();
			if (tailFree < curFree) {
				TaskIndexTriple temp = mVT_taskQueue.get(tailIndex);
				mVT_taskQueue.set(tailIndex, mVT_taskQueue.get(i));
				mVT_taskQueue.set(i, temp);
			}
		}
		TaskIndexTriple tit = mVT_taskQueue.get(tailIndex);
		mVT_taskQueue.remove(tailIndex);
		return tit;
	}
	
	public int size() {
		return mVT_taskQueue.size();
	}
}
