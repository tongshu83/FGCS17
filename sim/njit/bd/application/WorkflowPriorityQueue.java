package njit.bd.application;

import njit.bd.common.Constant;

public class WorkflowPriorityQueue {
	public final static int jobSchedTime = 0;    // used by MapperATPIEEDA
	public final static int TaskPriority = 1;    // used by MapperEEDA
	
	int mi_wfNum = 0;
	int mi_capacity = 0;
	int[] mai_workflowQueue;
	WorkflowGenerator mW_wfg;
	
	public WorkflowPriorityQueue(WorkflowGenerator wfg) {
		mi_wfNum = 0;
		mi_capacity = wfg.getWfNum();
		mai_workflowQueue = new int[mi_capacity];
		for (int i = 0; i < mi_capacity; i++) {
			mai_workflowQueue[i] = Constant.uncertainInt;
		}
		mW_wfg = wfg;
	}
	
	public void enqueue(int wfID, int type) {
		int curIndex = mi_wfNum;
		mai_workflowQueue[curIndex] = wfID;
		mi_wfNum++;
		while (curIndex > 0) {
			int parIndex = (curIndex - 1) / 2;
			int curWfID = mai_workflowQueue[curIndex];
			int parWfID = mai_workflowQueue[parIndex];
			if (compare(curWfID, parWfID, type)) {
				mai_workflowQueue[curIndex] = parWfID;
				mai_workflowQueue[parIndex] = curWfID;
				curIndex = parIndex;
			} else {
				break;
			}
		}
	}
	
	public int dequeue(int type) {
		int top = mai_workflowQueue[0];
		
		mai_workflowQueue[0] = mai_workflowQueue[mi_wfNum - 1];
		mi_wfNum--;
		mai_workflowQueue[mi_wfNum] = Constant.uncertainInt;
		int curIndex = 0;
		while (curIndex < mi_wfNum / 2) {
			int curWfID = mai_workflowQueue[curIndex];
			int leftChildIndex = 2 * curIndex + 1;
			int rightChildIndex = 2 * curIndex + 2;
			int childIndex = leftChildIndex;
			int childWfID = mai_workflowQueue[childIndex];
			if (rightChildIndex < mi_wfNum) {
				int rightWfID = mai_workflowQueue[rightChildIndex];
				if (compare(rightWfID, childWfID, type)) {
					childIndex = rightChildIndex;
					childWfID = mai_workflowQueue[childIndex];
				}
			}
			if (compare(childWfID, curWfID, type)) {
				mai_workflowQueue[curIndex] = childWfID;
				mai_workflowQueue[childIndex] = curWfID;
				curIndex = childIndex;
			} else {
				break;
			}
		}
		return top;
	}
	
	private boolean compare(int wfID1, int wfID2, int type) {
		if (type == jobSchedTime) {
			if (mW_wfg.getWorkflow(wfID1).getSchedTime() < mW_wfg.getWorkflow(wfID2).getSchedTime()) {
				return true;
			} else {
				return false;
			}
		}
		if (type == TaskPriority) {
			if (mW_wfg.getWorkflow(wfID1).getSchedTime() < mW_wfg.getWorkflow(wfID2).getSchedTime()) {
				return true;
			} else if (mW_wfg.getWorkflow(wfID1).getSchedTime() == mW_wfg.getWorkflow(wfID2).getSchedTime() 
					&& mW_wfg.getWorkflow(wfID1).getTaskRank() < mW_wfg.getWorkflow(wfID2).getTaskRank()) {
				return true;
			} else {
				return false;
			}
		}
		
		return true;
	}
	
	public int size() {
		return mi_wfNum;
	}
}
