package njit.bd.common;

public class IntPair {
	int mi_num1;
	int mi_num2;
	
	public IntPair() {}
	public IntPair(int num1, int num2) {
		mi_num1 = num1;
		mi_num2 = num2;
	}
	
	public int getFst() {
		return mi_num1;
	}
	
	public void setFst(int num1) {
		mi_num1 = num1;
	}
	
	public int getSnd() {
		return mi_num2;
	}
	
	public void setSnd(int num2) {
		mi_num2 = num2;
	}
}
