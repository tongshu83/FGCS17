package njit.bd.common;

public class DblDblIntIntQuad {
	double md_real1;
	double md_real2;
	int mi_num1;
	int mi_num2;
	
	public DblDblIntIntQuad() {}
	public DblDblIntIntQuad(double real1, double real2, int num1, int num2) {
		md_real1 = real1;
		md_real2 = real2;
		mi_num1 = num1;
		mi_num2 = num2;
	}
	
	public double getFstDbl() {
		return md_real1;
	}
	
	public void setFstDbl(double real1) {
		md_real1 = real1;
	}
	
	public double getSndDbl() {
		return md_real2;
	}
	
	public void setSndDbl(double real2) {
		md_real2 = real2;
	}
	
	public int getFstInt() {
		return mi_num1;
	}
	
	public void setFstInt(int num) {
		mi_num1 = num;
	}
	
	public int getSndInt() {
		return mi_num2;
	}
	
	public void setSndInt(int num) {
		mi_num2 = num;
	}
}
