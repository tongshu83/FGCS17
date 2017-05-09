package njit.bd.common;

public class Inputs {
	public int mi_inputNum = 0;
	public Input[] mI_inputs;
	
	public class Input {
		public int mi_machNum = 128;
		public int mi_jobNum = 50;
		public double md_avgArrInt = 1800.0;
		public double md_simulTime = 259200.0;
		public double md_deadlineScale = 0.1;
		public double md_epsilon = 0.2;
		public int md_wfShapeNum = 1;
		
		public Input() { }
	}
	
	public Inputs() {
		mi_inputNum = 20;
		mI_inputs = new Input[mi_inputNum];
		for (int i = 0; i < mi_inputNum; i++) {
			mI_inputs[i] = new Input();
		}
		
		mI_inputs[0].mi_machNum = 4;
		mI_inputs[0].mi_jobNum = 5;
		mI_inputs[0].md_avgArrInt = 14400.0;
		mI_inputs[0].md_simulTime = 604800.0;
		
		mI_inputs[1].mi_machNum = 8;
		mI_inputs[1].mi_jobNum = 10;
		mI_inputs[1].md_avgArrInt = 12000.0;
		mI_inputs[1].md_simulTime = 604800.0;
		
		mI_inputs[2].mi_machNum = 12;
		mI_inputs[2].mi_jobNum = 15;
		mI_inputs[2].md_avgArrInt = 9600.0;
		mI_inputs[2].md_simulTime = 604800.0;
		
		mI_inputs[3].mi_machNum = 16;
		mI_inputs[3].mi_jobNum = 20;
		mI_inputs[3].md_avgArrInt = 9000.0;
		mI_inputs[3].md_simulTime = 604800.0;
		
		mI_inputs[4].mi_machNum = 24;
		mI_inputs[4].mi_jobNum = 25;
		mI_inputs[4].md_avgArrInt = 7200.0;
		mI_inputs[4].md_simulTime = 604800.0;
		
		mI_inputs[5].mi_machNum = 32;
		mI_inputs[5].mi_jobNum = 30;
		mI_inputs[5].md_avgArrInt = 6300.0;
		mI_inputs[5].md_simulTime = 259200.0;
		
		mI_inputs[6].mi_machNum = 48;
		mI_inputs[6].mi_jobNum = 35;
		mI_inputs[6].md_avgArrInt = 5400.0;
		mI_inputs[6].md_simulTime = 259200.0;
		
		mI_inputs[7].mi_machNum = 64;
		mI_inputs[7].mi_jobNum = 40;
		mI_inputs[7].md_avgArrInt = 3600.0;
		mI_inputs[7].md_simulTime = 259200.0;
		
		mI_inputs[8].mi_machNum = 96;
		mI_inputs[8].mi_jobNum = 45;
		mI_inputs[8].md_avgArrInt = 2700.0;
		mI_inputs[8].md_simulTime = 259200.0;
		
		mI_inputs[9].mi_machNum = 128;
		mI_inputs[9].mi_jobNum = 50;
		mI_inputs[9].md_avgArrInt = 1800.0;
		mI_inputs[9].md_simulTime = 259200.0;
		
		mI_inputs[10].mi_machNum = 192;
		mI_inputs[10].mi_jobNum = 55;
		mI_inputs[10].md_avgArrInt = 1500.0;
		mI_inputs[10].md_simulTime = 86400.0;
		
		mI_inputs[11].mi_machNum = 256;
		mI_inputs[11].mi_jobNum = 60;
		mI_inputs[11].md_avgArrInt = 1200.0;
		mI_inputs[11].md_simulTime = 86400.0;
		
		mI_inputs[12].mi_machNum = 384;
		mI_inputs[12].mi_jobNum = 65;
		mI_inputs[12].md_avgArrInt = 1080.0;
		mI_inputs[12].md_simulTime = 86400.0;
		
		mI_inputs[13].mi_machNum = 512;
		mI_inputs[13].mi_jobNum = 70;
		mI_inputs[13].md_avgArrInt = 900.0;
		mI_inputs[13].md_simulTime = 86400.0;
		
		mI_inputs[14].mi_machNum = 768;
		mI_inputs[14].mi_jobNum = 75;
		mI_inputs[14].md_avgArrInt = 720.0;
		mI_inputs[14].md_simulTime = 86400.0;
		
		mI_inputs[15].mi_machNum = 1024;
		mI_inputs[15].mi_jobNum = 80;
		mI_inputs[15].md_avgArrInt = 600.0;
		mI_inputs[15].md_simulTime = 28800.0;
		
		mI_inputs[16].mi_machNum = 1536;
		mI_inputs[16].mi_jobNum = 85;
		mI_inputs[16].md_avgArrInt = 480.0;
		mI_inputs[16].md_simulTime = 28800.0;
		
		mI_inputs[17].mi_machNum = 2048;
		mI_inputs[17].mi_jobNum = 90;
		mI_inputs[17].md_avgArrInt = 360.0;
		mI_inputs[17].md_simulTime = 28800.0;
		
		mI_inputs[18].mi_machNum = 3072;
		mI_inputs[18].mi_jobNum = 95;
		mI_inputs[18].md_avgArrInt = 300.0;
		mI_inputs[18].md_simulTime = 28800.0;
		
		mI_inputs[19].mi_machNum = 4096;
		mI_inputs[19].mi_jobNum = 100;
		mI_inputs[19].md_avgArrInt = 240.0;
		mI_inputs[19].md_simulTime = 28800.0;
	}
}
