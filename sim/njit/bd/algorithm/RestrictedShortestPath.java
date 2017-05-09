package njit.bd.algorithm;

import java.lang.Double;
import java.util.Vector;
/*
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
*/
public class RestrictedShortestPath {
	int mi_jobNum = 0;
	int[] mai_maxTaskNum;
	Vector<Vector<Double>> mVD_time;
	Vector<Vector<Double>> mVD_energy;
	
	public class Vertex {
		int mi_vertexID = 0;
		int mi_precEdgeNum = 0;
		int[] mai_precEdges;
		int mi_succEdgeNum = 0;
		int[] mai_succEdges;
		
		public Vertex() { }
		public Vertex(int vertexID, int precEdgeNum, int succEdgeNum) {
			mi_vertexID = vertexID;
			mi_precEdgeNum = precEdgeNum;
			mi_succEdgeNum = succEdgeNum;
			mai_precEdges = new int[precEdgeNum];
			mai_succEdges = new int[succEdgeNum];
		}
	}
	
	public class Edge {
		int mi_edgeID = 0;
		int mi_startVertex = 0;
		int mi_endVertex = 0;
		double md_weight = 0.0;
		double md_cost = 0.0;
		
		public Edge() { }
		public Edge(int edgeID, int startVertex, int endVertex, double weight, double cost) {
			mi_edgeID = edgeID;
			mi_startVertex = startVertex;
			mi_endVertex = endVertex;
			md_weight = weight;
			md_cost = cost;
		}
	}
	
	int mi_vertexNum = 0;
	Vertex[] maV_vertices;
	int mi_edgeNum = 0;
	Edge[] maE_edges;
	int[] mai_path;
	
	public RestrictedShortestPath(int i_jobNum, Vector<Vector<Double>> VD_time, Vector<Vector<Double>> VD_energy)
    {
		mi_jobNum = i_jobNum;
		mai_maxTaskNum = new int[i_jobNum];

		for (int i = 0; i < i_jobNum; i++) {
			if (VD_time.get(i).size() == VD_energy.get(i).size()) {
				mai_maxTaskNum[i] = VD_time.get(i).size();
			} else {
				System.out.println("VD_time.get(" + i + ").size() != VD_energy.get(" + i + ").size() !");
			}
		}
		mVD_time = VD_time;
		mVD_energy = VD_energy;
		
		generateGraph();
    }
	
	private void generateGraph()
	{
		mi_vertexNum = 1;
		mi_edgeNum = 0;
		for (int i = 0; i < mi_jobNum; i++) {
			mi_vertexNum += mai_maxTaskNum[i] + 1;
			mi_edgeNum += 2 * mai_maxTaskNum[i];
		}
		
		maV_vertices = new Vertex[mi_vertexNum];
		maE_edges = new Edge[mi_edgeNum];
		int vertexIndex = 0;
		int edgeIndex = 0;
		int precVertex = 0;
		int succVertex = 0;
		maV_vertices[vertexIndex] = new Vertex(vertexIndex, 0, mai_maxTaskNum[0]);
		vertexIndex++;
		for (int i = 0; i < mi_jobNum; i++) {
			precVertex = succVertex;
			succVertex = vertexIndex + mai_maxTaskNum[i];
			
			for (int j = 0; j < mai_maxTaskNum[i]; j++) {
				maV_vertices[vertexIndex + j] = new Vertex(vertexIndex + j, 1, 1);
			}
			
			if (i < mi_jobNum - 1) {
				maV_vertices[vertexIndex + mai_maxTaskNum[i]] = new Vertex(vertexIndex + mai_maxTaskNum[i], mai_maxTaskNum[i], mai_maxTaskNum[i+1]);
			} else {
				maV_vertices[vertexIndex + mai_maxTaskNum[i]] = new Vertex(vertexIndex + mai_maxTaskNum[i], mai_maxTaskNum[i], 0);
			}
		
			for (int j = 0; j < mai_maxTaskNum[i]; j++) {
				maV_vertices[precVertex].mai_succEdges[j] = edgeIndex;
				maE_edges[edgeIndex] = new Edge(edgeIndex, precVertex, vertexIndex, mVD_time.get(i).get(j), mVD_energy.get(i).get(j));
				maV_vertices[vertexIndex].mai_precEdges[0] = edgeIndex;
				edgeIndex++;
				
				maV_vertices[succVertex].mai_precEdges[j] = edgeIndex;
				maE_edges[edgeIndex] = new Edge(edgeIndex, vertexIndex, succVertex, 0.0, 0.0);
				maV_vertices[vertexIndex].mai_succEdges[0] = edgeIndex;
				edgeIndex++;
				
				vertexIndex++;
			}
			vertexIndex++;
		}
		
		mai_path = new int[mi_jobNum];
		return;
	}
	
	public double minEnergy(double time, double epsilon)
	{
		double lb = getLowerBound();
		double ub = getUpperBound();
		if (lb <= 0.0) {
			System.out.println("Lower bound <= 0.0 !");
		}
		while (ub > 2 * lb) {
			double v = Math.sqrt(lb * ub);
			if (test(v, time, epsilon)) {
				lb = v;
			} else {
				ub = v * (1 + epsilon); 
			}
		}
		double energy = dynProg(lb, time, epsilon);
		return energy;
	}
	
	public int[] getTaskNums() {
		int[] taskNum = new int[mi_jobNum];
		int base = 0;
		for (int i = 0; i < mi_jobNum; i++) {
			taskNum[i] = (mai_path[i] - base) / 2 + 1;
			base += mai_maxTaskNum[i] * 2;
		}
		return taskNum;
	}
	
	private boolean test(double v, double time, double epsilon)
	{
		int costs[] = new int[mi_edgeNum];
		for (int i = 0; i < mi_edgeNum; i++) {
			costs[i] = (int) Math.floor(maE_edges[i].md_cost * mi_jobNum / (v * epsilon));
		}
		
		double infTime = getInfTime();
		Vector<double[]> timeMatrix = new Vector<double[]>();
		int c = 0;
		while (c < mi_jobNum / epsilon) {
			double[] timeVector = new double[mi_vertexNum];
			for (int i = 0; i < mi_vertexNum; i++) {
				timeVector[i] = infTime;
			}
			timeMatrix.add(timeVector);
			timeMatrix.get(c)[0] = 0.0;
			if (c > 0) {
				for (int i = 1; i < mi_vertexNum; i++) {
					double minTime = timeMatrix.get(c-1)[i];
					for (int j = 0; j < maV_vertices[i].mi_precEdgeNum; j++) {
						int edgeIndex = maV_vertices[i].mai_precEdges[j];
						if (costs[edgeIndex] <= c) {
							double t = timeMatrix.get(c - costs[edgeIndex])[maE_edges[edgeIndex].mi_startVertex]
									+ maE_edges[edgeIndex].md_weight;
							if (minTime > t) {
								minTime = t;
							}
						}
					}
					timeMatrix.get(c)[i] = minTime;
				}
			}
			if (timeMatrix.get(c)[mi_vertexNum - 1] <= time)
				return false;
			else
				c++;
		}
		return true;
	}
	
	private double dynProg(double lb, double time, double epsilon)
	{
		int costs[] = new int[mi_edgeNum];
		for (int i = 0; i < mi_edgeNum; i++) {
			costs[i] = (int) Math.floor(maE_edges[i].md_cost * mi_jobNum / (lb * epsilon));
		}
		
		double infTime = getInfTime();
		Vector<double[]> timeMatrix = new Vector<double[]>();
		Vector<int[]> precMatrix = new Vector<int[]>();
		int c = 0;
		do {
			double[] timeVector = new double[mi_vertexNum];
			for (int i = 0; i < mi_vertexNum; i++) {
				timeVector[i] = infTime;
			}
			timeMatrix.add(timeVector);
			timeMatrix.get(c)[0] = 0.0;
			int[] precVector = new int[mi_vertexNum];
			for (int i = 0; i < mi_vertexNum; i++) {
				precVector[i] = -1;
			}
			precMatrix.add(precVector);
			if (c > 0) {
				for (int i = 1; i < mi_vertexNum; i++) {
					double minTime = timeMatrix.get(c-1)[i];
					for (int j = 0; j < maV_vertices[i].mi_precEdgeNum; j++) {
						int edgeIndex = maV_vertices[i].mai_precEdges[j];
						if (costs[edgeIndex] <= c) {
							double t = timeMatrix.get(c - costs[edgeIndex])[maE_edges[edgeIndex].mi_startVertex]
									+ maE_edges[edgeIndex].md_weight;
							if (minTime > t) {
								minTime = t;
								precMatrix.get(c)[i] = edgeIndex;
							}
						}
					}
					timeMatrix.get(c)[i] = minTime;
				}
			}
			c++;
		} while (timeMatrix.get(c - 1)[mi_vertexNum - 1] > time);
		
		int count = mi_jobNum;
		int vertexIndex = mi_vertexNum - 1;
		c--;
		while (precMatrix.get(c)[vertexIndex] > -1) {
			count--;
			if (count >= 0) {
				mai_path[count] = maV_vertices[maE_edges[precMatrix.get(c)[vertexIndex]].mi_startVertex].mai_precEdges[0];
				c -= costs[mai_path[count]];
				vertexIndex = maE_edges[mai_path[count]].mi_startVertex;
			} else {
				System.out.println("count = " + count + " !");
			}
		}
		
		double minCost = 0.0;
		for (int i = 0; i < mi_jobNum; i++) {
			minCost = minCost + maE_edges[mai_path[i]].md_cost;
		}
		
		return minCost;
	}
	
	private double getUpperBound()
	{
		double maxTotalCost = 0.0;
		int edgeIndex = 0;
		for (int i = 0; i < mi_jobNum; i++) {
			double maxCost = 0.0;
			for (int j = 0; j < mai_maxTaskNum[i]; j++) {
				if (maxCost < maE_edges[edgeIndex].md_cost) {
					maxCost = maE_edges[edgeIndex].md_cost;
				}
				edgeIndex += 2;
			}
			maxTotalCost += maxCost;
		}
		return maxTotalCost;
	}
	
	private double getLowerBound()
	{
		double minTotalCost = 0.0;
		int edgeIndex = 0;
		for (int i = 0; i < mi_jobNum; i++) {
			double minCost = Double.MAX_VALUE;
			for (int j = 0; j < mai_maxTaskNum[i]; j++) {
				if (minCost > maE_edges[edgeIndex].md_cost) {
					minCost = maE_edges[edgeIndex].md_cost;
				}
				edgeIndex += 2;
			}
			minTotalCost += minCost;
		}
		return minTotalCost;
	}
	
	private double getInfTime()
	{
		double maxWeight = 0.0;
		for (int i = 0; i < mi_edgeNum; i++) {
			if (maxWeight < maE_edges[i].md_weight) {
				maxWeight = maE_edges[i].md_weight;
			}
		}
		return maxWeight * mi_edgeNum;
	}

	/*
	public static void main(String args[]) {
		Vector<String> jobName = new Vector<String>();
		Vector<Vector<String>> option = new Vector<Vector<String>>();
		Vector<Vector<Double>> time = new Vector<Vector<Double>>();
		Vector<Vector<Double>> energy = new Vector<Vector<Double>>();
		int jobNum = 15;
		for (int i = 0; i < jobNum; i++) {
			option.add(new Vector<String>());
			time.add(new Vector<Double>());
			energy.add(new Vector<Double>());
		}
		
		jobName.add(new String("App2Map"));
		option.get(0).add(new String("2359296000"));
		option.get(0).add(new String("1191182336"));
		option.get(0).add(new String("1084227584"));
		option.get(0).add(new String("598736896"));
		option.get(0).add(new String("521142272"));
		time.get(0).add(new Double(102.1));
		time.get(0).add(new Double(84.7));
		time.get(0).add(new Double(81.7));
		time.get(0).add(new Double(54.3));
		time.get(0).add(new Double(53.5));
		energy.get(0).add(new Double(3066.0));
		energy.get(0).add(new Double(3212.0));
		energy.get(0).add(new Double(3357.0));
		energy.get(0).add(new Double(3547.0));
		energy.get(0).add(new Double(3671.0));
		
		jobName.add(new String("App2Red"));
		option.get(1).add(new String("1"));
		option.get(1).add(new String("2"));
		option.get(1).add(new String("4"));
		option.get(1).add(new String("5"));
		option.get(1).add(new String("6"));
		option.get(1).add(new String("8"));
		time.get(1).add(new Double(38.9));
		time.get(1).add(new Double(24.3));
		time.get(1).add(new Double(20.5));
		time.get(1).add(new Double(18.7));
		time.get(1).add(new Double(17.2));
		time.get(1).add(new Double(14.9));
		energy.get(1).add(new Double(636.0));
		energy.get(1).add(new Double(657.0));
		energy.get(1).add(new Double(695.0));
		energy.get(1).add(new Double(713.0));
		energy.get(1).add(new Double(753.0));
		energy.get(1).add(new Double(785.0));
		
		jobName.add(new String("App3"));
		option.get(2).add(new String("2359296000"));
		option.get(2).add(new String("1191182336"));
		option.get(2).add(new String("1084227584"));
		option.get(2).add(new String("598736896"));
		option.get(2).add(new String("521142272"));
		time.get(2).add(new Double(65.1));
		time.get(2).add(new Double(61.1));
		time.get(2).add(new Double(60.0));
		time.get(2).add(new Double(44.2));
		time.get(2).add(new Double(44.0));
		energy.get(2).add(new Double(2125.0));
		energy.get(2).add(new Double(2298.0));
		energy.get(2).add(new Double(2430.0));
		energy.get(2).add(new Double(2651.0));
		energy.get(2).add(new Double(2728.0));
		
		jobName.add(new String("App1Map"));
		option.get(3).add(new String("2359296000"));
		option.get(3).add(new String("1191182336"));
		option.get(3).add(new String("1084227584"));
		option.get(3).add(new String("598736896"));
		option.get(3).add(new String("521142272"));
		time.get(3).add(new Double(79.9));
		time.get(3).add(new Double(70.0));
		time.get(3).add(new Double(68.9));
		time.get(3).add(new Double(50.3));
		time.get(3).add(new Double(48.1));
		energy.get(3).add(new Double(2770.0));
		energy.get(3).add(new Double(2877.0));
		energy.get(3).add(new Double(3014.0));
		energy.get(3).add(new Double(3256.0));
		energy.get(3).add(new Double(3304.0));
		
		jobName.add(new String("App1Red"));
		option.get(4).add(new String("1"));
		option.get(4).add(new String("2"));
		option.get(4).add(new String("4"));
		option.get(4).add(new String("5"));
		option.get(4).add(new String("6"));
		option.get(4).add(new String("7"));
		time.get(4).add(new Double(60.9));
		time.get(4).add(new Double(42.0));
		time.get(4).add(new Double(34.7));
		time.get(4).add(new Double(30.9));
		time.get(4).add(new Double(26.4));
		time.get(4).add(new Double(21.4));
		energy.get(4).add(new Double(726.0));
		energy.get(4).add(new Double(780.0));
		energy.get(4).add(new Double(792.0));
		energy.get(4).add(new Double(816.0));
		energy.get(4).add(new Double(838.0));
		energy.get(4).add(new Double(849.0));
		
		jobName.add(new String("App3"));
		option.get(5).add(new String("2359296000"));
		option.get(5).add(new String("1191182336"));
		option.get(5).add(new String("1084227584"));
		option.get(5).add(new String("598736896"));
		option.get(5).add(new String("521142272"));
		time.get(5).add(new Double(65.1));
		time.get(5).add(new Double(61.1));
		time.get(5).add(new Double(60.0));
		time.get(5).add(new Double(44.2));
		time.get(5).add(new Double(44.0));
		energy.get(5).add(new Double(2125.0));
		energy.get(5).add(new Double(2298.0));
		energy.get(5).add(new Double(2430.0));
		energy.get(5).add(new Double(2651.0));
		energy.get(5).add(new Double(2728.0));
		
		jobName.add(new String("App2Map"));
		option.get(6).add(new String("2359296000"));
		option.get(6).add(new String("1191182336"));
		option.get(6).add(new String("1084227584"));
		option.get(6).add(new String("598736896"));
		option.get(6).add(new String("521142272"));
		time.get(6).add(new Double(102.1));
		time.get(6).add(new Double(84.7));
		time.get(6).add(new Double(81.7));
		time.get(6).add(new Double(54.3));
		time.get(6).add(new Double(53.5));
		energy.get(6).add(new Double(3066.0));
		energy.get(6).add(new Double(3212.0));
		energy.get(6).add(new Double(3357.0));
		energy.get(6).add(new Double(3547.0));
		energy.get(6).add(new Double(3671.0));
		
		jobName.add(new String("App2Red"));
		option.get(7).add(new String("1"));
		option.get(7).add(new String("2"));
		option.get(7).add(new String("4"));
		option.get(7).add(new String("5"));
		option.get(7).add(new String("6"));
		option.get(7).add(new String("8"));
		time.get(7).add(new Double(38.9));
		time.get(7).add(new Double(24.3));
		time.get(7).add(new Double(20.5));
		time.get(7).add(new Double(18.7));
		time.get(7).add(new Double(17.2));
		time.get(7).add(new Double(14.9));
		energy.get(7).add(new Double(636.0));
		energy.get(7).add(new Double(657.0));
		energy.get(7).add(new Double(695.0));
		energy.get(7).add(new Double(713.0));
		energy.get(7).add(new Double(753.0));
		energy.get(7).add(new Double(785.0));
		
		jobName.add(new String("App3"));
		option.get(8).add(new String("2359296000"));
		option.get(8).add(new String("1191182336"));
		option.get(8).add(new String("1084227584"));
		option.get(8).add(new String("598736896"));
		option.get(8).add(new String("521142272"));
		time.get(8).add(new Double(65.1));
		time.get(8).add(new Double(61.1));
		time.get(8).add(new Double(60.0));
		time.get(8).add(new Double(44.2));
		time.get(8).add(new Double(44.0));
		energy.get(8).add(new Double(2125.0));
		energy.get(8).add(new Double(2298.0));
		energy.get(8).add(new Double(2430.0));
		energy.get(8).add(new Double(2651.0));
		energy.get(8).add(new Double(2728.0));
		
		jobName.add(new String("App1Map"));
		option.get(9).add(new String("2359296000"));
		option.get(9).add(new String("1191182336"));
		option.get(9).add(new String("1084227584"));
		option.get(9).add(new String("598736896"));
		option.get(9).add(new String("521142272"));
		time.get(9).add(new Double(79.9));
		time.get(9).add(new Double(70.0));
		time.get(9).add(new Double(68.9));
		time.get(9).add(new Double(50.3));
		time.get(9).add(new Double(48.1));
		energy.get(9).add(new Double(2770.0));
		energy.get(9).add(new Double(2877.0));
		energy.get(9).add(new Double(3014.0));
		energy.get(9).add(new Double(3256.0));
		energy.get(9).add(new Double(3304.0));
		
		jobName.add(new String("App1Red"));
		option.get(10).add(new String("1"));
		option.get(10).add(new String("2"));
		option.get(10).add(new String("4"));
		option.get(10).add(new String("5"));
		option.get(10).add(new String("6"));
		option.get(10).add(new String("7"));
		time.get(10).add(new Double(60.9));
		time.get(10).add(new Double(42.0));
		time.get(10).add(new Double(34.7));
		time.get(10).add(new Double(30.9));
		time.get(10).add(new Double(26.4));
		time.get(10).add(new Double(21.4));
		energy.get(10).add(new Double(726.0));
		energy.get(10).add(new Double(780.0));
		energy.get(10).add(new Double(792.0));
		energy.get(10).add(new Double(816.0));
		energy.get(10).add(new Double(838.0));
		energy.get(10).add(new Double(849.0));
		
		jobName.add(new String("App3"));
		option.get(11).add(new String("2359296000"));
		option.get(11).add(new String("1191182336"));
		option.get(11).add(new String("1084227584"));
		option.get(11).add(new String("598736896"));
		option.get(11).add(new String("521142272"));
		time.get(11).add(new Double(65.1));
		time.get(11).add(new Double(61.1));
		time.get(11).add(new Double(60.0));
		time.get(11).add(new Double(44.2));
		time.get(11).add(new Double(44.0));
		energy.get(11).add(new Double(2125.0));
		energy.get(11).add(new Double(2298.0));
		energy.get(11).add(new Double(2430.0));
		energy.get(11).add(new Double(2651.0));
		energy.get(11).add(new Double(2728.0));
		
		jobName.add(new String("App2Map"));
		option.get(12).add(new String("2359296000"));
		option.get(12).add(new String("1191182336"));
		option.get(12).add(new String("1084227584"));
		option.get(12).add(new String("598736896"));
		option.get(12).add(new String("521142272"));
		time.get(12).add(new Double(102.1));
		time.get(12).add(new Double(84.7));
		time.get(12).add(new Double(81.7));
		time.get(12).add(new Double(54.3));
		time.get(12).add(new Double(53.5));
		energy.get(12).add(new Double(3066.0));
		energy.get(12).add(new Double(3212.0));
		energy.get(12).add(new Double(3357.0));
		energy.get(12).add(new Double(3547.0));
		energy.get(12).add(new Double(3671.0));
		
		jobName.add(new String("App2Red"));
		option.get(13).add(new String("1"));
		option.get(13).add(new String("2"));
		option.get(13).add(new String("4"));
		option.get(13).add(new String("5"));
		option.get(13).add(new String("6"));
		option.get(13).add(new String("8"));
		time.get(13).add(new Double(38.9));
		time.get(13).add(new Double(24.3));
		time.get(13).add(new Double(20.5));
		time.get(13).add(new Double(18.7));
		time.get(13).add(new Double(17.2));
		time.get(13).add(new Double(14.9));
		energy.get(13).add(new Double(636.0));
		energy.get(13).add(new Double(657.0));
		energy.get(13).add(new Double(695.0));
		energy.get(13).add(new Double(713.0));
		energy.get(13).add(new Double(753.0));
		energy.get(13).add(new Double(785.0));
		
		jobName.add(new String("App3"));
		option.get(14).add(new String("2359296000"));
		option.get(14).add(new String("1191182336"));
		option.get(14).add(new String("1084227584"));
		option.get(14).add(new String("598736896"));
		option.get(14).add(new String("521142272"));
		time.get(14).add(new Double(65.1));
		time.get(14).add(new Double(61.1));
		time.get(14).add(new Double(60.0));
		time.get(14).add(new Double(44.2));
		time.get(14).add(new Double(44.0));
		energy.get(14).add(new Double(2125.0));
		energy.get(14).add(new Double(2298.0));
		energy.get(14).add(new Double(2430.0));
		energy.get(14).add(new Double(2651.0));
		energy.get(14).add(new Double(2728.0));
		
		RestrictedShortestPath rsp = new RestrictedShortestPath(jobNum, time, energy);
		double step = (1030.1 - 564.2) / 10;
		for (double deadline = 587.5; deadline < 1030.1; deadline += step) {
			System.out.println("\ndeadline\t" + deadline);
			double minEnergy = rsp.minEnergy(deadline, 0.001);	// 0.000025
			int[] taskNum = rsp.getTaskNums();
			double compTime = 0.0;
			for (int i = 0; i < rsp.mi_jobNum; i++) {
				compTime += time.get(i).get(taskNum[i]-1).doubleValue();
			}
			System.out.println("compTime\t" + compTime);
			System.out.println("DEC\t" + minEnergy);
			for (int i = 0; i < rsp.mi_jobNum; i++) {
				System.out.println(jobName.get(i) + "\t" + option.get(i).get(taskNum[i]-1));
			}
		}
	}
	
	public static void main(String args[]) {
		Vector<String> jobName = new Vector<String>();
		Vector<Vector<String>> option = new Vector<Vector<String>>();
		Vector<Vector<Double>> time = new Vector<Vector<Double>>();
		Vector<Vector<Double>> energy = new Vector<Vector<Double>>();
		int jobNum = 18;
		for (int i = 0; i < jobNum; i++) {
			option.add(new Vector<String>());
			time.add(new Vector<Double>());
			energy.add(new Vector<Double>());
		}
		
		jobName.add(new String("App1Map"));
		option.get(0).add(new String("2359296000"));
		option.get(0).add(new String("1191182336"));
		option.get(0).add(new String("1084227584"));
		option.get(0).add(new String("598736896"));
		option.get(0).add(new String("521142272"));
		time.get(0).add(new Double(79.9));
		time.get(0).add(new Double(70.0));
		time.get(0).add(new Double(68.9));
		time.get(0).add(new Double(50.3));
		time.get(0).add(new Double(48.1));
		energy.get(0).add(new Double(2770.0));
		energy.get(0).add(new Double(2877.0));
		energy.get(0).add(new Double(3014.0));
		energy.get(0).add(new Double(3256.0));
		energy.get(0).add(new Double(3304.0));
		
		jobName.add(new String("App1Red"));
		option.get(1).add(new String("1"));
		option.get(1).add(new String("2"));
		option.get(1).add(new String("4"));
		option.get(1).add(new String("5"));
		option.get(1).add(new String("6"));
		option.get(1).add(new String("7"));
		time.get(1).add(new Double(60.9));
		time.get(1).add(new Double(42.0));
		time.get(1).add(new Double(34.7));
		time.get(1).add(new Double(30.9));
		time.get(1).add(new Double(26.4));
		time.get(1).add(new Double(21.4));
		energy.get(1).add(new Double(726.0));
		energy.get(1).add(new Double(780.0));
		energy.get(1).add(new Double(792.0));
		energy.get(1).add(new Double(816.0));
		energy.get(1).add(new Double(838.0));
		energy.get(1).add(new Double(849.0));
		
		jobName.add(new String("App2Map"));
		option.get(2).add(new String("2359296000"));
		option.get(2).add(new String("1191182336"));
		option.get(2).add(new String("1084227584"));
		option.get(2).add(new String("598736896"));
		option.get(2).add(new String("521142272"));
		time.get(2).add(new Double(102.1));
		time.get(2).add(new Double(84.7));
		time.get(2).add(new Double(81.7));
		time.get(2).add(new Double(54.3));
		time.get(2).add(new Double(53.5));
		energy.get(2).add(new Double(3066.0));
		energy.get(2).add(new Double(3212.0));
		energy.get(2).add(new Double(3357.0));
		energy.get(2).add(new Double(3547.0));
		energy.get(2).add(new Double(3671.0));
		
		jobName.add(new String("App2Red"));
		option.get(3).add(new String("1"));
		option.get(3).add(new String("2"));
		option.get(3).add(new String("4"));
		option.get(3).add(new String("5"));
		option.get(3).add(new String("6"));
		option.get(3).add(new String("8"));
		time.get(3).add(new Double(38.9));
		time.get(3).add(new Double(24.3));
		time.get(3).add(new Double(20.5));
		time.get(3).add(new Double(18.7));
		time.get(3).add(new Double(17.2));
		time.get(3).add(new Double(14.9));
		energy.get(3).add(new Double(636.0));
		energy.get(3).add(new Double(657.0));
		energy.get(3).add(new Double(695.0));
		energy.get(3).add(new Double(713.0));
		energy.get(3).add(new Double(753.0));
		energy.get(3).add(new Double(785.0));
		
		jobName.add(new String("App3"));
		option.get(4).add(new String("2359296000"));
		option.get(4).add(new String("1191182336"));
		option.get(4).add(new String("1084227584"));
		option.get(4).add(new String("598736896"));
		option.get(4).add(new String("521142272"));
		time.get(4).add(new Double(65.1));
		time.get(4).add(new Double(61.1));
		time.get(4).add(new Double(60.0));
		time.get(4).add(new Double(44.2));
		time.get(4).add(new Double(44.0));
		energy.get(4).add(new Double(2125.0));
		energy.get(4).add(new Double(2298.0));
		energy.get(4).add(new Double(2430.0));
		energy.get(4).add(new Double(2651.0));
		energy.get(4).add(new Double(2728.0));
		
		jobName.add(new String("App1Map"));
		option.get(5).add(new String("2359296000"));
		option.get(5).add(new String("1191182336"));
		option.get(5).add(new String("1084227584"));
		option.get(5).add(new String("598736896"));
		option.get(5).add(new String("521142272"));
		time.get(5).add(new Double(79.9));
		time.get(5).add(new Double(70.0));
		time.get(5).add(new Double(68.9));
		time.get(5).add(new Double(50.3));
		time.get(5).add(new Double(48.1));
		energy.get(5).add(new Double(2770.0));
		energy.get(5).add(new Double(2877.0));
		energy.get(5).add(new Double(3014.0));
		energy.get(5).add(new Double(3256.0));
		energy.get(5).add(new Double(3304.0));
		
		jobName.add(new String("App1Red"));
		option.get(6).add(new String("1"));
		option.get(6).add(new String("2"));
		option.get(6).add(new String("4"));
		option.get(6).add(new String("5"));
		option.get(6).add(new String("6"));
		option.get(6).add(new String("7"));
		time.get(6).add(new Double(60.9));
		time.get(6).add(new Double(42.0));
		time.get(6).add(new Double(34.7));
		time.get(6).add(new Double(30.9));
		time.get(6).add(new Double(26.4));
		time.get(6).add(new Double(21.4));
		energy.get(6).add(new Double(726.0));
		energy.get(6).add(new Double(780.0));
		energy.get(6).add(new Double(792.0));
		energy.get(6).add(new Double(816.0));
		energy.get(6).add(new Double(838.0));
		energy.get(6).add(new Double(849.0));
		
		jobName.add(new String("App2Map"));
		option.get(7).add(new String("2359296000"));
		option.get(7).add(new String("1191182336"));
		option.get(7).add(new String("1084227584"));
		option.get(7).add(new String("598736896"));
		option.get(7).add(new String("521142272"));
		time.get(7).add(new Double(102.1));
		time.get(7).add(new Double(84.7));
		time.get(7).add(new Double(81.7));
		time.get(7).add(new Double(54.3));
		time.get(7).add(new Double(53.5));
		energy.get(7).add(new Double(3066.0));
		energy.get(7).add(new Double(3212.0));
		energy.get(7).add(new Double(3357.0));
		energy.get(7).add(new Double(3547.0));
		energy.get(7).add(new Double(3671.0));
		
		jobName.add(new String("App2Red"));
		option.get(8).add(new String("1"));
		option.get(8).add(new String("2"));
		option.get(8).add(new String("4"));
		option.get(8).add(new String("5"));
		option.get(8).add(new String("6"));
		option.get(8).add(new String("8"));
		time.get(8).add(new Double(38.9));
		time.get(8).add(new Double(24.3));
		time.get(8).add(new Double(20.5));
		time.get(8).add(new Double(18.7));
		time.get(8).add(new Double(17.2));
		time.get(8).add(new Double(14.9));
		energy.get(8).add(new Double(636.0));
		energy.get(8).add(new Double(657.0));
		energy.get(8).add(new Double(695.0));
		energy.get(8).add(new Double(713.0));
		energy.get(8).add(new Double(753.0));
		energy.get(8).add(new Double(785.0));
		
		jobName.add(new String("App1Map"));
		option.get(9).add(new String("2359296000"));
		option.get(9).add(new String("1191182336"));
		option.get(9).add(new String("1084227584"));
		option.get(9).add(new String("598736896"));
		option.get(9).add(new String("521142272"));
		time.get(9).add(new Double(79.9));
		time.get(9).add(new Double(70.0));
		time.get(9).add(new Double(68.9));
		time.get(9).add(new Double(50.3));
		time.get(9).add(new Double(48.1));
		energy.get(9).add(new Double(2770.0));
		energy.get(9).add(new Double(2877.0));
		energy.get(9).add(new Double(3014.0));
		energy.get(9).add(new Double(3256.0));
		energy.get(9).add(new Double(3304.0));
		
		jobName.add(new String("App1Red"));
		option.get(10).add(new String("1"));
		option.get(10).add(new String("2"));
		option.get(10).add(new String("4"));
		option.get(10).add(new String("5"));
		option.get(10).add(new String("6"));
		option.get(10).add(new String("7"));
		time.get(10).add(new Double(60.9));
		time.get(10).add(new Double(42.0));
		time.get(10).add(new Double(34.7));
		time.get(10).add(new Double(30.9));
		time.get(10).add(new Double(26.4));
		time.get(10).add(new Double(21.4));
		energy.get(10).add(new Double(726.0));
		energy.get(10).add(new Double(780.0));
		energy.get(10).add(new Double(792.0));
		energy.get(10).add(new Double(816.0));
		energy.get(10).add(new Double(838.0));
		energy.get(10).add(new Double(849.0));
		
		jobName.add(new String("App2Map"));
		option.get(11).add(new String("2359296000"));
		option.get(11).add(new String("1191182336"));
		option.get(11).add(new String("1084227584"));
		option.get(11).add(new String("598736896"));
		option.get(11).add(new String("521142272"));
		time.get(11).add(new Double(102.1));
		time.get(11).add(new Double(84.7));
		time.get(11).add(new Double(81.7));
		time.get(11).add(new Double(54.3));
		time.get(11).add(new Double(53.5));
		energy.get(11).add(new Double(3066.0));
		energy.get(11).add(new Double(3212.0));
		energy.get(11).add(new Double(3357.0));
		energy.get(11).add(new Double(3547.0));
		energy.get(11).add(new Double(3671.0));
		
		jobName.add(new String("App2Red"));
		option.get(12).add(new String("1"));
		option.get(12).add(new String("2"));
		option.get(12).add(new String("4"));
		option.get(12).add(new String("5"));
		option.get(12).add(new String("6"));
		option.get(12).add(new String("8"));
		time.get(12).add(new Double(38.9));
		time.get(12).add(new Double(24.3));
		time.get(12).add(new Double(20.5));
		time.get(12).add(new Double(18.7));
		time.get(12).add(new Double(17.2));
		time.get(12).add(new Double(14.9));
		energy.get(12).add(new Double(636.0));
		energy.get(12).add(new Double(657.0));
		energy.get(12).add(new Double(695.0));
		energy.get(12).add(new Double(713.0));
		energy.get(12).add(new Double(753.0));
		energy.get(12).add(new Double(785.0));
		
		jobName.add(new String("App3"));
		option.get(13).add(new String("2359296000"));
		option.get(13).add(new String("1191182336"));
		option.get(13).add(new String("1084227584"));
		option.get(13).add(new String("598736896"));
		option.get(13).add(new String("521142272"));
		time.get(13).add(new Double(65.1));
		time.get(13).add(new Double(61.1));
		time.get(13).add(new Double(60.0));
		time.get(13).add(new Double(44.2));
		time.get(13).add(new Double(44.0));
		energy.get(13).add(new Double(2125.0));
		energy.get(13).add(new Double(2298.0));
		energy.get(13).add(new Double(2430.0));
		energy.get(13).add(new Double(2651.0));
		energy.get(13).add(new Double(2728.0));
		
		jobName.add(new String("App1Map"));
		option.get(14).add(new String("2359296000"));
		option.get(14).add(new String("1191182336"));
		option.get(14).add(new String("1084227584"));
		option.get(14).add(new String("598736896"));
		option.get(14).add(new String("521142272"));
		time.get(14).add(new Double(79.9));
		time.get(14).add(new Double(70.0));
		time.get(14).add(new Double(68.9));
		time.get(14).add(new Double(50.3));
		time.get(14).add(new Double(48.1));
		energy.get(14).add(new Double(2770.0));
		energy.get(14).add(new Double(2877.0));
		energy.get(14).add(new Double(3014.0));
		energy.get(14).add(new Double(3256.0));
		energy.get(14).add(new Double(3304.0));
		
		jobName.add(new String("App1Red"));
		option.get(15).add(new String("1"));
		option.get(15).add(new String("2"));
		option.get(15).add(new String("4"));
		option.get(15).add(new String("5"));
		option.get(15).add(new String("6"));
		option.get(15).add(new String("7"));
		time.get(15).add(new Double(60.9));
		time.get(15).add(new Double(42.0));
		time.get(15).add(new Double(34.7));
		time.get(15).add(new Double(30.9));
		time.get(15).add(new Double(26.4));
		time.get(15).add(new Double(21.4));
		energy.get(15).add(new Double(726.0));
		energy.get(15).add(new Double(780.0));
		energy.get(15).add(new Double(792.0));
		energy.get(15).add(new Double(816.0));
		energy.get(15).add(new Double(838.0));
		energy.get(15).add(new Double(849.0));
		
		jobName.add(new String("App2Map"));
		option.get(16).add(new String("2359296000"));
		option.get(16).add(new String("1191182336"));
		option.get(16).add(new String("1084227584"));
		option.get(16).add(new String("598736896"));
		option.get(16).add(new String("521142272"));
		time.get(16).add(new Double(102.1));
		time.get(16).add(new Double(84.7));
		time.get(16).add(new Double(81.7));
		time.get(16).add(new Double(54.3));
		time.get(16).add(new Double(53.5));
		energy.get(16).add(new Double(3066.0));
		energy.get(16).add(new Double(3212.0));
		energy.get(16).add(new Double(3357.0));
		energy.get(16).add(new Double(3547.0));
		energy.get(16).add(new Double(3671.0));
		
		jobName.add(new String("App2Red"));
		option.get(17).add(new String("1"));
		option.get(17).add(new String("2"));
		option.get(17).add(new String("4"));
		option.get(17).add(new String("5"));
		option.get(17).add(new String("6"));
		option.get(17).add(new String("8"));
		time.get(17).add(new Double(38.9));
		time.get(17).add(new Double(24.3));
		time.get(17).add(new Double(20.5));
		time.get(17).add(new Double(18.7));
		time.get(17).add(new Double(17.2));
		time.get(17).add(new Double(14.9));
		energy.get(17).add(new Double(636.0));
		energy.get(17).add(new Double(657.0));
		energy.get(17).add(new Double(695.0));
		energy.get(17).add(new Double(713.0));
		energy.get(17).add(new Double(753.0));
		energy.get(17).add(new Double(785.0));
		
		RestrictedShortestPath rsp = new RestrictedShortestPath(jobNum, time, energy);
		double step = (1257.4 - 639.6) / 10;
		for (double deadline = 670.5; deadline < 1257.4; deadline += step) {
			System.out.println("\ndeadline\t" + deadline);
			double minEnergy = rsp.minEnergy(deadline, 0.002);	// 0.000025
			int[] taskNum = rsp.getTaskNums();
			double compTime = 0.0;
			for (int i = 0; i < rsp.mi_jobNum; i++) {
				compTime += time.get(i).get(taskNum[i]-1).doubleValue();
			}
			System.out.println("compTime\t" + compTime);
			System.out.println("DEC\t" + minEnergy);
			for (int i = 0; i < rsp.mi_jobNum; i++) {
				System.out.println(jobName.get(i) + "\t" + option.get(i).get(taskNum[i]-1));
			}
		}
	}
	*/
}
