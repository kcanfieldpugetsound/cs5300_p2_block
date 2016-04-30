package cs5300Project2;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class InputFileCreator {
	
	
	//blocks.txt
	//edges.txt
	
	List<Integer> blockSizes;
	
	String edge_filename;
	String block_filename;
	
	private double netId = .644;
	private double lowerBound = netId * 0.9;
	private double upperBound = lowerBound + 0.01;
	
	public InputFileCreator (String edgeFilename, String blockFilename) {
		try {
			blockSizes = new ArrayList<Integer>();
			
			edge_filename = edgeFilename;
			block_filename = blockFilename;
			
			blockSizes = new ArrayList<Integer>();
			
			Scanner blockScanner = new Scanner(new File(block_filename));
			
			String line;
			while (blockScanner.hasNextLine()) {
				line = blockScanner.nextLine().trim();
				blockSizes.add(new Integer(line));
			}
			
			blockScanner.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("InputFileCreator initialization failed");
		}
	}
	
	public boolean createInputFile (String outputFilename) {
		System.out.println("creating input file");
		try {
			
		Graph graph = new Graph();
		
		Scanner edgeScanner = new Scanner(new File(edge_filename));
		PrintWriter writer = new PrintWriter(new File(outputFilename));
		
		//for each edge, add it to the graph
		String line;
		while (edgeScanner.hasNextLine()) {
			line = edgeScanner.nextLine().trim();
			
			String[] split = line.split("\\s+");
			
			//ignore a large subset of the edges, in accordance
			//with project specifications
			double filter = Double.parseDouble(split[2]);
			if (filter < lowerBound || filter > upperBound) {
				continue;
			}		
			
			int fromNId = Integer.parseInt(split[0]);
			int toNId   = Integer.parseInt(split[1]);
			int fromBId = getBlockId(fromNId);
			int toBId   = getBlockId(toNId);
			
			graph.addEdge(fromNId, fromBId, toNId, toBId);
		}
		
		//page ranks are all 1/N to start
		double initialPageRank = 1 / graph.size();
		for (Node n : graph) {
			n.setCurrPageRank(initialPageRank);
		}
		
		
		//write the graph to the file!
		for (Integer nid : graph.getNodes().keySet()) {
			writer.println(graph.getNodes().get(nid).toString());
		}
		
		edgeScanner.close();
		writer.close();
		
		System.out.println("created input file");
		
		return true;
		
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("failed to create input file");
			return false;
		}
	}
	
	private int getBlockId (int nodeId) {
		int sum = 0;
		
		for (int i = 0; i < blockSizes.size(); i++) {
			sum += blockSizes.get(i);
			if (nodeId < sum) {
				return i;
			}
		}
		throw new IllegalArgumentException("nodeId " + nodeId + " out of bounds");
	}
	
}
