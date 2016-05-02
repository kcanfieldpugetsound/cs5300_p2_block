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
	
	private int numLines;
	
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
		
			System.out.print("initializing preprocessor");
			Scanner edgeScanner = new Scanner(new File(edge_filename));
			numLines = 0;
			while (edgeScanner.hasNextLine()) {
				if (numLines % 1000000 == 0) System.out.print(".");
				edgeScanner.nextLine();
				numLines++;
			}
			edgeScanner.close();
			System.out.println("done");
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("InputFileCreator initialization failed");
		}
	}
	
	public boolean createInputFile (String outputFilename) {
		long startTime = System.currentTimeMillis();
		System.out.print("[ step 1 of 2 ] creating nodes in memory [ 0% ] complete");
		try {
			
		Graph graph = new Graph();
		
		Scanner edgeScanner = new Scanner(new File(edge_filename));
		PrintWriter writer = new PrintWriter(new File(outputFilename));
		
		//for each edge, add it to the graph
		String line;
		int count = 0;
		int percent = 0;
		int one_percent = numLines / 100;
		while (edgeScanner.hasNextLine()) {
			if (count++ % one_percent == 0) {
				System.out.print("\r[ step 1 of 2 ] creating nodes in memory [ " + percent++ + "% ] complete");
			}
			line = edgeScanner.nextLine().trim();
			
			String[] split = line.split("\\s+");
			
			//ignore a large subset of the edges, in accordance
			//with project specifications
			double filter = Double.parseDouble(split[2]);
			if (filter >= lowerBound && filter <= upperBound) {
				continue;
			}
			
			int fromNId = Integer.parseInt(split[0]);
			int toNId   = Integer.parseInt(split[1]);
			int fromBId = getBlockId(fromNId);
			int toBId   = getBlockId(toNId);
			
			graph.addEdge(fromNId, fromBId, toNId, toBId);
		}
		edgeScanner.close();
		long midTime = System.currentTimeMillis();
		System.out.println("\r[ step 1 of 2 ] creating nodes in memory [ 100% ] completed in "
			+ (midTime - startTime) / 1000 + " seconds");
		
		//page ranks are all 1/N to start
		double initialPageRank = 1.0 / graph.size();
		for (Node n : graph) {
			n.setCurrPageRank(initialPageRank);
			//System.out.println("------> curr page rank for node '" + n.nodeId() + "' is '" + n.getCurrPageRank());
		}
		
		System.out.print("[ step 2 of 2 ] writing nodes to file [ 0% ] complete");
		//write the graph to the file!
		count = 0;
		percent = 0;
		one_percent = graph.getNodes().size() / 100;
		for (Integer nid : graph.getNodes().keySet()) {
			if (count++ % one_percent == 0) {
				System.out.print("\r[ step 2 of 2 ] writing nodes to file [ " + percent++ + "% ] complete");
			}
			Node n = graph.getNodes().get(nid);
			writer.println(n.nodeId() + "\t" + n.toString());
		}
		writer.close();
		System.out.println("\r[ step 2 of 2 ] writing nodes to file [ 100% ] completed in "
			+ (System.currentTimeMillis() - midTime) / 1000 + " seconds");
		
		System.out.println("preprocessing completed in " 
			+ (System.currentTimeMillis() - startTime) / 1000 + " seconds");
		
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
