package cs5300Project2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Node {
	
	int blockId;
	int nodeId;
	
	double prevPageRank;
	double currPageRank;
	
	/**
	 * [&lt;blockId, nodeId&gt;, ...]
	 */
	List<Pair<Integer, Integer>> adjacencyList;
	
	public Node (int bid, int nid, double ppr, double cpr) {
		blockId = bid;
		nodeId = nid;
		
		prevPageRank = ppr;
		currPageRank = cpr;
		
		adjacencyList = new ArrayList<Pair<Integer,Integer>>();
	}
	
	//blockId,nodeId,toBlockId1:toNodeId1;toBlockId2:toNodeId2,prevPageRank,currPageRank
	public Node (String s) {
		String[] split = s.split(",");
		
		blockId = Integer.parseInt(split[0]);
		nodeId = Integer.parseInt(split[1]);
		
		adjacencyList = new ArrayList<Pair<Integer, Integer>>();
		//if we have outgoing edges...
		if (split[2].length() != 0) {
			String[] edges = split[2].split(";");
			for (int i = 0; i < edges.length; i++) {
				String[] parts = edges[i].split(":");
				int blockId = Integer.parseInt(parts[0]);
				int nodeId = Integer.parseInt(parts[1]);
				Pair<Integer, Integer> p = 
					new Pair<Integer, Integer>(blockId, nodeId);
				
				adjacencyList.add(p);
			}
		}
		
		prevPageRank = Double.parseDouble(split[3]);
		currPageRank = Double.parseDouble(split[4]);		
	}
	
	public void addAdjacentNode (Pair<Integer, Integer> node) {
		adjacencyList.add(node);
	}
	
	public List<Pair<Integer,Integer>> getAdjacencyList () {
		return adjacencyList;
	}
	
	public int nodeId () {
		return nodeId;
	}
	
	public void setBlockId (int bid) {
		blockId = bid;
	}
	
	public int blockId () {
		return blockId;
	}
	
	public void setPrevPageRank (double ppr) {
		prevPageRank = ppr;
	}
	
	public void setCurrPageRank (double cpr) {
		currPageRank = cpr;
	}
	
	public double getPrevPageRank () {
		return prevPageRank;
	}
	
	public double getCurrPageRank () {
		return currPageRank;
	}
	
	public String toString () {
		StringBuilder sb = new StringBuilder();
		
		sb.append(blockId).append(",");
		sb.append(nodeId).append(",");
		
		Iterator<Pair<Integer,Integer>> iter = adjacencyList.iterator();
		while (iter.hasNext()) {
			Pair<Integer,Integer> node = iter.next();
			sb.append(node.left()).append(":").append(node.right());
			if (iter.hasNext())
				sb.append(";");
		}
		sb.append(",");
		
		sb.append(prevPageRank).append(",");
		sb.append(currPageRank);
		
		return sb.toString();
	}

}
