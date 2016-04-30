package cs5300Project2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Graph implements Iterable<Node>{

	private Map<Integer, Node> nodes;
	
	public Graph () {
		nodes = new HashMap<Integer, Node>();
	}
	
	public void addEdge (int fromNId, int fromBId, int toNId, int toBId) {
		Node n = nodes.get(fromNId);
		if (n == null) {
			//for third and fourth arguments to Node constructor:
			//prevPageRank can stay 0
			//currPageRank is initialized later in InputFileCreator
			n = new Node (fromBId, fromNId, 0, 0);
		}
		
		n.addAdjacentNode(new Pair<Integer,Integer>(toNId, toBId));
		
		nodes.put(n.nodeId(), n);
	}

	public void addNode (Node n) {
		nodes.put(n.nodeId(), n);
	}
	
	public Node getNode (int nid) {
		return nodes.get(nid);
	}
	
	public Map<Integer, Node> getNodes () {
		return nodes;
	}
	
	public int size () {
		return nodes.size();
	}

	@Override
	public Iterator<Node> iterator() {
		return nodes.values().iterator();
	}
}
