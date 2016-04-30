package cs5300Project2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BlockedReducer extends Reducer<LongWritable, Text, Text, Text> {
	
	private static final double ALPHA = 0.85;
	
	Text outKey = new Text();
	Text outValue = new Text();
	
	public void reduce(LongWritable _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/**
		 * Local graph comprised of nodes of this block
		 */
		Graph graph = new Graph();
		
		/**
		 * Boundary conditions, consisting of: <br>
		 * &lt;nodeId, pageRank&gt;
		 */
		Map<Integer,Double> boundaryConditions = new HashMap<Integer,Double>();
		
		for (Text t : values) {
			String text = t.toString();
			String contents = text.substring(1);
			char type = text.charAt(0);
			
			if (type == 'B') {
				String[] split = contents.split(",");
				Integer nodeId = new Integer(split[0]);
				Double pageRank = new Double(split[1]);
				
				boundaryConditions.put(nodeId, pageRank);
			} else if (type == 'N') {
				graph.addNode(new Node(contents));
			} else {
				throw new IllegalArgumentException
					("Illegal reducer input value: starts with '" + t.charAt(0)
					+ "', must start with 'B' or 'N'");
			}
		}
		
		//we've constructed the block, now let's do some iteration! WOOOOOOOO
		
		//get ready to compute next page rank iteration
		for (Node n : graph) {
			n.setPrevPageRank(n.getCurrPageRank());
			n.setCurrPageRank(0);
		}
		
		//do local iteration
		for (Node n : graph) {
			double outgoing = n.getPrevPageRank() / n.getAdjacencyList().size();
			for (Pair<Integer,Integer> p : n.getAdjacencyList()) {
				//p consists of <blockId, nodeId> || outgoing edges
				
				//ignore non-local edges! WOOO!
				if (!p.left().equals(n.blockId())) continue;
				double oldPR = graph.getNode(p.right()).getCurrPageRank();
				double newPR = oldPR + outgoing;
				graph.getNode(p.right()).setCurrPageRank(newPR);
			}
		}
		
		//account for boundary conditions
		for (Integer nodeId : boundaryConditions.keySet()) {
			double oldPR = graph.getNode(nodeId).getCurrPageRank();
			double newPR = oldPR + boundaryConditions.get(nodeId);
			graph.getNode(nodeId).setCurrPageRank(newPR);
		}
		
		//apply damping factor
		for (Node n : graph) {
			double oldPR = n.getCurrPageRank();
			double newPR = (ALPHA * oldPR) + ((1 - ALPHA) / graph.size());
			n.setCurrPageRank(newPR);
		}
		
		//emit the results LOL
		for (Node n : graph) {
			outKey.set(String.valueOf(n.nodeId()));
			outValue.set(n.toString());
			context.write(outKey, outValue);
		}
	}
}