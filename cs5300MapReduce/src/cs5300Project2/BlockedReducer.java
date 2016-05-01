package cs5300Project2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
		//Map<Integer,Double> boundaryConditions = new HashMap<Integer,Double>();
		List<Pair<Integer,Double>> boundaryConditions = new ArrayList<Pair<Integer,Double>>();
		
		for (Text t : values) {
			String text = t.toString();
			String contents = text.substring(1);
			char type = text.charAt(0);
			
			if (type == 'B') {
				String[] split = contents.split(",");
				Integer nodeId = new Integer(split[0]);
				Double pageRank = new Double(split[1]);
				
				boundaryConditions.add(new Pair<Integer,Double>(nodeId, pageRank));
			} else if (type == 'N') {
				graph.addNode(new Node(contents));
			} else {
				throw new IllegalArgumentException
					("Illegal reducer input value: starts with '" + text.charAt(0)
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
			double outgoingPR = n.getPrevPageRank() / n.getAdjacencyList().size();
			for (Pair<Integer,Integer> p : n.getAdjacencyList()) {
				//p consists of <blockId, nodeId> || outgoing edges
				Integer outgoingBlockId = p.left();
				Integer outgoingNodeId  = p.right();
				
				//ignore non-local edges! WOOO!
				if (!outgoingBlockId.equals(n.blockId())) continue;
				if (graph.getNode(outgoingNodeId) == null) {
					System.out.println("------> tried to get node '" + outgoingNodeId + "' but only have nodes '" + graph.getNodes().keySet() + "'");
				}
				//deal with local edges only! WOOO!
				double oldPR = graph.getNode(outgoingNodeId).getCurrPageRank();
				double newPR = oldPR + outgoingPR;
				graph.getNode(outgoingNodeId).setCurrPageRank(newPR);
			}
		}
		
		//account for boundary conditions
		for (Pair<Integer,Double> p : boundaryConditions) {
			//p contains <destination nodeId, pageRank>
			Integer nodeId = p.left();
			if (graph.getNode(nodeId) == null) {
				System.out.println("------> in block '" + _key.toString() + "' tried to get node id '" + nodeId + "' for boundary condition but we only have '" + graph.getNodes().keySet() + "'");
			}
			double oldPR = graph.getNode(nodeId).getCurrPageRank();
			double newPR = oldPR + p.right();
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