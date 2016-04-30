package cs5300Project2.blocked;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BlockedMapper extends Mapper<Text, Text, LongWritable, Text> {
	
	private LongWritable outKey = new LongWritable();
	private Text outValue = new Text();

	public void map(Text ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		
		//ikey   -> nodeid
		//ivalue -> node information, described below
		
		//node information:
		//  b_id,n_id,to_bid_1:to_nid_1;to_bid_2:to_nid_2,prevPR,currPR
		
		Node n = new Node(ivalue.toString());
		
		//write boundary conditions
		for (Pair<Integer,Integer> p : n.getAdjacencyList()) {
			//p consists of (blockID, nodeID)
			
			//if this is an external connection
			if (!p.left().equals(n.blockId())) {
				Double outgoingPR = (n.getCurrPageRank() / n.getAdjacencyList().size());
				String output = "B" + p.right() + "," + outgoingPR;
				outValue.set(output);
				context.write(outKey, outValue);
			} 
			//otherwise we don't need to emit a boundary condition
		}
		
		//write basic node information
		outValue.set("N" + n.toString());
		context.write(outKey, outValue);
		
	}
	
}