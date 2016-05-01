package cs5300Project2;

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
			
			Integer outgoingBlockId = p.left();
			Integer outgoingNodeId = p.right();
			
			//if this is a connection external to this block
			if (!outgoingBlockId.equals(n.blockId())) {
				//output boundary conditions
				Double outgoingPR = (n.getCurrPageRank() / n.getAdjacencyList().size());
				String output = "B" + outgoingNodeId + "," + outgoingPR;
				outKey.set(outgoingBlockId);
				outValue.set(output);
				context.write(outKey, outValue);
			} 
			//otherwise we don't need to emit a boundary condition
		}
		
		//write node information		
		outKey.set(n.blockId());
		outValue.set("N" + n.toString());
		context.write(outKey, outValue);
		
	}
	
}