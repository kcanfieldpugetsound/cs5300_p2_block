package cs5300Project2.blocked;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
	
	public static void main (String[] elephants) throws Exception {
		//edges.txt filename, blocks.txt filename, output directory name, num runs
		pageRank(elephants[0], elephants[1], elephants[2], elephants[3]);
	}
	
	public static void pageRank (String edgesFile, String blocksFile, String outputDirectory, String numRuns) throws Exception {
		
		//perform some Hadoop configuration
		Configuration config = new Configuration();
		Path outputDir = new Path(outputDirectory);
		outputDir.getFileSystem(config).delete(outputDir, true);
		
		Path input = new Path(outputDir, "input.txt");
		
		int NUM_RUNS = Integer.parseInt(numRuns);
		
		//create our input file from the given edges.txt,  blocks.txt
		InputFileCreator ifc = new InputFileCreator(edgesFile, blocksFile);
		ifc.createInputFile(outputDirectory + "/input.txt");
		
		
		//run the actual mapreduce job
		for (int i = 0; i < NUM_RUNS; i++) {
			Path output = new Path(outputDir, String.valueOf(i));
			
			runPageRank(input, output);
			
			input = output;
		}
	}
	
	private static double runPageRank(Path input, Path jobOutput) throws Exception {
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "BlockedPageRank");
		job.setJarByClass(Main.class);
		job.setMapperClass(BlockedMapper.class);
		job.setReducerClass(BlockedReducer.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, jobOutput);
		
		org.apache.log4j.BasicConfigurator.configure();
		
		if (!job.waitForCompletion(true)){
			throw new Exception("The job has failed to complete");
		}
		
		return Double.POSITIVE_INFINITY;
	}

}
