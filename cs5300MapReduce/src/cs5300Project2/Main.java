package cs5300Project2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {
	
	private static final String usage = "USAGE FOR: Blocked PageRank | MapReduce\n"
		+ "\nto preprocess data:"
			+ "\n\tpre edge_filepath block_filepath output_directory"
		+ "\nto run program:"
			+ "\n\trun output_directory num_runs"
		+ "\nfor help:"
			+ "\n\thelp";
	
	public static void main (String... elephants) {
		try {
			switch (elephants[0]) {
			case "pre":
				pre(elephants[1], elephants[2], elephants[3]);
				break;
			case "run":
				run(elephants[1], elephants[2]);
				break;
			default:
				System.out.println(usage);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(usage);
		}
	}
	
	public static void pre (String edgesFile, String blocksFile, String outputDirectory) {
		//create our input file from the given edges.txt,  blocks.txt
		InputFileCreator ifc = new InputFileCreator(edgesFile, blocksFile);
		ifc.createInputFile(outputDirectory + "/input.txt");
		return;
	}
	
	public static void run (String outputDirectory, String numRuns) throws Exception {
		//perform some Hadoop configuration
//		Configuration config = new Configuration();
		Path outputDir = new Path(outputDirectory);
//		outputDir.getFileSystem(config).delete(outputDir, true);
//		outputDir.getFileSystem(config).mkdirs(outputDir);
		
		Path input = new Path(outputDir, "input.txt");
		
		int NUM_RUNS = Integer.parseInt(numRuns);
		
		//run the actual mapreduce job
		for (int i = 0; i < NUM_RUNS; i++) {
			System.out.println("running pagerank iteration " + i);
			Path output = new Path(outputDir, String.valueOf(i));
			
			runPageRank(input, output);
			
			input = output;
			System.out.println("\tcompleted pagerank iteration " + i);
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
