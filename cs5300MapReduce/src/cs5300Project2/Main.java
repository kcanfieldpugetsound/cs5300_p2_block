package cs5300Project2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
			+ "\n\trun input_filename output_directory num_runs"
		+ "\nto run another iteration (once already started):"
			+ "\n\tstep output_directory"
		+ "\nfor help:"
			+ "\n\thelp";
	
	private static final String help = "HELP FOR: Blocked PageRank | MapReduce\n"
		+ "\npre usage: pre edge_filepath block_filepath output_directory "
			+ "\n\tproduces the initial input to the MapReduce algorithm "
			+ "\n\tusing the given edge file and block file, and outputs "
			+ "\n\tthe result to output_directory/input.txt"
		+ "\nrun usage: run input_filename output_directory num_runs "
			+ "\n\truns MapReduce num_runs times using input_filename for "
			+ "\n\tthe input for the first iteration, and stores intermediate "
			+ "\n\tdata in output_directory; note that the input file must not "
			+ "\n\tbe in output_directory, as output_directory is cleared "
			+ "\n\tbefore each run";
	
	public static void main (String... elephants) {
		try {
			switch (elephants[0]) {
			case "pre":
				pre(elephants[1], elephants[2], elephants[3]);
				break;
			case "run":
				run(elephants[1], elephants[2], elephants[3]);
				break;
			case "step":
				step(elephants[1]);
				break;
			case "usage":
				System.out.println(usage);
				break;
			case "help":
				System.out.println(help);
				break;
			default:
				System.out.println("Invalid arguments.");
				System.out.println(usage);
			}
		} catch (Exception e) {
			System.out.println("Invalid arguments or internal error.");
			System.out.println(usage);
		}
	}
	
	public static void pre (String edgesFile, String blocksFile, String outputDirectory) {
		//create our input file from the given edges.txt,  blocks.txt
		InputFileCreator ifc = new InputFileCreator(edgesFile, blocksFile);
		ifc.createInputFile(outputDirectory + "/input.txt");
		return;
	}
	
	public static void run (String inputFilename, String outputDirectory, String numRuns) throws Exception {
		//perform some Hadoop configuration
		Configuration config = new Configuration();
		Path outputDir = new Path(outputDirectory);
		outputDir.getFileSystem(config).delete(outputDir, true);
		outputDir.getFileSystem(config).mkdirs(outputDir);
		
		Path input = new Path(inputFilename);
		
		int NUM_RUNS = Integer.parseInt(numRuns);
		
		//run the actual mapreduce job
		for (int i = 0; i < NUM_RUNS; i++) {
			Path output = new Path(outputDir, String.valueOf(i));
			
			runPageRank(input, output);
			
			input = output;
		}
	}
	
	public static void step (String outputDirectory) throws Exception {
		Configuration config = new Configuration();
		Path outputDir = new Path(outputDirectory);
		FileSystem fs = outputDir.getFileSystem(config);
		Path inputFile = null;
		int max_dir = 100;
		int i;
		for (i = max_dir; i >= 0; i--) {
			Path p = new Path(outputDirectory + "/" + i);
			if (fs.exists(inputFile)) {
				inputFile = p;
				break;
			}
		}
		
		if (inputFile == null) {
			System.out.println("run `run' before using step; run `usage' or "
				+ "`help' for more information");
		} else {
			System.out.println("Running `step' with input " + i 
				+ " and output " + (i + 1));
			Path outputFile = new Path(outputDirectory + "/" + (i + 1));
			runPageRank(inputFile, outputFile);
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
