package cs5300Project2;

import java.io.File;
import java.io.PrintWriter;

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
	
	private static double ACCEPTABLE_CONVERGENCE = 0.001;
	
	private static final String usage = "USAGE FOR: Blocked PageRank | MapReduce\n"
		+ "\nto preprocess data:"
			+ "\n\tpre edge_filepath block_filepath output_directory"
		
		+ "\nto run program until it converges:"
			+ "\n\tconverge input_filename output_directory"
		
		+ "\nto run program n times:"
			+ "\n\trun input_filename output_directory num_runs"
		
		+ "\nto run another iteration (once already started):"
			+ "\n\tstep output_directory"
		
		+ "\nfor help:"
			+ "\n\thelp";
	
	private static final String help = "HELP FOR: Blocked PageRank | MapReduce\n"
		+ "\npre usage: pre edge_filepath block_filepath output_directory"
			+ "\n\tproduces the initial input to the MapReduce algorithm"
			+ "\n\tusing the given edge file and block file, and outputs"
			+ "\n\tthe result to output_directory/input.txt"
			
		+ "\nconverge usage: converge input_filename output_directory"
			+ "\n\truns MapReduce (using input_file for the input for the first "
			+ "\n\titeration, and stores intermediate data in output_directory) "
			+ "\n\tuntil it converges to a value of less"
			+ "\n\tthan " + ACCEPTABLE_CONVERGENCE + "; note that the input file "
			+ "\n\tmust not be in output_directory, as output_directory is cleared"
			+ "\n\tbefore each run"
			
		+ "\nrun usage: run input_filename output_directory num_runs"
			+ "\n\truns MapReduce (using input_filename for"
			+ "\n\tthe input for the first iteration, and stores intermediate"
			+ "\n\tdata in output_directory) num_runs times; note that the input "
			+ "\n\tfile must not be in output_directory, as output_directory "
			+ "\n\tis cleared before each run"
			
		+ "\nstep usage: step output_directory [num_steps]"
			+ "\n\tsearches output_directory for the most recent output, and"
			+ "\n\truns one (or num_steps if provided) iterations using this"
			+ "\n\tmost recent output as input";
	
	public static void main (String... elephants) {
		try {
			switch (elephants[0]) {
			case "pre":
				pre(elephants[1], elephants[2], elephants[3]);
				break;
			case "run":
				run(elephants[1], elephants[2], elephants[3]);
				break;
			case "converge":
				converge(elephants[1], elephants[2]);
				break;
			case "step":
				if (elephants.length > 2) {
					step(elephants[1], elephants[2]);
				} else {
					step(elephants[1], String.valueOf(1));
				}
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
	
	public static void converge (String inputFilename, String outputDirectory) throws Exception {
		//perform some Hadoop configuration
		Configuration config = new Configuration();
		Path outputDir = new Path(outputDirectory);
		outputDir.getFileSystem(config).delete(outputDir, true);
		outputDir.getFileSystem(config).mkdirs(outputDir);
		
		Path input = new Path(inputFilename);
		Path output;
		
		double convergence = Double.POSITIVE_INFINITY;	
		int iteration = 0;
		while (convergence > ACCEPTABLE_CONVERGENCE) {
			output = new Path(outputDirectory + "/" + iteration);
			convergence = runPageRank(input, output);
			input = output;
			iteration++;
		}
		System.out.println("reached convergence " + convergence + " after " 
			+ iteration + " iteration(s)");
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
		double convergence = Double.POSITIVE_INFINITY;
		for (int i = 0; i < NUM_RUNS; i++) {
			Path output = new Path(outputDir, String.valueOf(i));
			
			convergence = runPageRank(input, output);
			
			input = output;
		}

		System.out.println("reached convergence " + convergence + " after " 
			+ NUM_RUNS + " iteration(s)");
	}
	
	public static void step (String outputDirectory, String numSteps) throws Exception {
		Configuration config = new Configuration();
		Path outputDir = new Path(outputDirectory);
		FileSystem fs = outputDir.getFileSystem(config);
		Path inputFile = null;
		
		int NUM_STEPS = Integer.parseInt(numSteps);
		
		//I don't know how to get children of the output directory, so 
		//I'll just have to guess and check... sigh... but it works...
		//I'm guessing you won't be running more than 100 iterations...
		int max_dir = 100;
		int i;
		for (i = max_dir; i >= 0; i--) {
			Path p = new Path(outputDirectory + "/" + i);
			if (fs.exists(p)) {
				inputFile = p;
				break;
			}
		}
		
		if (inputFile == null) {
			System.out.println("`run' must be called before using step; "
				+ "`usage' or `help' for more information");
		} else {
			int iterationNumber = i;
			Path outputFile;
			double convergence = Double.POSITIVE_INFINITY;
			for (int j = 0; j < NUM_STEPS; j++) {
				iterationNumber++;
				outputFile = new Path(outputDirectory + "/" + iterationNumber);
				convergence = runPageRank(inputFile, outputFile);
				inputFile = outputFile;
			}
			System.out.println("reached convergence " + convergence + " after " 
				+ NUM_STEPS + " more iteration(s) (total iterations: " 
				+ (iterationNumber + 1));
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
		
		long scaledConvergence = job.getCounters().findCounter(BlockedReducer.Counter.CONVERGENCE).getValue();
		long numNodes = job.getCounters().findCounter(BlockedReducer.Counter.NUM_NODES).getValue();
		
		double convergence = 
			((double) scaledConvergence) / 
			((double) BlockedReducer.SCALING_FACTOR) / 
			((double) numNodes);
		
		PrintWriter pw = new PrintWriter(new File(jobOutput.getParent().toUri() + "/stats.txt"));
		pw.println("\n\n\n---------- ITERATION ----------\n");
		pw.println("reached convergence " + convergence);
		pw.println("\nBLOCK REPORT:\n");
		for (Integer blockId : BlockedReducer.report.keySet()) {
			Node lowest = BlockedReducer.report.get(blockId).left();
			Node second = BlockedReducer.report.get(blockId).right();
			pw.println("block " + blockId);
			pw.println("\tnode " + lowest.nodeId() + ": " + lowest.getCurrPageRank());
			pw.println("\tnode " + second.nodeId() + ": " + second.getCurrPageRank());
		}
		pw.close();
		return convergence;
	}

}
