import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Date;
import java.lang.Integer;
import java.lang.Long;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class AirlinesOnSchedule {

	public static class FilterMapper 
		extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private final static IntWritable zero = new IntWritable(0);

		private boolean caseSensitive;
		private Configuration conf;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			caseSensitive = conf.getBoolean("case.sensitive", true);
		}

		@Override
		public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

			String line = value.toString();
			String[] items = line.split(",");
			if (items.length == 29 && !items[0].equals("Year")) {
				String uniqueCarrier = (caseSensitive) ? items[8] : items[8].toUpperCase();
				Text airline = new Text();
				airline.set(uniqueCarrier);

				String arrDelay = items[14];
				String depDelay = items[15];
				try {
					if (Integer.parseInt(arrDelay) <= 0 && Integer.parseInt(depDelay) >= 0) {
						context.write(airline, one);
					} else {
						context.write(airline, zero);
					}
				} catch (NumberFormatException e) {
//					System.out.println(e.getMessage());
//					System.out.println("NumberFormatException: arrDelay = " + arrDelay + ", depDelay = " + depDelay);
				}
			}
		}
	}

	public static class ProbReducer 
		extends Reducer<Text, IntWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {

			int onSchedule = 0;
			int sum = 0;
			for (IntWritable val : values) {
				onSchedule += val.get();
				sum += 1;
			}
			double prob = (double) onSchedule / (double) sum;
			DoubleWritable probWritable = new DoubleWritable();
			probWritable.set(prob);
			context.write(key, probWritable);
		}
	}
	
	public static class SwapMapper 
		extends Mapper<Object, Text, DoubleWritable, Text> {

		@Override
		public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] items = line.split("\t");
			if (items.length != 2) {
				System.out.println("items.length = " + items.length);
			} else {
				Text airline = new Text();
				airline.set(items[0]);
				
				double prob;
				try {
					prob = Double.parseDouble(items[1]);
					DoubleWritable probWritable = new DoubleWritable();
					probWritable.set(prob);
					context.write(probWritable, airline);
				} catch (NumberFormatException e) {
//					System.out.println(e.getMessage());
//					System.out.println("NumberFormatException: prob = " + items[1]);
				}
			}
		}
	}
	
	public static class SortReducer 
		extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {

			double prob = key.get();
			DoubleWritable probWritable = new DoubleWritable();
			probWritable.set(prob);

			Text airline = new Text(); 
			for (Text val : values) {
				airline.set(val.toString());
				context.write(airline, probWritable);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		if (remainingArgs.length != 4) {
			System.err.println("Usage: AirlinesOnSchedule <in> <out> <spltSize> <numRdcr>");
			System.exit(2);
		}

		final long DEFAULT_SPLIT_SIZE = 128 * 1024 * 1024;
//		long split_minsize = conf.getLong(FileInputFormat.SPLIT_MINSIZE, DEFAULT_SPLIT_SIZE);
//		System.out.println("Default min split size = " + ((double) split_minsize) / (1024 * 1024) + " MB");
//		long split_maxsize = conf.getLong(FileInputFormat.SPLIT_MAXSIZE, DEFAULT_SPLIT_SIZE);
//		System.out.println("Default max split size = " + ((double) split_maxsize) / (1024 * 1024) + " MB");
		long split_size = 1024 * 1024 * Long.parseLong(remainingArgs[2]);
		conf.setLong(FileInputFormat.SPLIT_MINSIZE, split_size);
		conf.setLong(FileInputFormat.SPLIT_MAXSIZE, split_size);
//		split_minsize = conf.getLong(FileInputFormat.SPLIT_MINSIZE, DEFAULT_SPLIT_SIZE);
//		split_maxsize = conf.getLong(FileInputFormat.SPLIT_MAXSIZE, DEFAULT_SPLIT_SIZE);
//		if (split_minsize == split_size && split_maxsize == split_size) {
//			System.out.println("Split size = " + ((double) split_size) / (1024 * 1024) + " MB");
//		}

		Job job = Job.getInstance(conf, "Probs of Airlines on schedule");
		job.setJarByClass(AirlinesOnSchedule.class);
		job.setMapperClass(FilterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(ProbReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setNumReduceTasks(Integer.parseInt(remainingArgs[3]));
		FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path("airlinesOnScheduleIntermediate"));

		Date startTime = new Date();
		int status = job.waitForCompletion(true) ? 0 : 1;
		Date endTime = new Date();
		long executionTime = endTime.getTime() - startTime.getTime();
		System.out.println("The execution time is " + ((double) executionTime) / 1000 + " s.");

		Configuration conf2 = new Configuration();

//		split_maxsize = conf2.getLong(FileInputFormat.SPLIT_MAXSIZE, DEFAULT_SPLIT_SIZE);
//		System.out.println("Default max split size = " + ((double) split_maxsize) / (1024 * 1024) + " MB");
//		conf2.setLong(FileInputFormat.SPLIT_MAXSIZE, split_maxsize / 2);
//		split_maxsize = conf2.getLong(FileInputFormat.SPLIT_MAXSIZE, DEFAULT_SPLIT_SIZE);
//		System.out.println("Max split size = " + ((double) split_maxsize) / (1024 * 1024) + " MB");

		Job job2 = Job.getInstance(conf2, "Rank of Airlines on schedule");
		job2.setJarByClass(AirlinesOnSchedule.class);
		job2.setMapperClass(SwapMapper.class);
		job2.setMapOutputKeyClass(DoubleWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setReducerClass(SortReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(DoubleWritable.class);
		job2.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job2, new Path("airlinesOnScheduleIntermediate"));
		FileOutputFormat.setOutputPath(job2, new Path(remainingArgs[1]));

		startTime = new Date();
		status = job2.waitForCompletion(true) ? 0 : 1;
		endTime = new Date();
		executionTime = endTime.getTime() - startTime.getTime();
		System.out.println("The execution time is " + ((double) executionTime) / 1000 + " s.");

		System.exit(status);
	}
}

