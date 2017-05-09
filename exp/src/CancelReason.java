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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class CancelReason {

	public static class FilterMapper 
		extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

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
				String cancelled = items[21];
				Text cancellationCode = new Text();
				try {
					if (Integer.parseInt(cancelled) != 0) {
						cancellationCode.set(items[22]);
						context.write(cancellationCode, one);
					}
				} catch (NumberFormatException e) { }
			}
		}
	}

	public static class IntSumReducer 
		extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			IntWritable sumWritable = new IntWritable();
			sumWritable.set(sum);
			context.write(key, sumWritable);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		if (remainingArgs.length != 4) {
			System.err.println("Usage: CancelReason <in> <out> <spltSize> <numRdcr>");
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

		System.out.println("mapreduce.map.memory.mb = " + conf.get("mapreduce.map.memory.mb"));
		System.out.println("mapreduce.map.java.opts = " + conf.get("mapreduce.map.java.opts"));
		conf.set("mapreduce.map.memory.mb", "1280");
		conf.set("mapreduce.map.java.opts", "-Xmx1024m");
		System.out.println("mapreduce.map.memory.mb = " + conf.get("mapreduce.map.memory.mb"));
		System.out.println("mapreduce.map.java.opts = " + conf.get("mapreduce.map.java.opts"));

		Job job = Job.getInstance(conf, "Occurance of reasons");
		job.setJarByClass(CancelReason.class);
		job.setMapperClass(FilterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(Integer.parseInt(remainingArgs[3]));
		FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

		Date startTime = new Date();
		int status = job.waitForCompletion(true) ? 0 : 1;
		Date endTime = new Date();
		long executionTime = endTime.getTime() - startTime.getTime();
		System.out.println("The execution time is " + ((double) executionTime) / 1000 + " s.");

		System.exit(status);
	}
}

