/**
 * 
 */
package com.gasr.hadoopbook.chapter09;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.gasr.hadoopbook.chapter2.MaxTemperatureReducer;
import com.gasr.hadoopbook.common.JobBuilder;
import com.gasr.hadoopbook.common.NcdcRecordParser;

/**
 * @author alex
 *
 */
public class MaxTemperatureWithCounters extends Configured implements Tool {

	enum Temperature {
		MISSING, 
		MALFORMED
	}

	static class MaxTemperatureMapperWithCounters extends Mapper<LongWritable, Text, Text, IntWritable> {

		private NcdcRecordParser parser = new NcdcRecordParser();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			parser.parse(value);
			if (parser.isValidTemperature()) {
				int airTemperature = parser.getAirTemperature();
				context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
			} else if (parser.isMalformedTemperature()) {
				System.err.println("Ignoring possible corrupt input: " + value);
				context.getCounter(Temperature.MALFORMED).increment(1);
			} else if (parser.isMissingTemperature()) {
				context.getCounter(Temperature.MISSING).increment(1);
			}
			// dynamic counter
			context.getCounter("TemperatureQuality", parser.getQuality()).increment(1);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
		if (job == null) {
			return -1;
		}
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MaxTemperatureMapperWithCounters.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducer.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main2(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MaxTemperatureWithCounters(), args);
		System.exit(exitCode);
	}

	/*public static void main(String[] args) throws Exception {
		List<String> list = new ArrayList<String>();
		list.add("abc");
		list.add("b");
		list.add("c");
		list.add("d");
		
		Predicate<String> pred = p -> {
			return p.equals("abc");
		};
		pred.and(s -> s.length() != 0);
		pred.or(s -> s == null);
		System.out.println(firstOrNull(list, pred));
	}
	
	private static String firstOrNull(List<String> list, Predicate<String> pred) {
		String result = list.stream().filter(p -> pred.test(p)).findFirst().orElse(null);
		return result;
	}*/

	//clear && printf '\e[3J' && ./sqoop import --connect jdbc:mysql://localhost/hadoopguide --table widgets -m 1 --username=''@'localhost' --password=123456 --direct
}
