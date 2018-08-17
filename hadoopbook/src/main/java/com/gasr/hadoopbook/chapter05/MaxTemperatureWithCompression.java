/**
 * 
 */
package com.gasr.hadoopbook.chapter05;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.gasr.hadoopbook.chapter2.MaxTemperature;
import com.gasr.hadoopbook.chapter2.MaxTemperatureMapper;
import com.gasr.hadoopbook.chapter2.MaxTemperatureReducer;

/**
 * @author alex
 *
 */
public class MaxTemperatureWithCompression {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MaxTemperatureWithCompression <input path> <output path>");
			System.exit(1);
		}
		
		Job job = Job.getInstance();
		job.setJarByClass(MaxTemperature.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		
		boolean flag = job.waitForCompletion(true);
		System.out.println("FINISHED: " + flag);
		System.exit(flag ? 0 : 1);
	}

}
