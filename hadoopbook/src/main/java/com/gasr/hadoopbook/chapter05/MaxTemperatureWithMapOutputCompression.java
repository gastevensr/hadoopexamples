/**
 * 
 */
package com.gasr.hadoopbook.chapter05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
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
public class MaxTemperatureWithMapOutputCompression {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MaxTemperatureWithMapOutputCompression <input path> <output path>");
			System.exit(1);
		}
		
		Configuration conf = new Configuration();
		conf.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
		conf.setClass(Job.MAP_OUTPUT_COLLECTOR_CLASS_ATTR, GzipCodec.class, CompressionCodec.class);
		Job job = Job.getInstance();
		job.setJarByClass(MaxTemperature.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setCombinerClass(MaxTemperatureReducer.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		
		boolean flag = job.waitForCompletion(true);
		System.out.println("Finished? " + flag);
		System.exit(flag ? 0 : 1);
	}

}
