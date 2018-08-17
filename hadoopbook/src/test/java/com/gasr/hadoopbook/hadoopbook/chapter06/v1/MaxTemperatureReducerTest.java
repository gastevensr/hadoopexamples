/**
 * 
 */
package com.gasr.hadoopbook.hadoopbook.chapter06.v1;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import com.gasr.hadoopbook.chapter06.v1.MaxTemperatureReducer;

/**
 * @author alex
 *
 */
public class MaxTemperatureReducerTest {

	@Test
	public void returnMaximumIntegerInValues() throws IOException, InterruptedException {
		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
		.withReducer(new MaxTemperatureReducer())
		.withInput(new Text("1950"), Arrays.asList(new IntWritable(10), new IntWritable(5)))
		.withOutput(new Text("1950"), new IntWritable(10))
		.runTest();
	}
}
