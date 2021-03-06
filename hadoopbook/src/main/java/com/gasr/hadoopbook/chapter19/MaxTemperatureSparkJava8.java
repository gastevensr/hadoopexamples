/**
 * 
 */
package com.gasr.hadoopbook.chapter19;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * @author alex
 *
 */
public class MaxTemperatureSparkJava8 {
	
	//clear && printf '\e[3J' && spark-submit --class com.gasr.hadoopbook.chapter19.MaxTemperatureSparkJava8 --master local hadoopbook-0.0.1-SNAPSHOT.jar hdfs://localhost/user/alex/input/ncdc/micro-tab/sample.txt outputspark
	public static void main(String[] args) {
		if (args.length != 2) {
			System.err.println("Usage: MaxTemperatureSparkJava8 <input path> <output path>");
			System.exit(-1);
		}
		SparkConf conf = new SparkConf();
		JavaSparkContext sc = new JavaSparkContext("local", "MaxTemperatureSparkJava8", conf);

		sc.textFile(args[0])
		.map(line -> line.split("\t"))
		.filter(line -> !line[1].equals("9999") && line[2].matches("[01459]"))
		.mapToPair(line -> new Tuple2<Integer, Integer>(Integer.parseInt(line[0]), Integer.parseInt(line[1])))
		.reduceByKey((i1, i2) -> Math.max(i1, i2))
		.saveAsTextFile(args[1]);
		
		sc.close();
	}

}
