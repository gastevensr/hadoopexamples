/**
 * 
 */
package com.gasr.hadoopbook.chapter19;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author alex
 *
 */
public class MaxTemperatureSpark {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 2) {
			System.err.println("Usage: MaxTemperatureSpark <input path> <output path>");
			System.exit(-1);
		}
		SparkConf conf = new SparkConf();
		JavaSparkContext sc = new JavaSparkContext("local", "MaxTemperatureSpark", conf);
		JavaRDD<String> lines = sc.textFile(args[0]);
		JavaRDD<String[]> records = lines.map(new Function<String, String[]>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 5486081304901183552L;

			@Override
			public String[] call(String s) {
				return s.split("\t");
			}
		});
		JavaRDD<String[]> filtered = records.filter(new Function<String[], Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 3867993223940547391L;

			@Override
			public Boolean call(String[] rec) throws Exception {
				return rec[1] != "9999" && rec[2].matches("[01459]");
			}
		});
		JavaPairRDD<Integer, Integer> tuples = filtered.mapToPair(new PairFunction<String[], Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -1836531182786764005L;

			@Override
			public Tuple2<Integer, Integer> call(String[] rec) throws Exception {
				return new Tuple2<Integer, Integer>(Integer.parseInt(rec[0]), Integer.parseInt(rec[1]));
			}
		});
		JavaPairRDD<Integer, Integer> maxTemps = tuples.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 8914828898764367660L;

			@Override
			public Integer call(Integer i1, Integer i2) throws Exception {
				return Math.max(i1, i2);
			}
		});
		maxTemps.saveAsTextFile(args[1]);
		sc.close();
	}

}
/*
 * Python script:
from pyspark import SparkContext

import re
import sys

sc = SparkContext("local", "MaxTemperature")
sc.textFile(sys.argv[1])
.map(lambda s: s.split("\t"))
.filter(lambda rec: (rec[1] != "9999" and re.match("[01459]", rec[2])))
.map(lambda rec: (int(rec[0]), int(rec[1])))
.reduceByKey(max)
.saveAsTextFile(sys.argv[2])

 */
