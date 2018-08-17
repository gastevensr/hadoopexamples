/**
 * 
 */
package com.gasr.hadoopbook.chapter05;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;

/**
 * @author alex
 *
 */
public class SequenceFileWriteDemo {
	
	private static final String[] DATA = {
			"One, two, buckle my shoe",
			"Three, four, shut the door",
			"Five, six, pick up sticks",
			"Seven, eight, lay them straight",
			"Nine, ten, a big fat hen"
		};

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);

		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;
		
		try {
			//writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			Option optPath = SequenceFile.Writer.file(path);
			Option optKey = SequenceFile.Writer.keyClass(key.getClass());
			Option optValue = SequenceFile.Writer.valueClass(value.getClass());
			writer = SequenceFile.createWriter(conf, optPath, optKey, optValue);

			for (int i = 0; i < 100; i++) {
				key.set(100 - i);
				value.set(DATA[i % DATA.length]);
				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}

}
