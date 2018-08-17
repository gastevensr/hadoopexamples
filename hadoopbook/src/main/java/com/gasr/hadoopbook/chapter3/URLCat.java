/**
 * 
 */
package com.gasr.hadoopbook.chapter3;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * @author alex
 *
 */
public class URLCat {

	/*static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}*/

	/**
	 * @param args
	 */
	public static void main0(String[] args) throws Exception {
		InputStream is = null;
		try {
			is = new URL(args[0]).openStream();
			IOUtils.copyBytes(is, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(is);
		}
	}
	
	/**
	 * @param args
	 */
	public static void main1(String[] args) throws Exception {
		String uri = args[0];
		Configuration conf = new Configuration();
		System.out.println(conf.toString());
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		InputStream is = null;
		try {
			is = fs.open(new Path(uri));
			System.out.println("Directly from fs:");
			IOUtils.copyBytes(is, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(is);
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		String uri = args[0];
		Configuration conf = new Configuration();
		System.out.println(conf.toString());
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream is = null;
		try {
			is = fs.open(new Path(uri));
			IOUtils.copyBytes(is, System.out, 4096, false);
			is.seek(0);
			System.out.println("Back to the start: =========");
			IOUtils.copyBytes(is, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(is);
		}
	}

}
