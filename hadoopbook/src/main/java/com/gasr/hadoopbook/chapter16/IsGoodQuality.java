/**
 * 
 */
package com.gasr.hadoopbook.chapter16;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * @author alex
 *
 */
public class IsGoodQuality extends FilterFunc {

	@Override
	public Boolean exec(Tuple tuple) throws IOException {
		if (tuple == null || tuple.size() == 0) {
			return false;
		}
		try {
			Object o = (Object) tuple.get(0);
			if (o == null) {
				return false;
			}
			int i = (Integer) o;
			return i == 0 || i == 1 || i == 4 || i == 5 || i == 9;
		} catch (ExecException ex) {
			throw new IOException(ex);
		}
	}

}
