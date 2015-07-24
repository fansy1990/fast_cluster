/**
 * 
 */
package fz.fast_cluster.mr;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fz.fast_cluster.FilterCounter;
import fz.fast_cluster.keytype.IntPairWritable;

/**
 * @author fansy
 * @date 2015-6-26
 */
public class CalDistanceReducer extends
		Reducer<DoubleWritable, IntPairWritable, DoubleWritable, IntPairWritable> {

	public void reduce(DoubleWritable key,Iterable<IntPairWritable> values,Context cxt)throws InterruptedException,IOException{
		for(IntPairWritable v:values){
			cxt.getCounter(FilterCounter.REDUCE_COUNTER).increment(1);
			cxt.write(key, v);
		}
	}
}
