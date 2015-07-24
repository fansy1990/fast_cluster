/**
 * 
 */
package fz.fast_cluster.mr;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fz.fast_cluster.keytype.DoublePairWritable;
import fz.fast_cluster.keytype.IntDoublePairWritable;

/**
 * 输出
 * <density_i*min_distancd_j> <first:density_i,second:min_distance_j,third:i>
 * 		DoubleWritable,  IntDoublePairWritable
 * @author fansy
 * @date 2015-6-1
 */
public class DeltaDistanceReducer extends
		Reducer<IntWritable, DoublePairWritable, DoubleWritable, IntDoublePairWritable> {

	private IntDoublePairWritable i_density_distance = new IntDoublePairWritable();
	private DoubleWritable mul = new DoubleWritable();
	
	private int max_density_vector_id=-1;
	
	@Override
	public void setup(Context cxt) throws IOException{
		max_density_vector_id = cxt.getConfiguration().getInt("MAX_LOCAL_DENSITY_ID", -1);
	}
	
	@Override
	public void reduce(IntWritable key,Iterable<DoublePairWritable> values,Context cxt) throws IOException,InterruptedException{
		// 如果是最大局部密度，则寻找最大距离，否则寻找最小距离
		double minDistance = key.get()!=max_density_vector_id?Double.MAX_VALUE:-Double.MAX_VALUE;
		double density=0.0;
		for(DoublePairWritable s:values){
			
			if(key.get()!=max_density_vector_id&&s.getSecond()<minDistance){// 寻找距离最小的
				minDistance = s.getSecond();
				density=s.getFirst();
			}
			if(key.get()==max_density_vector_id&&s.getSecond()>minDistance){// 寻找距离最大的
				minDistance = s.getSecond();
				density=s.getFirst();
			}
		}

		i_density_distance.setFirst(density);
		i_density_distance.setSecond(minDistance);
		i_density_distance.setThird(key.get());
		
		mul.set(density*minDistance);
		cxt.write(mul, i_density_distance);
	}
}
