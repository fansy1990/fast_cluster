/**
 * 
 */
package fz.fast_cluster.mr;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fz.fast_cluster.keytype.DoublePairWritable;
import fz.fast_cluster.keytype.IntPairWritable;


/**
 * 
 * @author fansy
 * @date 2015-6-1
 */
public class DeltaDistanceMapper extends
		Mapper<DoubleWritable, IntPairWritable, IntWritable, DoublePairWritable> {
	private Logger log = LoggerFactory.getLogger(DeltaDistanceMapper.class);
	
	private IntWritable vector_i= new IntWritable();
	private DoublePairWritable density_distance = new DoublePairWritable();
	
	private Map<Integer,Double> densityMap= new HashMap<Integer,Double>();// vector_id,density
	private int max_density_vector_id=-1;// 最大的局部密度下标
	private double max_density=0.0;
	@Override
	public void setup(Context cxt) throws IOException{
		max_density= cxt.getConfiguration().getDouble("MAX_LOCAL_DENSITY", 0.0);
		max_density_vector_id = cxt.getConfiguration().getInt("MAX_LOCAL_DENSITY_ID", -1);
		String densityMap_= cxt.getConfiguration().get("LOCALDENSITYMAP");
		System.out.println(densityMap_);
		String[] densityArr = densityMap_.split(",");
		int index = -1;
		for(int i=0;i<densityArr.length;i++){
			index= densityArr[i].indexOf("|");
			densityMap.put(Integer.parseInt(densityArr[i].substring(0, index)), 
					Double.parseDouble(densityArr[i].substring(index+1,densityArr[i].length())));
		}
	}

	// distance_ij,<i,j>
	@Override
	public void map(DoubleWritable key, IntPairWritable value, Context cxt) throws IOException,InterruptedException{
		int vectorI= value.getFirst();
		int vectorJ= value.getSecond();
		if(vectorI==max_density_vector_id||vectorJ==max_density_vector_id){// 最大局部密度，需寻找离该点最大的距离,应该在reducer中判断
			// 这里应该直接输出即可
			vector_i.set(max_density_vector_id);
			density_distance.setFirst(max_density);
			density_distance.setSecond(key.get());
			cxt.write(vector_i, density_distance);
			log.info("vector_i:{},density:{},distance:{}",new Object[]{max_density_vector_id,max_density,key.get()});
			return ;
			
		}
		// 不是局部密度最大点，则找最小的即可
		double densityI=0;
		double densityJ=0;
		if(!densityMap.containsKey(vectorI)||!densityMap.containsKey(vectorJ)){//  两个任一个不存在，则不输出任何
			return ;
		}
		
		densityI=densityMap.get(vectorJ);
		densityJ=densityMap.get(vectorI);
		
		if(densityI<densityJ){// 输出   <I,大于I 的density的distance>
			vector_i.set(vectorI);
			density_distance.setFirst(densityI);
			density_distance.setSecond(key.get());// second 是distance
			cxt.write(vector_i, density_distance);
			log.info("vector_i:{},density:{},distance:{}",new Object[]{vectorI,densityI,key.get()});
		}
		if(densityI>densityJ){// 输出   <J,大于J 的density的distance>
			vector_i.set(vectorJ);
			density_distance.setFirst(densityJ);
			density_distance.setSecond(key.get());
			cxt.write(vector_i, density_distance);
			log.info("vector_i:{},density:{},distance:{}",new Object[]{vectorJ,densityJ,key.get()});
		}
	}
}






