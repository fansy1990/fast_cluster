/**
 * 
 */
package fz.fast_cluster.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import fz.fast_cluster.keytype.DoubleArrIntWritable;
import fz.utils.HUtils;

/**
 * @author fansy
 * @date 2015-6-3
 */
public class ToSeqMapper extends Mapper<LongWritable, Text,IntWritable, DoubleArrIntWritable> {

	private String splitter =null;
	
	private DoubleArrIntWritable doubleArr= new DoubleArrIntWritable();
	private IntWritable idInt = new IntWritable(-1);
	@Override
	public void setup(Context cxt){
		splitter = cxt.getConfiguration().get("SPLITTER", ",");
	}
	
	@Override
	public void map(LongWritable key,Text value,Context cxt)throws IOException,InterruptedException{
		int index = value.toString().indexOf(splitter);
		
		double[] id= HUtils.getInputI(value.toString().substring(0, index),splitter);
		index = value.toString().indexOf(splitter, index);// 第二个值是label
		double[] vector =HUtils.getInputI(value.toString().substring(index+1, value.toString().length()),
				splitter);
		
		doubleArr.setValue(vector);
		idInt.set((int)id[0]);
		cxt.write(idInt,doubleArr);
	}
}
