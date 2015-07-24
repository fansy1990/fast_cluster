/**
 * 
 */
package fz.fast_cluster;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import fz.utils.HUtils;



/**
 * 根据提供的id找到所对应的数据
 * @author fansy
 * @date 2015-7-3
 */
public class IDVectorJob extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception
	 * method: gaussian,cutoff
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(HUtils.getConf(), new IDVectorJob(), args);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = HUtils.getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err
					.println("Usage: fz.fastcluster.SortJob <in> <out> <ids>");
			System.exit(3);
		}
		conf.set("IDS", args[2]);
		Job job = Job.getInstance(conf,
				"find id vectors by id strings ,input:"
						+ otherArgs[0]+",id strings:"+args[2]);
		job.setJarByClass(IDVectorJob.class);
		System.out.println("任务名称："+job.getJobName());
		job.setMapperClass(IDVectorMapper.class);
		job.setReducerClass(Reducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		SequenceFileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(conf).delete(new Path(otherArgs[1]), true);
		return job.waitForCompletion(true) ? 0 : 1;

	}
	
	private static class IDVectorMapper extends Mapper<LongWritable,Text,
		IntWritable,Text>{
		private IntWritable id = new IntWritable();
		private String[] ids = null;
		public void setup(Context cxt){
			String tmpids = cxt.getConfiguration().get("IDS");
			
			ids = tmpids.split(",");
		}
		
		public void map(LongWritable key,Text value,Context cxt)throws IOException,
			InterruptedException{
			int index = value.toString().indexOf(",");
			String idStr = value.toString().substring(0, index);
			if(find(ids,idStr)){// 找到id，则直接输出
				id.set(Integer.parseInt(idStr));
				cxt.write(id, value);
			}
		}

		/**
		 * @param ids2
		 * @param idStr
		 * @return
		 */
		private boolean find(String[] ids, String idStr) {
			for (int i=0;i<ids.length;i++){
				if(idStr.equals(ids[i])){
					return true;
				}
			}
			return false;
		}
	}
	
	
}
