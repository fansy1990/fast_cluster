/**
 * 
 */
package fz.fast_cluster;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import fz.fast_cluster.keytype.DoublePairWritable;
import fz.fast_cluster.keytype.IntDoublePairWritable;
import fz.fast_cluster.mr.DeltaDistanceMapper;
import fz.fast_cluster.mr.DeltaDistanceReducer;
import fz.utils.HUtils;


/**
 * find delta distance of every point
 * 寻找大于自身密度的最小其他向量的距离
 * mapper输入：
 * 输入为<距离d_ij,<向量i编号，向量j编号>>
 * 把LocalDensityJob的输出
 * 		i,density_i
 * 放入一个map中，用于在mapper中进行判断两个局部密度的大小以决定是否输出
 * mapper输出：
 *      i,<density_i,min_distance_j>
 *      IntWritable,DoublePairWritable
 * reducer 输出：
 * 		<density_i*min_distancd_j> <density_i,min_distance_j,i>
 * 		DoubleWritable,  IntDoublePairWritable
 * @author fansy
 * @date 2015-7-3
 */
public class DeltaDistanceJob extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(HUtils.getConf(), new DeltaDistanceJob(), args);
	}

	public int run(String[] args) throws Exception {

		Configuration conf = HUtils.getConf();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length !=4) {
	      System.err.println("Usage: fz.fast_cluster.DeltaDistanceJob <in> <out> <local_density> <num_reducer>");
	      System.exit(4);
	    }
	    // 把localdensity最大值写入conf，
	    //把所有localdensity作为一个map的字符串，写入conf
	    initConfByLocalDensity(conf,otherArgs[2]);
	    
	    Job job =  Job.getInstance(conf,"find the nearest distance with the bigger near neighours");
	    System.out.println("任务名称："+job.getJobName());
	    job.setJarByClass(DeltaDistanceJob.class);
	    job.setMapperClass(DeltaDistanceMapper.class);
	    job.setReducerClass(DeltaDistanceReducer.class);
	    
	    job.setNumReduceTasks(Integer.parseInt(otherArgs[3]));
	    
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(DoublePairWritable.class);
	    
	    job.setOutputKeyClass(DoubleWritable.class);
	    job.setOutputValueClass(IntDoublePairWritable.class);
	    
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    SequenceFileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    SequenceFileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
	    
	    FileSystem.get(conf).delete(new Path(otherArgs[1]), true);
	    return job.waitForCompletion(true) ? 0 : 1;

	}

	/**
	 * @param conf2 
	 * @param string
	 * @return
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	private void initConfByLocalDensity(Configuration conf, String string) throws FileNotFoundException, IOException {
		Path densityPath = new Path(string);
		StringBuffer buff = new StringBuffer();
		SequenceFile.Reader reader = null;
		int max_density_vector_id=-1;// 最大的局部密度下标
		double max_density=-Double.MAX_VALUE;// 最大的局部密度 
		FileStatus[] fss=densityPath.getFileSystem(conf).listStatus(densityPath);
		for(FileStatus f:fss){
			if(!f.toString().contains("part")){
				continue; // 排除其他文件
			}
			try {
				reader = new SequenceFile.Reader(conf, Reader.file(f.getPath()),
						Reader.bufferSize(4096), Reader.start(0));
				IntWritable dkey = (IntWritable) ReflectionUtils.newInstance(
						reader.getKeyClass(), conf);
				DoubleWritable dvalue = (DoubleWritable) ReflectionUtils.newInstance(
						reader.getValueClass(), conf);
				while (reader.next(dkey, dvalue)) {// 循环读取文件
//					densityMap.put(dkey.get(), dvalue.get());
					buff.append(dkey.get()+"|"+dvalue.get()).append(",");
					if(dvalue.get()>max_density){// 寻找最大局部密度的下标
						max_density=dvalue.get();
						max_density_vector_id=dkey.get();
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				max_density_vector_id=-1;
			} finally {
				IOUtils.closeStream(reader);
			}
		}
		if(max_density_vector_id==-1){
			System.out.println("没有找到局部密度最大的向量，程序出错！");
			System.exit(-1);
		}
		conf.setInt("MAX_LOCAL_DENSITY_ID", max_density_vector_id);
		conf.setDouble("MAX_LOCAL_DENSITY", max_density);
		conf.set("LOCALDENSITYMAP", buff.toString());
//		System.out.println(buff.toString());
		return ;
	}
}