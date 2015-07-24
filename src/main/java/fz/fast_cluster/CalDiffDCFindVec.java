/**
 * 
 */
package fz.fast_cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ToolRunner;

import fz.draw.DrawPic;
import fz.utils.HUtils;

/**
 * 使用不同的阈值来作为聚类中心点的阈值，并且找出这些数据
 * 并画图
 * @author fansy
 * @date 2015-7-23
 */
public class CalDiffDCFindVec {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		if(args.length!=9){
			System.err.println("Usage:fz.fast_cluster.CalDiffDCFindVec <path> <numRecords> " +
					"<from> <to> <howMany> <localDensityNumReducer> <deltaDistanceNumReducer>" +
					" <localSavePath> <numPoints>");
			System.exit(-1);
		}
		String path = args[0];
		long numRecords = Long.parseLong(args[1]);
		double from = Double.parseDouble(args[2]);
		double to = Double.parseDouble(args[3]);
		int howMany = Integer.parseInt(args[4]);
		
		String localDensityNumReducer = args[5];
		String deltaDistanceNumReducer = args[6];
		String localSavePath = args[7];
		String numPoints=args[8];
		
		double [] distances =null;
		try{
			distances=readDistanceAndFindDC(path,numRecords,from ,to,howMany);
		}catch(Exception e){
			e.printStackTrace();
			System.exit(-1);
		}
		for (int i=0;i<distances.length;i++){
			System.out.println("running "+i+" iteration...");
			runMRs(i,distances[i],localDensityNumReducer,deltaDistanceNumReducer);
			
			// 读取数据，写入本地文件，并画图
			String sort_out = "sort_out/part-r-00000";
 			readVecAndDraw(sort_out,localSavePath,numPoints,i);
		}
		
		System.out.println("All jobs are done!");
		
	}
	
	/**
	 * 读取数据，写入本地文件，并画图
	 * @param sort_out
	 * @param localSavePath
	 * @param numPoints
	 * @param i
	 * @throws Exception 
	 * @throws NumberFormatException 
	 */
	private static void readVecAndDraw(String sort_out, String localSavePath,
			String numPoints, int i) throws NumberFormatException, Exception {
		try {
			DrawPic.drawPic(DrawPic.getXYSeries(sort_out, localSavePath+"/log_center_"+i+".dat",
					Integer.parseInt(numPoints)), localSavePath+"/decision_"+i+".png");
		} catch (Exception e) {
			e.printStackTrace();
		}
			
	}

	/**
	 * 运行 LocalDensityJob和DeltaDistanceJob 和SortJob
	 * 
	 * @param distance
	 * @throws Exception 
	 */
	public static void runMRs(int iter_i,double distance,String numLReducer,String numDReducer ) throws Exception{
		// 运行 localDensity 
		String cal_distance_out="hdfs://node101:/user/root/cal_distance_out";
		String[] arg= new String[]{
				cal_distance_out,
				"localdensity_out",
				String.valueOf(distance) ,
				"cut-off",
				numLReducer
		};
		// <in> <out> <dc> <method> <num_reducer>
		int ret = ToolRunner.run(new Configuration() , new LocalDensityJob(), arg);
		if(ret!=0){
			System.err.println("i:"+iter_i+",localdensity job failed!");
		}
		
		arg=new String[]{
				cal_distance_out,
				"delta_distance_out",
				"localdensity_out",
				numDReducer
		};
		//<in> <out> <local_density> <num_reducer>
		ret = ToolRunner.run(new Configuration(), new DeltaDistanceJob(), arg);
		if(ret!=0){
			System.err.println("i:"+iter_i+",deltadistance job failed!");
		}
		arg=new String[]{
				"delta_distance_out",
				"sort_out",
				"1"
		};
		// <in> <out> <num_reducer>
		ret = ToolRunner.run(new Configuration(), new SortJob(), arg);
		if(ret!=0){
			System.err.println("i:"+iter_i+",sort job failed!");
		}
	}
	
	/**
	 * 寻找从from到to的最小距离howMany个距离
	 * @param path
	 * @param num
	 * @param from
	 * @param to
	 * @param howMany
	 * @return
	 * @throws Exception 
	 */
	public static double[] readDistanceAndFindDC(String path,long num,double from ,double to,int howMany) throws Exception{
		Path input = null;
		input = new Path(path);
		Configuration conf = HUtils.getConf();
		SequenceFile.Reader reader = null;
		long counter = 0;

		double[] distances = new double[howMany];
		
		double[] percents = new double[howMany];
		
		double delta = (to-from)/howMany;
		for(int i=0;i<howMany;i++){
			percents[i]=from+i*delta;
		}
		
		try {
			reader = new SequenceFile.Reader(conf, Reader.file(input),
					Reader.bufferSize(4096), Reader.start(0));
			DoubleWritable dkey = (DoubleWritable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			Writable dvalue = (Writable) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);
			int i=0;
			long tmp=0;
			while (reader.next(dkey, dvalue)) {// 循环读取文件
				counter++;
				tmp=(long)(percents[i]*num);
				if(counter==tmp){ // 
					distances[i]=dkey.get();
					System.out.println(percents[i]*100+"%的距离是："+dkey.get());
					i++;
					
				}
				if (i>=howMany||counter>to*num) {
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			IOUtils.closeStream(reader);
		}
		return distances;
	}
	
	

}
