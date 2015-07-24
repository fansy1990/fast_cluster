/**
 * 
 */
package fz.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import fz.fast_cluster.keytype.DDoubleWritable;
import fz.fast_cluster.keytype.DoubleArrWritable;

/**
 * @author fansy
 * @date 2015-6-2
 */
public class HUtils {

	
	public static final String CENTERPATH="/user/root/fc_center";
	
	public static final String CENTERPATHPREFIX="/user/root/fc_center/iter_";
	
	private static Configuration conf = null;
	public static final String DEFAULTFS="hdfs://node101:8020";
//	public static final String DELTADISTANCEBIN = "/user/root/fc_delta.bin";
	public static final String LOCALDENSITYMAP = "/user/root/localdensity.bin";
	
	public static final String DELTADISTANCEOUTPUT="/user/root/fc_deltadistance";
	public static final String FIRSTCENTERPATH="/user/root/fc_center/iter_0/clustered/part-m-00000";
	
	public static final String FIRSTUNCLUSTEREDPATH="/user/root/fc_center/iter_0/unclustered";

	public static final String LOCALDENSITYOUTPUT="/user/root/fc_localdensity";
	
	
	//
	public static final double VERYSMALL=0.00000000000000000000000000001;
	
	public static String doubleArr2Str(double[] d){
		StringBuffer buff = new StringBuffer();
		
		for(int i=0;i<d.length;i++){
			buff.append(d[i]).append(",");
		}
		return buff.substring(0, buff.length()-1);
	}
	
	/**
	 * find whether the d can replace one of the r array
	 * if can return the index
	 * else return -1
	 * @param r
	 * @param d
	 */
	public static int findLargest(double[] r, double d) {
		double max = -Double.MAX_VALUE;
		int index=0;
		for(int i=0;i<r.length;i++){
			if(r[i]>max){
				max=r[i];
				index=i;
			}
		}
		if(r[index]>d){
			return index;
		}
		return -1;
	}
	
	/**
	 * find whether the d can replace one of the r array
	 * if can return the index
	 * else return -1
	 * @param r
	 * @param d
	 */
	private static int findSmallest(double[] r, double d) {
		double small = Double.MAX_VALUE;
		int index=0;
		for(int i=0;i<r.length;i++){
			if(r[i]<small){
				small=r[i];
				index=i;
			}
		}
		if(r[index]<d){
			return index;
		}
		return -1;
	}
	
	

	/**
	 * get the cluster center by the given k
	 * return the dc for next ClusterDataJob
	 * @param input
	 * @param output
	 * @param k
	 * @throws IOException
	 */
	public static double[] getCenterVector(String input ,String output,int k) throws IOException{
		double [] r= new double [k];
		String[] queue = new String[k];
		
		//initialize the r array
		for(int i=0;i<k;i++){
			r[i]=-Double.MAX_VALUE;
		}
		Path path = new Path(input);
		Configuration conf = HUtils.getConf();
	    InputStream in =null;  
        try {  
        	FileSystem fs = FileSystem.get(URI.create(input), conf);  
        	in = fs.open(path);  
        	BufferedReader read = new BufferedReader(new InputStreamReader(in));  
            String line=null;  
            int index=-1;
            while((line=read.readLine())!=null){  
//                [5.5,4.2,1.4,0.2]	5,0.3464101615137755
                String[] lines = line.split("\t");
                String[] sd= lines[1].split(",");
                index =findSmallest(r,Double.parseDouble(sd[0])*Double.parseDouble(sd[1]));
				if(index !=-1){
					r[index]=Double.parseDouble(sd[0])*Double.parseDouble(sd[1]);
					queue[index]=lines[0];
				}
            }  
     
	    } catch (IOException e) {  
	        e.printStackTrace();  
	    }finally{  
            try {  
                in.close();  
            } catch (IOException e) {  
                e.printStackTrace();  
            }  
	    }  
		
		// print
        double dc =Double.MAX_VALUE;
        double dc_max = -Double.MAX_VALUE;
        double distance =0.0;
		for(int i=0;i<queue.length;i++){
			System.out.print("vector:"+queue[i]);
			for(int j=i+1;j<queue.length;j++){
				distance = HUtils.getDistance(getInputI(queue[i].substring(1, queue[i].length()-1), ","),
						getInputI(queue[j].substring(1, queue[j].length()-1), ","));
				if(distance<dc){
					dc = distance ;
				}
				if(distance>dc_max){
					dc_max=distance;
				}
			}
			System.out.print("\tr:"+r[i]+"\n");
		}
		// write to hdfs
		
		path = new Path(output);
		DoubleArrWritable key = null;
		IntWritable value = new IntWritable();
		SequenceFile.Writer writer =null;
		try{
			writer =SequenceFile.createWriter(conf, 
					Writer.file(path),
					Writer.keyClass(DoubleArrWritable.class),
					Writer.valueClass(value.getClass())
					);
			for(int i=0;i<queue.length;i++){
				key = new DoubleArrWritable(getInputI(queue[i].substring(1, queue[i].length()-1), ","));
				value.set(i+1);
				writer.append(key, value);
			}
		}finally{
			IOUtils.closeStream(writer);
		}
		return new double[]{dc/5,dc_max/3};
	}
	
	public static Configuration getConf(){
		
		if(conf ==null){
			conf = new Configuration ();
			conf.setBoolean("mapreduce.app-submission.cross-platform", true);// 配置使用跨平台提交任务  
		    conf.set("fs.defaultFS", DEFAULTFS);//指定namenode    
		    conf.set("mapreduce.framework.name", "yarn");  // 指定使用yarn框架  
		    conf.set("yarn.resourcemanager.address", "node101:8032"); // 指定resourcemanager  
		    conf.set("yarn.resourcemanager.scheduler.address", "node101:8030");// 指定资源分配器
		}
		
		return conf;
	}
	
	/**
	 * use the oath distance 
	 * @param inputI
	 * @param ds
	 * @return
	 */
	public static double getDistance(double[] inputI, double[] ds) {
		double error =0.0;
		for(int i=0;i<inputI.length;i++){
			error+=(inputI[i]-ds[i])*(inputI[i]-ds[i]);
		}
		return Math.sqrt(error);
	}
	
	
	public static String getHDFSPath(String url){
		return DEFAULTFS+url;
	}


	public static double[] getInputI(String value,String splitter){
		String[] inputStrArr = value.split(splitter);
		double[] inputI = new double[inputStrArr.length];
		
		for(int i=0;i<inputI.length;i++){
			inputI[i]= Double.parseDouble(inputStrArr[i]);
		}
		return inputI;
	}
	/**
	 * 字符串转为double数组
	 * @param value
	 * @return
	 */
	public static double[] getInputI(Text value,String splitter) {
		return getInputI(value.toString(),splitter);
	}
	
	public static String intArr2Str(int[] d){
		StringBuffer buff = new StringBuffer();
		
		for(int i=0;i<d.length;i++){
			buff.append(d[i]).append(",");
		}
		return buff.substring(0, buff.length()-1);
	}
	
	public static void main(String[] args) throws IOException {
//		String input = "hdfs://node101:8020/user/root/iris_out00/part-r-00000";
//		String output = "hdfs://node101:8020/user/root/iris_center/center00.dat";
//		String tmp= "hdfs://node101:8020/user/root/iris_clustered/part-m-00000";
//		int k =3;
		
//		getCenterVector(input,output,k);
//		readSeq(output);
//		readSeq(tmp);
	}
	/**
	 * read center to local file
	 * @param iter_i
	 * @param localPath
	 */
	public static void readCenterToLocal(int iter_i,String localPath){
		
		FileSystem fs =null;
		FileWriter writer =null;
		BufferedWriter bw =null;
		try{
		fs = FileSystem.get(getConf());
		// read all before center files 
		String parentFolder =null;
		Path path =null;
		writer = new FileWriter(localPath);
		
        bw = new BufferedWriter(writer);
         
        SequenceFile.Reader reader = null;
		int start = iter_i==0?0:1;
		for(int i=start;i<=iter_i;i++){
			parentFolder=HUtils.getHDFSPath(HUtils.CENTERPATH+"/iter_"+i+"/clustered");
			if(!fs.exists(new Path(parentFolder))){
				continue;
			}
			RemoteIterator<LocatedFileStatus> files=fs.listFiles(new Path(parentFolder), false);
			while(files.hasNext()){
				path = files.next().getPath();
				if(!path.toString().contains("part")){
					continue; // return 
				}
				reader = new SequenceFile.Reader(conf, Reader.file(path),
						Reader.bufferSize(4096), Reader.start(0));
				DoubleArrWritable dkey = (DoubleArrWritable) ReflectionUtils.newInstance(
						reader.getKeyClass(), conf);
				IntWritable dvalue = (IntWritable) ReflectionUtils.newInstance(
						reader.getValueClass(), conf);
				while (reader.next(dkey, dvalue)) {// read file literally
					bw.write(doubleArr2Str(dkey.getDoubleArr())+","+dvalue.get());
					bw.newLine();
				}
			}
		}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			 try {
				bw.close();writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	         
		}
	}
	
	/**
	 * 输出 1%~10%的数据的距离
	 * @param path
	 * @param num
	 */
	public static void readDistanceAndFindDC(String path,long num){
	Path input = null;
	input = new Path(path);
	Configuration conf = HUtils.getConf();
	SequenceFile.Reader reader = null;
	long counter = 0;
	double percent=0.01;
	long percent_ = (long) (percent * num);
	try {
		reader = new SequenceFile.Reader(conf, Reader.file(input),
				Reader.bufferSize(4096), Reader.start(0));
		DoubleWritable dkey = (DoubleWritable) ReflectionUtils.newInstance(
				reader.getKeyClass(), conf);
		Writable dvalue = (Writable) ReflectionUtils.newInstance(
				reader.getValueClass(), conf);
		while (reader.next(dkey, dvalue)) {// 循环读取文件
			counter++;
			if(counter%1000==0){
//				System.out.println("读取了"+counter+"条记录。。。");
			}
			if (counter == percent_) {
				System.out.println(percent*100+"%的距离是："+dkey.get());
				percent+=0.01;
				percent_=(long)(percent*num);
			}
		}
	} catch (Exception e) {
		e.printStackTrace();
	} finally {
		IOUtils.closeStream(reader);
	}
}
	public static void readHDFSFile(String url,String localPath){
		Path path = new Path(url);
		Configuration conf = HUtils.getConf();
		FileWriter writer =null;
		BufferedWriter bw =null;
		 InputStream in =null;  
		try {
			writer = new FileWriter(localPath);
	        bw = new BufferedWriter(writer);
	        FileSystem fs = FileSystem.get(URI.create(url), conf);  
        	in = fs.open(path);  
        	BufferedReader read = new BufferedReader(new InputStreamReader(in));  
            String line=null;  
             
            while((line=read.readLine())!=null){  
//                System.out.println("result:"+line.trim());  
//                [5.5,4.2,1.4,0.2]	5,0.3464101615137755
                String[] lines = line.split("\t");
                bw.write(lines[1]);
                bw.newLine();
            }  
			System.out.println(new java.util.Date()+"ds file:"+localPath);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();bw.close();writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	public static void readSeq(String url,String localPath){
		Path path = new Path(url);
		Configuration conf = HUtils.getConf();
		SequenceFile.Reader reader = null;
		FileWriter writer =null;
		BufferedWriter bw =null;
		try {
			writer = new FileWriter(localPath);
	        bw = new BufferedWriter(writer);
			reader = new SequenceFile.Reader(conf, Reader.file(path),
					Reader.bufferSize(4096), Reader.start(0));
			DoubleArrWritable dkey = (DoubleArrWritable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			DDoubleWritable dvalue = (DDoubleWritable) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);

			while (reader.next(dkey, dvalue)) {// 循环读取文件
				bw.write(dvalue.getDistance()+","+dvalue.getSum());
				bw.newLine();
			}
			System.out.println(new java.util.Date()+"ds file:"+localPath);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
			try {
				bw.close();writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
