/**
 * 
 */
package fz.fast_cluster;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import fz.utils.HUtils;

/**
 * 寻找和写入center文件
 * @author fansy
 * @date 2015-7-24
 */
public class FindWriteCenter {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		if(args.length!=6){
			System.out.println("Usage:fz.fast_cluster.FindWriteCenter <local_centeridfile> <densityDelta> " +
					"<distanceDelta> <hdfs_original_data> <hdfs_mr_out> <loca_save_file>");
			System.exit(-1);
		}
		
		String filename = args[0];
		double densityDelta = Double .parseDouble(args[1]);
		double distanceDelta = Double.parseDouble(args[2]);
		String hdfs_original_data = args[3];
		String hdfs_mr_out = args[4];
		String local_save_file= args[5];
		
		FindWriteCenter fwc = new FindWriteCenter();
		
		// 1. 从本地distance、density、id文件中获得聚类中心向量的id
		List<Integer> ids = fwc.getIDs(filename, densityDelta, distanceDelta);
		
		// 2. 运行MR任务，从原始hdfs文件中找到这些id对应的数据，并输出；
		StringBuffer buff =new  StringBuffer();
		for(Integer id:ids){
			buff.append(id).append(",");
		}
		String[] arg = new String[]{
				hdfs_original_data,
				hdfs_mr_out,
				buff.substring(0, buff.length())
		};
		IDVectorJob.main(arg);
		// 3. 解析MR的输出到本地文件
		resolveToLocal(hdfs_mr_out+"/part-r-00000",local_save_file);
	}
	/**
	 * 解析MR的输出到本地文件
	 * @param string
	 * @param local_save_file
	 * @throws IOException 
	 */
	public static void resolveToLocal(String string, String local_save_file) throws IOException {
		Path input = null;
		input = new Path(string);
		Configuration conf = HUtils.getConf();
		SequenceFile.Reader reader = null;
		FileWriter writer =null;
		BufferedWriter bw =null;
		
		try {
			writer = new FileWriter(local_save_file);
			
	        bw = new BufferedWriter(writer);
			reader = new SequenceFile.Reader(conf, Reader.file(input),
					Reader.bufferSize(4096), Reader.start(0));
			IntWritable dkey = (IntWritable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			Text dvalue = (Text) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);
			String value = null;
			int index =0;
			while (reader.next(dkey, dvalue)) {// 循环读取文件
				value=dvalue.toString();
//				System.out.println(""+value);
				bw.append("id:"+dkey.get()+",");
				index = value.indexOf(",");
				bw.append("type:"+value.substring(index+1,value.indexOf(",", index+1))+",");
				index = value.indexOf(",", index);
				bw.append("vector:"+value.substring(index+1));		
				bw.newLine();
				
			}
			System.out.println("聚类中心文件存储在:"+local_save_file);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
			bw.close();writer.close();
		}
	}
	/**
	 * 获取中心向量数据的id
	 * @param filename
	 * @param densityDelta
	 * @param distanceDelta
	 * @return
	 * @throws IOException
	 */
	public List<Integer> getIDs(String filename,double densityDelta,double distanceDelta) throws IOException{
		 List<Integer> list = new ArrayList<Integer>();
		  FileReader reader = new FileReader(filename);
          BufferedReader br = new BufferedReader(reader);
         
          String str = null;
          double density;
          double distance ;
          int id;
          while((str = br.readLine()) != null) {
        	  //multi:1315544.599043301,id:719,density:342.0,distance:3846.6216346295355
        	  
        	  density = Double.parseDouble(findStrByFromTo(str,"density:",","));
        	  distance = Double .parseDouble(findStrByFromTo(str,"distance:",null));
        	  if(density>=densityDelta&& distance>=distanceDelta){
        		  id = Integer.parseInt(findStrByFromTo(str,"id:",","));
        		  list.add(id);
        	  }
          }
         
          br.close();
          reader.close();
          return list;
	}
	/**
	 * 根据起始和结束字符串返回中间字符
	 * @param str
	 * @param fromStr
	 * @param endStr
	 * @return
	 */
	private String findStrByFromTo(String str,String fromStr,String endStr){
		int from =0;
		int end =0;
		from = str.indexOf(fromStr);
		if(endStr==null){
			return str.substring(from+fromStr.length());
		}else{	
			end = str.indexOf(endStr,from+1);
			return str.substring(from+fromStr.length(), end);
		}
	}
}
