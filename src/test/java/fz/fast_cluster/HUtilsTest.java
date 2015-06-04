/**
 * 
 */
package fz.fast_cluster;

import fz.utils.HUtils;

/**
 * @author fansy
 * @date 2015-6-3
 */
public class HUtilsTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String localFile= "D:/ds.txt";
		String url ="hdfs://node101:8020/user/root/iris_deltadistance/part-r-00000";
		
//		HUtils.readHDFSFile(url, localFile);
		
		int iter_i = 8;
		String localFilePre ="D:/iteration_";
		getAllCenters(iter_i,localFilePre);
//		
		
	}

	public static void getAllCenters(int iter_i,String localFilePre){
		String localFile =null;
		for(int i=0;i<=iter_i;i++){
			localFile=localFilePre+i+".txt";
			
			HUtils.readCenterToLocal(i, localFile);
			System.out.println(new java.util.Date()+" localFile:"+localFile+" finished writing!");
		}
	}
}
