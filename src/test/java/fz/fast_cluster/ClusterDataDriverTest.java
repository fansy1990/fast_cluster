/**
 * 
 */
package fz.fast_cluster;

import org.jfree.util.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fz.draw.DrawPic;
import fz.utils.HUtils;

/**
 * @author fansy
 * @date 2015-6-2
 */
public class ClusterDataDriverTest {

	/**
	 * @param args
	 */
	private static Logger log = LoggerFactory.getLogger(ClusterDataDriverTest.class);
	static ClusterDataDriver cdd = new ClusterDataDriver();
	public static void main(String[] args) throws Exception {
		
//		String input ="hdfs://node101:8020/user/root/path_based.csv";
//		String dc ="1.4";
//		String k ="3";
		
		
		String input ="hdfs://node101:8020/user/root/flume.csv";
		String dc ="4.0";
		String k="2";
		
		String splitter =",";
		String method ="gaussian";
		
		
//		test_findCenter(input,dc,splitter,method);
		
		
//		String input ="hdfs://node101:8020/user/root/path_based.csv";
		
		test_clusterData(input,k,splitter);
		
	}
	
	
	public static void test_clusterData(String input,String k,String splitter) throws Exception{
//		String input ="hdfs://node101:8020/user/root/iris_in/iris_data.csv";
//		String input ="hdfs://node101:8020/user/root/path_based.csv";
//		String splitter =",";
//		String k ="3";
		clusterData(input,k,splitter);
	
	}
	
	public static void test_findCenter(String input,String dc,String splitter,
			String method) throws Exception{
//		String input ="hdfs://node101:8020/user/root/iris_in/iris_data.csv";
//		String input ="hdfs://node101:8020/user/root/path_based.csv";
//		String dc ="1.4";
//		String splitter =",";
//		String method ="gaussian";
	
		findCenter(input,dc,splitter,method);// use this to find Center 
		
		// use this to draw the pic 
		DrawPic.drawPic(HUtils.getHDFSPath(HUtils.DELTADISTANCEOUTPUT)+"/part-r-00000");

	}
	
	
	public static void findCenter(String input,String dc,String splitter,String method) throws Exception{
		int ret =cdd.runLocalDensityJob(input, dc, splitter, method);
		if(ret!=0){
			Log.info("LocalDensityJob failed!");
			System.exit(-1);
		}
		
		ret = cdd.runDeltaDistanceJob();
		if(ret!=0){
			Log.info("DeltaDistanceJob failed!");
			System.exit(-1);
		}
		
	}
	
	public static void clusterData(String input,String k,String splitter) throws Exception{
		
		// run prepare cluster
		int ret = cdd.runPreCluster(input, HUtils.getHDFSPath(HUtils.FIRSTUNCLUSTEREDPATH), splitter);
		if(ret!=0){
			Log.info("PreClusterJob failed!");
			System.exit(-1);
		}
		// write center 
		double[] dcs =cdd.findAndWriteCenter(Integer.parseInt(k));
		log.info("dc_min:{},dc_max:{}",new Object[]{dcs[0],dcs[1]});
		ret = cdd.runClusterData( k, dcs);
		if(ret!=0){
			Log.info("ClusterDataJob failed!");
			System.exit(-1);
		}
	}

}
