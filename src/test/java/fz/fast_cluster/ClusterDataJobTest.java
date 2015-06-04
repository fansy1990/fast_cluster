/**
 * 
 */
package fz.fast_cluster;

import org.apache.hadoop.util.ToolRunner;

import fz.utils.HUtils;

/**
 * @author fansy
 * @date 2015-6-2
 */
public class ClusterDataJobTest {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		//<in> <out> <centerPath> <centerK> <splitter>
		String[] ar={
				"hdfs://node101:/user/root/iris_in/iris_data.csv",
				"hdfs://node101:/user/root/iris_clustered",
				"hdfs://node101:/user/root/iris_center/center00.dat",
				"3",
				",",
				"0.5"
		};
		
		ToolRunner.run(HUtils.getConf(), new ClusterDataJob(), ar);
	}

}
