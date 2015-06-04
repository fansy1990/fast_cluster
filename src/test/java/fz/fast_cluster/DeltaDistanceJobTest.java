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
public class DeltaDistanceJobTest {

	public static void main(String[] args) throws Exception {
		
	
	String[] ar={
			"hdfs://node101:/user/root/iris_out/part-r-00000",
			"hdfs://node101:/user/root/iris_out00"
	};
	
	ToolRunner.run(HUtils.getConf(), new DeltaDistanceJob(), ar);
	}
}
