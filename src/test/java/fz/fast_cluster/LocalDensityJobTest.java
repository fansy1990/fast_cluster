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
public class LocalDensityJobTest {
	public static void main(String[] args) throws Exception {
		String[] ar={
				"hdfs://node101:/user/root/iris_in/iris_data.csv",
				"hdfs://node101:/user/root/iris_out",
				"0.5",
				",",
				"gaussian"
		};
		
		ToolRunner.run(HUtils.getConf(), new LocalDensityJob(), ar);
	}
}
