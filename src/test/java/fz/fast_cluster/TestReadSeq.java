/**
 * 
 */
package fz.fast_cluster;

import java.io.IOException;

/**
 * @author fansy
 * @date 2015-7-24
 */
public class TestReadSeq {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		String string = "hdfs://node101:/user/root/center_out/part-r-00000";
		String local_save_file ="d:/test.dat";
		FindWriteCenter.resolveToLocal(string, local_save_file);
	}

}
