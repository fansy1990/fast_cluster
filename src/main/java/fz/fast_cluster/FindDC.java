/**
 * 
 */
package fz.fast_cluster;

import fz.utils.HUtils;

/**
 * @author fansy
 * @date 2015-7-22
 */
public class FindDC {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		 if (args.length !=2) {
		      System.err.println("Usage: fz.fast_cluster.FindDC <in> <records>");
		      System.exit(1);
		 }
		 
		 HUtils.readDistanceAndFindDC(args[0], Long.parseLong(args[1]));
		 
	}

}
