/**
 * 
 */
package fz.draw;


/**
 * @author fansy
 * @date 2015-7-23
 */
public class DrawPicTest {

	/**
	 * @param args
	 * @throws Exception 
	 * @throws NumberFormatException 
	 */
	public static void main(String[] args) throws NumberFormatException, Exception {
		String[] arg=new String[]{
				"hdfs://node101:8020/user/root/sort_out/part-r-00000",
				"d:/test.png",
				"500"
		};
		DrawPic.main(arg);
	}

}
