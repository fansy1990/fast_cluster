/**
 * 
 */
package fz.draw;

import java.awt.Font;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.util.ReflectionUtils;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.StandardChartTheme;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import fz.fast_cluster.keytype.CustomDoubleWritable;
import fz.fast_cluster.keytype.IntDoublePairWritable;
import fz.utils.HUtils;

/**
 * 画决策图
 * @author fansy
 * @date 2015-7-23
 */
public class DrawPic {

	/**
	 * @param args
	 * @throws Exception 
	 * @throws NumberFormatException 
	 */
	public static void main(String[] args) throws NumberFormatException, Exception {
		
		if (args.length != 3) {
			System.err.println("Usage: fz.draw.DrawPic <in> <out> <numRecords>");
			System.exit(3);
		}
		drawPic(args[0],args[1],Integer.parseInt(args[2]));
	}
/**
 * 画图
 * @param url
 * @param out
 * @param records 获取文件的前面records条记录来画图
 * @throws Exception 
 */
	public static void drawPic(String url,String out,int records) throws Exception {
		try {
		XYSeries xyseries = getXYSeries(url,records);
		XYSeriesCollection xyseriescollection = new XYSeriesCollection(); // 再用XYSeriesCollection添加入XYSeries
																			// 对象
		xyseriescollection.addSeries(xyseries);

		// 创建主题样式
		StandardChartTheme standardChartTheme = new StandardChartTheme("CN");
		// 设置标题字体
		standardChartTheme.setExtraLargeFont(new Font("隶书", Font.BOLD, 20));
		// 设置图例的字体
		standardChartTheme.setRegularFont(new Font("宋书", Font.PLAIN, 15));
		// 设置轴向的字体
		standardChartTheme.setLargeFont(new Font("宋书", Font.PLAIN, 15));
		// 应用主题样式
		ChartFactory.setChartTheme(standardChartTheme);
		// JFreeChart chart=ChartFactory.createXYAreaChart("xyPoit", "点的标号",
		// "出现次数", xyseriescollection, PlotOrientation.VERTICAL, true, false,
		// false);
		JFreeChart chart = ChartFactory.createScatterPlot("决策图", "点密度",
				"点距离", xyseriescollection, PlotOrientation.VERTICAL, true,
				false, false);
		String file=out;
		
			ChartUtilities.saveChartAsPNG(new File(file), chart,
					500, 500);
			System.out.println(new java.util.Date()+": finished drawing the pic in "+file);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(new java.util.Date()+":failed drawing the pic !");
		}
		
	}
/**
 * 画图
 * @param xyseries
 * @param out
 * @throws Exception
 */
	public static void drawPic(XYSeries xyseries,String out) throws Exception {
		try {
		XYSeriesCollection xyseriescollection = new XYSeriesCollection(); // 再用XYSeriesCollection添加入XYSeries
																			// 对象
		xyseriescollection.addSeries(xyseries);

		// 创建主题样式
		StandardChartTheme standardChartTheme = new StandardChartTheme("CN");
		// 设置标题字体
		standardChartTheme.setExtraLargeFont(new Font("隶书", Font.BOLD, 20));
		// 设置图例的字体
		standardChartTheme.setRegularFont(new Font("宋书", Font.PLAIN, 15));
		// 设置轴向的字体
		standardChartTheme.setLargeFont(new Font("宋书", Font.PLAIN, 15));
		// 应用主题样式
		ChartFactory.setChartTheme(standardChartTheme);
		// JFreeChart chart=ChartFactory.createXYAreaChart("xyPoit", "点的标号",
		// "出现次数", xyseriescollection, PlotOrientation.VERTICAL, true, false,
		// false);
		JFreeChart chart = ChartFactory.createScatterPlot("决策图", "点密度",
				"点距离", xyseriescollection, PlotOrientation.VERTICAL, true,
				false, false);
		String file=out;
		
			ChartUtilities.saveChartAsPNG(new File(file), chart,
					500, 500);
			System.out.println(new java.util.Date()+": finished drawing the pic in "+file);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(new java.util.Date()+":failed drawing the pic !");
		}
		
	}
	
	/**
	 * 获取数据点
	 * @param url
	 * @return
	 * @throws Exception 
	 */
	public static XYSeries getXYSeries(String url,int records) throws Exception{
		XYSeries xyseries = new XYSeries("");
		Path path = new Path(url);
		Configuration conf = HUtils.getConf();
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(conf, Reader.file(path),
					Reader.bufferSize(4096), Reader.start(0));
			CustomDoubleWritable dkey = (CustomDoubleWritable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			IntDoublePairWritable dvalue = (IntDoublePairWritable) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);

			while (reader.next(dkey, dvalue)&&records-->0) {// 循环读取文件
//				<density_i*min_distancd_j> <density_i,min_distance_j,i>
//				 * 		DoubleWritable,  IntDoublePairWritable
				xyseries.add(dvalue.getFirst(), dvalue.getSecond());
				System.out.println("multi:"+dkey.get()+",id:"+dvalue.getThird()+
						",density:"+dvalue.getFirst()+",distance:"+dvalue.getSecond());
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			IOUtils.closeStream(reader);
		}

		return xyseries;
	}
	
	public static XYSeries getXYSeries(String url,String out ,int records) throws Exception{
		XYSeries xyseries = new XYSeries("");
		Path path = new Path(url);
		Configuration conf = HUtils.getConf();
		SequenceFile.Reader reader = null;
		FileWriter writer = new FileWriter(out);
        BufferedWriter bw = new BufferedWriter(writer);
        
		try {
			reader = new SequenceFile.Reader(conf, Reader.file(path),
					Reader.bufferSize(4096), Reader.start(0));
			CustomDoubleWritable dkey = (CustomDoubleWritable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			IntDoublePairWritable dvalue = (IntDoublePairWritable) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);
			// 第一行数据
			reader.next(dkey,dvalue);
			bw.write("multi:"+dkey.get()+",id:"+dvalue.getThird()+
					",density:"+dvalue.getFirst()+",distance:"+dvalue.getSecond());
			bw.newLine();
			while (reader.next(dkey, dvalue)&&records-->0) {// 循环读取文件

				xyseries.add(dvalue.getFirst(), dvalue.getSecond());
				bw.write("multi:"+dkey.get()+",id:"+dvalue.getThird()+
						",density:"+dvalue.getFirst()+",distance:"+dvalue.getSecond());
				bw.newLine();
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			IOUtils.closeStream(reader);
			bw.close();
	        writer.close();
		}

		return xyseries;
	}


}
