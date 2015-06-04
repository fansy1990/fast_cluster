/**
 * 
 */
package fz.fast_cluster;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fz.utils.HUtils;

/**
 * @author fansy
 * @date 2015-6-2
 */
public class ClusterDataDriver {
	private Logger log = LoggerFactory.getLogger(ClusterDataDriver.class);
	public int runLocalDensityJob(String input,String dc,String splitter,String method) throws Exception{
		String [] args ={
				input,
				HUtils.getHDFSPath(HUtils.LOCALDENSITYOUTPUT),
				dc,
				splitter,
				method
		};
		return ToolRunner.run(HUtils.getConf(), new LocalDensityJob(),args );
	}
	
	public int runDeltaDistanceJob() throws Exception{
		String[] ar={
				HUtils.getHDFSPath(HUtils.LOCALDENSITYOUTPUT)+"/part-r-00000",
				HUtils.getHDFSPath(HUtils.DELTADISTANCEOUTPUT)
		};
		return ToolRunner.run(HUtils.getConf(), new DeltaDistanceJob(), ar);
	}
	
	public double[] findAndWriteCenter(int k) throws IOException{
//		FileSystem.get(HUtils.getConf()).delete(new Path(
//				HUtils.getHDFSPath(HUtils.CENTERPATHPREFIX)+"*"	),true);
//		log.info("Deleted file :{}",HUtils.getHDFSPath(HUtils.CENTERPATHPREFIX)+"*");
		return HUtils.getCenterVector(HUtils.getHDFSPath(HUtils.DELTADISTANCEOUTPUT)+
				"/part-r-00000", HUtils.getHDFSPath(HUtils.FIRSTCENTERPATH), k);
	}

	public int runPreCluster(String input,String output,String splitter) throws Exception{
		FileSystem.get(HUtils.getConf()).delete(new Path(
		HUtils.getHDFSPath(HUtils.CENTERPATH)	),true);
		log.info("Deleted file :{}",HUtils.getHDFSPath(HUtils.CENTERPATH));		
		
		int ret =-1;
		String[] ar ={
				input,
				output,
				splitter
		};
		
		ret = ToolRunner.run(HUtils.getConf(), new ToSeqJob(), ar);
		if(ret!=0){
			log.info("TopSeqJob failed, with input {}",new Object[]{input});
			System.exit(-1);
		}	
		
		return ret;
		
	}
	public int runClusterData(String k,double[] dcs) throws Exception{
		int iter_i=0;
		int ret=0;
		double dc =dcs[0];
		do{
			log.info("this is the {} iteration,with dc:{}",new Object[]{iter_i,dc});
			
			String[] ar={
					HUtils.getHDFSPath(HUtils.CENTERPATHPREFIX)+iter_i+"/unclustered/part-m-00000",
					HUtils.getHDFSPath(HUtils.CENTERPATHPREFIX)+(iter_i+1),//output
					//HUtils.getHDFSPath(HUtils.CENTERPATHPREFIX)+iter_i+"/clustered/part-m-00000",//center file
					k,
					String.valueOf(dc),
					String.valueOf((iter_i+1))
			};
			
			ret = ToolRunner.run(HUtils.getConf(), new ClusterDataJob(), ar);
			if(ret!=0){
				log.info("ClusterDataJob failed, with iteration {}",new Object[]{iter_i});
				System.exit(-1);
			}	
			iter_i++;
			dc+=dc*0.1;// every time increase 10%
			if(dc>dcs[1]){
				dc=dcs[1];
			}
		}while(shouldRunNextPhrase(iter_i));
		if(ret==0){
			log.info("All cluster Job finished with iteration {}",new Object[]{iter_i});
		}
		return ret;
	}

	/**
	 * @param iter_i
	 * @return
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	private boolean shouldRunNextPhrase(int iter_i) throws IllegalArgumentException, IOException {
		String before = HUtils.getHDFSPath(HUtils.CENTERPATHPREFIX)+(iter_i-1)+"/unclustered/part-m-00000";
		String next = HUtils.getHDFSPath(HUtils.CENTERPATHPREFIX)+iter_i+"/unclustered/part-m-00000";
		FileSystem fs =FileSystem.get(HUtils.getConf());
		if(!fs.exists(new Path(next))){
			return false;
		}
		long beforeSize = fs.getFileStatus(new Path(before)).getLen();
		long nextSize = fs.getFileStatus(new Path(next)).getLen();
		if(beforeSize==nextSize){
			return false;
		}
		return true;
	}
	
}
