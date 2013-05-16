package com.wankun.stormtest.bolt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class WriteFileBolt extends BaseBasicBolt {
	public static Logger logger = Logger.getLogger(WriteFileBolt.class);
	long cnttime = System.currentTimeMillis();
	long curtime;
	int  tcount = 0;
	public static long allnums = 0;
	File f;
	private static Object lock =new Object();

	public void execute(Tuple input, BasicOutputCollector collector) {
		curtime = System.currentTimeMillis();
		synchronized (lock) {
			tcount++;
			allnums++;
		}
		BufferedWriter output=null;
		try {
			if(curtime >=cnttime +1000) {	
				f = new File("/usr/local/storm/logs/rate-"+Thread.currentThread().getId()+".txt");
				try {
					if (!f.exists())
						f.createNewFile();
				} catch (IOException e) {
					e.printStackTrace();
				}
				output = new BufferedWriter(new FileWriter(f,true));
				output.write("WriteFileBolt rate:"+ tcount/((float)(curtime-cnttime)/1000)+"  nums:"+tcount+" times:"+(int)(curtime-cnttime)+"ms 记录总条数："+allnums+" current time:"+System.currentTimeMillis()+"\n");
				output.flush();
				cnttime = System.currentTimeMillis();
				tcount=0;				
			}
		} catch (Exception e2) {
			e2.printStackTrace();
		}finally{
			try {
				if(null!=output)
					output.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			f=null;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}