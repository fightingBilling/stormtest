package com.wankun.stormtest.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import com.wankun.stormtest.bolt.InsertFlowBolt;
import com.wankun.stormtest.bolt.InsertSigBolt;
import com.wankun.stormtest.bolt.WriteFileBolt;
import com.wankun.stormtest.spout.SocketMngSpout;

public class MyTopo {
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		
		int spoutnum = 1;
		int bolt1num = 1;
		int bolt2num = 1;
		int bolt3num = 1;
		if(args.length>1) spoutnum = Integer.parseInt(args[1]);
		if(args.length>2) bolt1num = Integer.parseInt(args[2]);
		if(args.length>3) bolt2num = Integer.parseInt(args[3]);
		if(args.length>4) bolt3num = Integer.parseInt(args[4]);
		
		builder.setSpout("spout", new SocketMngSpout(), spoutnum);
		builder.setBolt("insertflow", new InsertFlowBolt(), bolt1num).shuffleGrouping("spout");
		builder.setBolt("insertsig", new InsertSigBolt(), bolt2num).shuffleGrouping("spout");
		builder.setBolt("writefile", new WriteFileBolt(), bolt3num).shuffleGrouping("insertflow").shuffleGrouping("insertsig");
		
		Config conf = new Config();
		conf.setDebug(false);
		
		if (args != null && args.length > 0) {
			conf.setNumWorkers(16);
			
			StormSubmitter.submitTopology(args[0], conf,builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("socket-voltdb", conf, builder.createTopology());
			Thread.sleep(60000);
			cluster.shutdown();
		}
	}
}
