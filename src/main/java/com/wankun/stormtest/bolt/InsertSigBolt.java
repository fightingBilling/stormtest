package com.wankun.stormtest.bolt;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.wankun.stormtest.util.TestUtil;

public class InsertSigBolt  extends BaseBasicBolt {
	private static final long serialVersionUID = -9070942866205009664L;
	public static Logger logger = Logger.getLogger(InsertSigBolt.class);
	org.voltdb.client.Client myApp = null;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("type","keyid","msisdn","cnt"));		
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		if(null==tuple.getString(0)||!"2".equals(tuple.getString(0)))
		{
			return ;
		}
		else
		{
			String keyid = tuple.getString(1);
			String msisdn = tuple.getString(3);
			Long cnt = (long) 0;
			// insert Sig
			try {
				if(myApp==null)
				{
					myApp = ClientFactory.createClient();
					myApp.createConnection(TestUtil.VOLTDB_IP);
				}
				ClientResponse response = myApp.callProcedure("Selret", msisdn,100);
				if (response.getStatus() != ClientResponse.SUCCESS) {
					return;
				}
				VoltTable results[] = response.getResults();
				if (results.length == 0 || results[0].getRowCount() != 1) {
					return;
				}
				VoltTable resultTable = results[0];
				VoltTableRow row = resultTable.fetchRow(0);
				cnt = row.getLong("cnt");
			} catch (Exception e) {
				e.printStackTrace();
			}
			collector.emit(new Values("2",keyid,msisdn,cnt));
		}
	}
}
