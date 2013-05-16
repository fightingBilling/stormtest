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

public class InsertFlowBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 5448304713513697863L;
	public static Logger logger = Logger.getLogger(InsertFlowBolt.class);
	org.voltdb.client.Client myApp = null;

    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	if(null==tuple.getString(0)||!"1".equals(tuple.getString(0))){
			return ;
		}
		else
		{
			String keyid = tuple.getString(1);
			String url = tuple.getString(2);
			// insert Flow 
			try {
				if(myApp==null)
				{
					myApp = ClientFactory.createClient();
					myApp.createConnection(TestUtil.VOLTDB_IP);
				}
				String msisdn = getMsisdn(myApp, keyid);
				if(null!=msisdn && !"".equals(msisdn))
					myApp.callProcedure("Updret", url, msisdn);
			} catch (Exception e) {
				e.printStackTrace();
			}
			collector.emit(new Values("1",keyid,url));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("type","keyid","url"));			
	}
	
	public String  getMsisdn(org.voltdb.client.Client myApp,String keyid)
    {
    	String msisdn ="";
    	try {
			ClientResponse response = myApp.callProcedure("Selsig", keyid);
			if (response.getStatus() != ClientResponse.SUCCESS) {
				return msisdn;
			}
			VoltTable results[] = response.getResults();
			if (results.length == 0 || results[0].getRowCount() != 1) {
				return msisdn;
			}
			VoltTable resultTable = results[0];
			VoltTableRow row = resultTable.fetchRow(0);
			msisdn = row.getString("msisdn");
		} catch (Exception e) {
			e.printStackTrace();
		}
    	return msisdn;
    }
}