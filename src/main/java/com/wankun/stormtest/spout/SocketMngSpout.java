package com.wankun.stormtest.spout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.voltdb.client.ClientFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.wankun.stormtest.util.TestUtil;

public class SocketMngSpout extends BaseRichSpout {
	public static Logger logger = Logger.getLogger(SocketMngSpout.class);
	private SpoutOutputCollector _collector;
	private static LinkedBlockingQueue<String> _queue = new LinkedBlockingQueue<String>();
	Socket socket = null;
	org.voltdb.client.Client myApp = null;
	Listener listener = null;
	
	public static SimpleDateFormat df = new SimpleDateFormat("ddHHmmss");

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		try {

			if (socket == null) {
				socket = new Socket(TestUtil.MSG_SERVER_IP,
						TestUtil.MSG_SERVER_PORT);
				socket.setKeepAlive(true);
				socket.setSoTimeout(10000);
			}

			listener = new Listener(socket, _queue);
			new Thread(listener).start();

			logger.info("Listener 开始接收Socket数据......");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void nextTuple() {
		try {
			String line = _queue.take();
			if (null == line) {
				logger.info("xxxx the line is null");
				return;
			}
			if (myApp == null) {
				myApp = ClientFactory.createClient();
				myApp.createConnection(TestUtil.VOLTDB_IP);
			}
			long time = System.currentTimeMillis();
			if (line.startsWith("1")) {
				String[] strs = line.split(":");
				myApp.callProcedure("Insflow", strs[1], strs[2]);
				_collector.emit(new Values("1", strs[1], strs[2], "", time));

			} else if (line.startsWith("2")) {
				String[] strs = line.split(":");
				myApp.callProcedure("Inssig", strs[1], strs[2]);
				_collector.emit(new Values("2", strs[1], "", strs[2], time));
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("type", "keyid", "url", "msisdn", "time"));
	}

	@Override
	public void close() {
		try {
			if (socket != null)
				socket.close(); // 关闭Socket
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

class Listener implements Serializable, Runnable {
	public static Logger logger = Logger.getLogger(Listener.class);

	Socket socket = null;
	BufferedReader is = null;
	LinkedBlockingQueue<String> queue;
	public long recnum = 0;

	private static final long serialVersionUID = -4072148698328990872L;

	public Listener(Socket socket, LinkedBlockingQueue<String> queue) {
		this.socket = socket;
		this.queue = queue;
	}

	public void run() {
		try {
			synchronized (new Object()) {
				recnum++;
				if(recnum%2000==0)
					logger.warn("接收到数据："+recnum);
			}
			is = new BufferedReader(new InputStreamReader(socket.getInputStream()));

			logger.info("begin listener");
			while (true) {
				String line = is.readLine();
				// logger.info("ServerMngStream 接收到消息" + line);
//				if(null!=line)
					queue.put(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			try {
				is.close(); // 关闭Socket输入流
				socket.close(); // 关闭Socket
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
