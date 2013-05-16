package msgserver;


import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Logger;

public class MsgCreater implements Runnable {

	public static Logger logger = Logger.getLogger(MsgCreater.class);
	public Socket socket;
	PrintWriter os =null;
	public long counter = 0l;

	public MsgCreater(Socket socket) {
		this.socket = socket;
	}

	public void run() {
		InputStream inputStream = this.getClass().getClassLoader()
				.getResourceAsStream("MsgCreater.properties");
		Properties p = new Properties();
		try {
			p.load(inputStream);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		long msgnums=Long.parseLong(p.getProperty("msgnums"));
		long rate=Long.parseLong(p.getProperty("rate"));
		System.out.println(msgnums+":"+rate);
		
		try {
			os = new PrintWriter(new BufferedWriter(
					new OutputStreamWriter(socket.getOutputStream())), true);
			String line1;
			String line2;
			Random random = new Random();
			String[] mysource = { "http//new.baidu", "http//new.google",
					"http//new.sina", "http//new.sohu" };
			String keyid;
			int cycle = (int) (msgnums / rate);
			long begtime;
			long endtime;
			long tmp = 0;
			long allbegtime = System.currentTimeMillis();
			SimpleDateFormat df =new SimpleDateFormat("ddHHmmss");
			for (int i = 0; i < cycle; i++) {
				begtime = System.currentTimeMillis(); // 毫秒
				// 每次发送两条记录
				for (int j = 0; j < rate / 2; ++j) {
					tmp++;
					keyid = Thread.currentThread().getId()
							+ df.format(new Date())
							+ String.format("%05d", tmp%100000); 
					line1 = "1:" + keyid + ":"
							+ mysource[random.nextInt(4)];
					line2 = "2:" + keyid + ":" + 15850600000l
							+ random.nextInt(6);
					os.println(line1);
					os.println(line2);
					counter += 2;
					// logger.info("--->thread.id: "+Thread.currentThread().getId()+" recored:"+tmp+"socket:"+socket.toString());
					/*if (tmp % 10000 == 0) {
						System.out.printf("--->thread.id:%d recored:%d socket:%s curetime:&d \n",
										Thread.currentThread().getId(),
										tmp, socket.toString(),
										System.currentTimeMillis());
					}*/
					os.flush();
				}
				endtime = System.currentTimeMillis(); // 毫秒
				Thread.sleep(endtime > begtime + 1000 ? 0 : begtime + 1000
						- endtime);
			}
			long allendtime = System.currentTimeMillis();
			System.out.println(allbegtime);
			System.out.println(allendtime);
			if(allendtime - allbegtime>0)
			{
				System.out.println("Thread:"+Thread.currentThread().getId()+
							" send msg num:"+msgnums+",rate:"+rate+" ,realrate:"+1000* msgnums / (allendtime - allbegtime));
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			os.close();
			try {
				socket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
