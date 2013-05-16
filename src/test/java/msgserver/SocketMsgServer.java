package msgserver;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

public class SocketMsgServer {

	public static Logger logger = Logger.getLogger(SocketMsgServer.class);

	static BufferedReader is = null;
	static PrintWriter os = null;

	public static void main(String[] args) {
		ServerSocket server = null;
		try {
			try {
				server = new ServerSocket(15025);
				// 创建一个ServerSocket在端口15025监听客户请求
			} catch (Exception e) {
				logger.info("can not listen to:" + e);
				// 出错，打印出错信息
			}

			try {
				while (true) {
					Socket socket = null;
					socket = server.accept();

					if (socket != null) {
						MsgCreater mc = new MsgCreater(socket);
						new Thread(mc).start();
					}
					logger.info("----->socket" + socket.toString());
					// 使用accept()阻塞等待客户请求，有客户请求到来则产生一个Socket对象，并继续执行
				}
			} catch (Exception e) {
				logger.info("Error." + e);
			}
		} catch (Exception e) {
			try {
				server.close();
			} catch (IOException e1) {
				e1.printStackTrace();

			} // 关闭ServerSocket
			logger.info("Socket Error -------------" + e);
			// 出错，打印出错信息
		}
	}
}
