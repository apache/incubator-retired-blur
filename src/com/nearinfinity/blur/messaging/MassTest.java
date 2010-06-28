package com.nearinfinity.blur.messaging;

import java.util.ArrayList;
import java.util.List;

public class MassTest {

	public static void main(String[] args) throws Exception {
		
		int port = 3001;
		String hostname = "localhost/";
		List<String> hosts = new ArrayList<String>();
		List<BlurServer> servers = new ArrayList<BlurServer>();
		for (int i = 0; i < 30; i++) {
			servers.add(startServer(port+i));
			hosts.add(hostname + (port+i));
		}
		
		new MasterController(3000, hosts).start();
		
//		Thread.sleep(10000);
//		
//		System.out.println("Stoping server...");
//		
//		servers.get(0).stop();

	}

	private static BlurServer startServer(int port) throws Exception {
		BlurServer server = new BlurServer(port, new MessageHandler() {
			@Override
			public byte[] handleMessage(byte[] message) {
				return message;
			}
		});
		server.start();
		return server;
	}
	

}
