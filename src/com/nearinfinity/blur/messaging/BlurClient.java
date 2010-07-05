package com.nearinfinity.blur.messaging;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class BlurClient {
//	private static Log LOG = LogFactory.getLog(BlurClient.class);
	
	private int port;
	private String hostName;
	private Socket socket;
	private DataInputStream inputStream;
	private DataOutputStream outputStream;
	
	public static void main(String[] args) throws Exception {
		BlurClient client = new BlurClient("localhost", 3000);
		byte[] bs = "hello world".getBytes();
		long totalTime = 0;
		long total = 0;
		int count = 0;
		int max = 100;
		byte[] result = new byte[]{};
		while (true) {
			if (count >= max) {
				double avg = (totalTime / (double) total) / 1000000.0d;
				System.out.println("avg [" + avg + "] ms " + new String(result));
				count = 0;
				total = 0;
				totalTime = 0;
			}
			long s = System.nanoTime();
			result = client.send(bs);
			long e = System.nanoTime();
			totalTime += (e-s);
			total++;
			count++;
			Thread.sleep(1);
		}
	}

	public BlurClient(String hostName, int port) throws IOException {
		this.hostName = hostName;
		this.port = port;
		connect();
	}
	
	private void connect() throws UnknownHostException, IOException {
		socket = new Socket(this.hostName, this.port);
		inputStream = new DataInputStream(socket.getInputStream());
		outputStream = new DataOutputStream(socket.getOutputStream());
	}

	public BlurClient(String connectionStr) throws IOException {
		String[] split = connectionStr.split("\\/");
		this.hostName = split[0];
		this.port = Integer.parseInt(split[1]);
		connect();
	}

	public byte[] send(byte[] message) throws Exception {
		if (socket.isConnected()) {
			outputStream.writeInt(message.length);
			outputStream.write(message);
			outputStream.flush();
				
			int length = inputStream.readInt();
			byte[] buffer = new byte[length];
			inputStream.readFully(buffer);
			return buffer;
		}
		return null;
	}

}
