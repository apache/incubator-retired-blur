package com.nearinfinity.blur.messaging;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BlurRpcClient {
	private static Log LOG = LogFactory.getLog(BlurRpcClient.class);
	
	private int port;
	private String hostName;
	private Socket socket;
	private DataInputStream inputStream;
	private DataOutputStream outputStream;

	public BlurRpcClient(String hostName, int port) throws IOException {
		this.hostName = hostName;
		this.port = port;
		connect();
	}
	
	private void connect() throws UnknownHostException, IOException {
		socket = new Socket(this.hostName, this.port);
		inputStream = new DataInputStream(socket.getInputStream());
		outputStream = new DataOutputStream(socket.getOutputStream());
	}

	public BlurRpcClient(String connectionStr) throws IOException {
		String[] split = connectionStr.split("\\/");
		this.hostName = split[0];
		this.port = Integer.parseInt(split[1]);
		connect();
	}

	public byte[] send(byte[] message) throws IOException {
		if (socket.isConnected()) {
			outputStream.writeInt(message.length);
			outputStream.write(message);
			outputStream.flush();
				
			int length = inputStream.readInt();
			byte[] buffer = new byte[length];
			inputStream.readFully(buffer);
			return buffer;
		}
		LOG.error("Socket is not connected.");
		return null;
	}

}
