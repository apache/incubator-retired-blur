package com.nearinfinity.blur.messaging;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.nearinfinity.blur.search.SearchMessageHandler;

public class BlurServer {
	
	private static Log LOG = LogFactory.getLog(BlurServer.class);
	
	private int port;
	private Thread listenerThread;
	private ServerSocket serverSocket;
	private ExecutorService executor;
	private MessageHandler handler;
	private volatile boolean shutdown = false;

	public static void main(String[] args) throws Exception {
		new BlurServer(Integer.parseInt(args[0]), new SearchMessageHandler(args)).start();
	}
	
	public BlurServer(final int port, MessageHandler handler) {
		this.port = port;
		this.handler = handler;
		this.executor = Executors.newCachedThreadPool(new ThreadFactory() {
			private AtomicInteger count = new AtomicInteger();
			@Override
			public Thread newThread(Runnable r) {
				Thread thread = new Thread(r);
				thread.setName("Blur-Message-Handler-" + port + "-" + count.incrementAndGet());
				return thread;
			}
		});
	}
	
	public synchronized void start() throws Exception {
		if (listenerThread != null) {
			return;
		}
		serverSocket = new ServerSocket(port);
		LOG.info("Server has started on port [" + port + "]");
		listenerThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (!shutdown) {
					try {
						handleConnection(serverSocket.accept());
					} catch (IOException e) {
						LOG.error("unknown error",e);
						throw new RuntimeException(e);
					}
				}
			}
		});
		listenerThread.setName("Blur-Server-Socket-Listener-"+port);
		listenerThread.start();
	}
	
	protected void handleConnection(final Socket socket) {
		executor.submit(new Runnable() {
			@Override
			public void run() {
				try {
					DataInputStream inputStream = new DataInputStream(socket.getInputStream());
					DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
					while (socket.isConnected() && !shutdown) {
						byte[] message = readMessage(inputStream);
						byte[] response = handler.handleMessage(message);
						sendResponse(response,outputStream);
					}
					socket.close();
				} catch (Exception e) {
					if (!socket.isClosed()) {
						try {
							socket.close();
						} catch (IOException ex) {
						}
					}
				}
			}
		});
	}

	public void sendResponse(byte[] response, DataOutputStream outputStream) throws IOException {
		outputStream.writeInt(response.length);
		outputStream.write(response);
		outputStream.flush();
	}

	public static byte[] readMessage(DataInputStream inputStream) throws IOException {
		int length = inputStream.readInt();
		byte[] buffer = new byte[length];
		inputStream.readFully(buffer);
		return buffer;
	}

	public void stop() throws Exception {
		shutdown = true;
		serverSocket.close();
		listenerThread.interrupt();
		executor.shutdownNow();
	}

}
