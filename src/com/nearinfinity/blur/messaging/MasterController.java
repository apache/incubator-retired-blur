package com.nearinfinity.blur.messaging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MasterController extends BlurServer {
	
	public static interface MessageJoiner {
		byte[] join(Collection<byte[]> responses);
	}
	
	private static Log LOG = LogFactory.getLog(MasterController.class);
	
	public static class MCMessageHandler implements MessageHandler {
		
		private Map<String, BlockingQueue<BlurClient>> clientCache = new ConcurrentHashMap<String, BlockingQueue<BlurClient>>();
		private ExecutorService service;
		private Collection<String> clientStrs;
		private MessageJoiner joiner;
		
		public MCMessageHandler(final int port, Collection<String> clientStrs, MessageJoiner joiner) throws Exception {
			this.service = Executors.newCachedThreadPool(new ThreadFactory() {
				private AtomicInteger count = new AtomicInteger();
				@Override
				public Thread newThread(Runnable r) {
					Thread thread = new Thread(r);
					thread.setName("Master-Controller-Client-" + port + "-" + count.incrementAndGet());
					return thread;
				}
			});
			this.joiner = joiner;
			this.clientStrs = clientStrs;
			for (String client : clientStrs) {
				clientCache.put(client, getClients(4,client));
			}
		}

		private BlockingQueue<BlurClient> getClients(int size, String client) throws InterruptedException, IOException {
			ArrayBlockingQueue<BlurClient> queue = new ArrayBlockingQueue<BlurClient>(size);
			for (int i = 0; i < size; i++) {
				queue.put(new BlurClient(client));
			}
			return queue;
		}

		@Override
		public byte[] handleMessage(byte[] message) {
			Collection<Future<byte[]>> futures = new ArrayList<Future<byte[]>>();
			for (String client : clientStrs) {
				futures.add(call(client, message));
			}
			
			Collection<byte[]> responses = new ArrayList<byte[]>();
			for (Future<byte[]> future : futures) {
				try {
					responses.add(future.get());
				} catch (InterruptedException e) {
					LOG.error("unknown error",e);
				} catch (ExecutionException e) {
					LOG.error("Error from connection may want to remove clients",e);
				}
			}
			return joiner.join(responses);
		}

		private Future<byte[]> call(final String clientStr, final byte[] message) {
			return service.submit(new Callable<byte[]>() {
				@Override
				public byte[] call() throws Exception {
					BlockingQueue<BlurClient> blockingQueue = clientCache.get(clientStr);
					BlurClient client = blockingQueue.take();
					try {
						return client.send(message);
					}  finally {
						blockingQueue.put(client);
					}
				}
			});
		}
	}
	
	public static void main(String[] args) throws Exception {
		new MasterController(3000,Arrays.asList(
				"localhost/3001",
				"localhost/3002",
				"localhost/3003",
				"localhost/3004",
				"localhost/3005",
				"localhost/3006",
				"localhost/3007",
				"localhost/3008"
				)).start();
	}

	public MasterController(int port, Collection<String> clients) throws Exception {
		super(port, new MCMessageHandler(port,clients,new MessageJoiner() {
			@Override
			public byte[] join(Collection<byte[]> responses) {
				try {
					ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
					for (byte[] response : responses) {
						if (response != null) {
							outputStream.write(response);
						}
					}
					outputStream.close();
					return outputStream.toByteArray();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}));
	}

}
