package com.nearinfinity.blur.lucene.store.dao.cassandra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class CassandraExecutor {
	
	public static interface Command<T> {
		T execute(Client client) throws Exception;
	}

	private static final long BACK_OFF_TIME = 250;
	private static int port;
	private static BlockingQueue<Client> clients;
	private static Random random = new Random();
	private static List<String> hostNames = new ArrayList<String>();
	
	public static void setup(int p, int clientNumber, String... hosts) {
		port = p;
		hostNames = Arrays.asList(hosts);
		discoverOthersHosts();
		clients = new ArrayBlockingQueue<Client>(clientNumber);
		for (int i = 0; i < clientNumber; i++) {
			try {
				clients.put(newClient());
			} catch (TTransportException e) {
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	private static void discoverOthersHosts() {
		Set<String> realHostNames = new TreeSet<String>();
		for (String host : hostNames) {
			Client client = null;
			try {
				client = newClient(host);
				for (String keyspace : client.describe_keyspaces()) {
					realHostNames.addAll(getHosts(client.describe_ring(keyspace)));
				}
			} catch (TTransportException e) {
				//
			} catch (TException e) {
				//
			} catch (InvalidRequestException e) {
				//
			} finally {
				close(client);
			}
		}
		if (!realHostNames.isEmpty()) {
			hostNames = new ArrayList<String>(realHostNames);
		}
	}

	private static Collection<String> getHosts(List<TokenRange> describeRing) {
		Collection<String> hosts = new HashSet<String>();
		for (TokenRange range : describeRing) {
			hosts.addAll(range.getEndpoints());
		}
		return hosts;
	}

	public static <T> T execute(Command<T> command) throws IOException {
		int retryCount = 0;
		while(true) {
			try {
				Client client = clients.take();
				try {
					synchronized (client) {
						return command.execute(client);
					}
				} catch (TimedOutException e) {
					System.out.println("time out");
					if (retryCount == 0) {
						sleep(BACK_OFF_TIME);
					} else if (retryCount < 10) {
						close(client);
						discoverOthersHosts();
						client = newClient();
					} else {
						throw e;
					}
					retryCount++;
				} finally {
					clients.put(client);
				}
			} catch (Exception e) {
				throw new IOException(e);
			}
		}
	}
	
	private static Client newClient(String host) throws TTransportException {
		TTransport tr = new TSocket(host, port);
		TProtocol proto = new TBinaryProtocol(tr);
		Client client = new Client(proto);
		tr.open();
		return client;
	}
	
	private static Client newClient() throws TTransportException {
		return newClient(getRandomHostName());
	}

	private static String getRandomHostName() {
		return hostNames.get(random.nextInt(hostNames.size()));
	}

	private static void close(Client client) {
		client.getInputProtocol().getTransport().close();
		client.getOutputProtocol().getTransport().close();
	}

	private static void sleep(long ms) throws InterruptedException {
		Thread.sleep(ms);
	}
}
