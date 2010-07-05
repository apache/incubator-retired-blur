package com.nearinfinity.blur.store.dao.cassandra;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;


public class CassandraClient {

	private static Log LOG = LogFactory.getLog(CassandraClient.class);

	public static void main(String[] args) {
		process(new Operation<String>() {
			@Override
			public String execute(Client client) throws InvalidRequestException, UnavailableException,
					TimedOutException, TException {
				return null;
			}
		});
	}

	public final static Void VOID = new Void();

	private static final int TIMEOUT_PAUSE = 100;

	private static ClientPool pool;
	
	public void addSetupInfo(String hostnames, int port) {
		
	}

	public static <T> T process(Operation<T> operation) {
		Client client = pool.getClient();
		int timeouts = 0;
		try {
			while (true) {
				try {
					return operation.execute(client);
				} catch (InvalidRequestException e) {
					throw new RuntimeException(e);
				} catch (TimedOutException e) {
					timeoutPause(++timeouts);
				} catch (Exception e) {
					client = failOver(client);
					timeoutPause(++timeouts);
				}
			}
		} finally {
			pool.putClient(client);
		}
	}

	private static void timeoutPause(int i) {
		try {
			int time = i * TIMEOUT_PAUSE;
			LOG.info("Timeout pause [" + time + "]");
			Thread.sleep(time);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

	}

	private static Client failOver(Client client) {
		
		return null;
	}
	
	public static class Void {
		private Void() {
		}
	}

	public interface Operation<T> {
		T execute(Client client) throws InvalidRequestException, UnavailableException, TimedOutException, TException;
	}

}
