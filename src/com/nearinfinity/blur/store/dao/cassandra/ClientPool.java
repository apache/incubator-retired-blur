package com.nearinfinity.blur.store.dao.cassandra;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class ClientPool {

	private BlockingQueue<Client> clients;

	public ClientPool(int size, String hostName, int port) throws Exception {
		clients = new ArrayBlockingQueue<Client>(size);
		for (int i = 0; i < size; i++) {
			TTransport tr = new TSocket(hostName, port);
			TProtocol proto = new TBinaryProtocol(tr);
			Client client = new Client(proto);
			tr.open();
			clients.add(client);
		}

	}

	public Client getClient() {
		try {
			return clients.take();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public void putClient(Client client) {
		try {
			clients.put(client);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		Collection<Client> closing = new HashSet<Client>();
		clients.drainTo(closing);
		for (Client client : closing) {
			client.getInputProtocol().getTransport().close();
			client.getOutputProtocol().getTransport().close();
		}
	}

}
