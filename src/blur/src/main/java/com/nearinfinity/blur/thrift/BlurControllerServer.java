package com.nearinfinity.blur.thrift;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;
import com.nearinfinity.mele.store.util.ZkUtils;

public class BlurControllerServer extends BlurAdminServer implements Watcher, BlurConstants {
	
	private static final Log LOG = LogFactory.getLog(BlurControllerServer.class);
	private List<String> nodeList = new ArrayList<String>();

	public BlurControllerServer() throws IOException {
		super();
		createBlurClients();
	}

	@Override
	public Hits search(final String table, final String query, final boolean superQueryOn, final ScoreType type, final String postSuperFilter, final String preSuperFilter, 
			final long start, final int fetch, final long minimumNumberOfHits, final long maxQueryTime) throws BlurException, TException {
//		try {
//			return ForkJoin.execute(executor, clients.values(), new ParallelCall<String,Hits>() {
//				@Override
//				public Hits call(Blur.Client client) throws Exception {
//					return client.search(table, query, superQueryOn, type, postSuperFilter, preSuperFilter, start, 
//							fetch, minimumNumberOfHits, maxQueryTime);
//				}
//			}).merge(new HitsMerger());
//		} catch (Exception e) {
//			LOG.error("Unknown error",e);
//			throw new BlurException(e.getMessage());
//		}
	    
	    return null;
	}

	@Override
	public void process(WatchedEvent event) {
		createBlurClients();
	}
	
	@Override
	protected NODE_TYPE getType() {
		return NODE_TYPE.CONTROLLER;
	}
	
	private synchronized void createBlurClients() {
//		try {
//			Map<String,Blur.Client> newClients = new TreeMap<String,Blur.Client>();
//			String path = blurNodePath + "/" + NODE_TYPE.NODE.name();
//			ZkUtils.mkNodesStr(zk, path);
//			List<String> children = zk.getChildren(path, this);
//			for (String hostname : children) {
//				Client client = clients.get(hostname);
//				if (client == null) {
//					Client newClient = createClient(hostname);
//					if (newClient == null) {
//						newClients.put(hostname,newClient);
//					}
//				} else {
//					newClients.put(hostname,client);
//				}
//			}
//			clients = newClients;
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
	}

//	private Client createClient(String hostname) {
//		TTransport tr = new TSocket(hostname, nodePort);
//		TProtocol proto = new TBinaryProtocol(tr);
//		Client client = new Client(proto);
//		try {
//			tr.open();
//		} catch (TTransportException e) {
//			LOG.error("Error opening client to host " + hostname);
//			return null;
//		}
//		return client;
//	}

	@Override
	public void appendRow(String table, Row row) throws BlurException,
			TException {
		throw new RuntimeException();
	}

	@Override
	public Row fetchRow(String table, String id) throws BlurException,
			TException {
		throw new RuntimeException();
	}

	@Override
	public void removeRow(String table, String id) throws BlurException,
			TException {
		throw new RuntimeException();
	}

	@Override
	public void replaceRow(String table, Row row) throws BlurException,
			TException {
		throw new RuntimeException();
	}
}