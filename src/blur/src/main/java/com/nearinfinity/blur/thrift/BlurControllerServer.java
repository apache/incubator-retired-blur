package com.nearinfinity.blur.thrift;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.mele.Mele;

public class BlurControllerServer extends BlurAdminServer implements BlurConstants {
	
	private static final Log LOG = LogFactory.getLog(BlurControllerServer.class);

	public BlurControllerServer(Mele mele) throws IOException {
		super(mele);
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
	protected NODE_TYPE getType() {
		return NODE_TYPE.CONTROLLER;
	}

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