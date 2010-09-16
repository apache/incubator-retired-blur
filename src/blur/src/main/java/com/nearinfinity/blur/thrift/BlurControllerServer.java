package com.nearinfinity.blur.thrift;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.manager.hits.HitsIterableBlurClient;
import com.nearinfinity.blur.manager.hits.HitsIterableMerger;
import com.nearinfinity.blur.thrift.BlurClientManager.Command;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;
import com.nearinfinity.mele.Mele;

public class BlurControllerServer extends BlurAdminServer implements BlurConstants {
	
	private static final Log LOG = LogFactory.getLog(BlurControllerServer.class);

	public BlurControllerServer(Mele mele) throws IOException {
		super(mele);
	}

	@Override
	public Hits search(final String table, final String query, final boolean superQueryOn, final ScoreType type, final String postSuperFilter, final String preSuperFilter, 
			final long start, final int fetch, final long minimumNumberOfHits, final long maxQueryTime) throws BlurException, TException {
		try {
			HitsIterable hitsIterable = ForkJoin.execute(executor, shardServerList(), new ParallelCall<String,HitsIterable>() {
				@Override
				public HitsIterable call(final String hostnamePort) throws Exception {
					return BlurClientManager.execute(hostnamePort, new Command<HitsIterable>() {
		                @Override
		                public HitsIterable call(Client client) throws Exception {
		                    return new HitsIterableBlurClient(client,hostnamePort,table,query,
		                            superQueryOn,type,postSuperFilter,preSuperFilter,
		                            minimumNumberOfHits,maxQueryTime);
		                }
		            });
				}
			}).merge(new HitsIterableMerger(minimumNumberOfHits));
			return convertToHits(hitsIterable, start, fetch, minimumNumberOfHits);
		} catch (Exception e) {
			LOG.error("Unknown error during search of [" +
					getParametersList("table",table, "query", query, "superQueryOn", superQueryOn,
					        "type", type, "postSuperFilter", postSuperFilter, "preSuperFilter", preSuperFilter, 
				            "start", start, "fetch", fetch, "minimumNumberOfHits", minimumNumberOfHits, 
				            "maxQueryTime", maxQueryTime) + "]",e);
			throw new BlurException(e.getMessage());
		}
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