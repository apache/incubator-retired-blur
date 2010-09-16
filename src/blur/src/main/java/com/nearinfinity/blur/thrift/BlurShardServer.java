package com.nearinfinity.blur.thrift;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.IndexManager.TableManager;
import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.mele.Mele;

public class BlurShardServer extends BlurAdminServer implements BlurConstants {

	private static final Log LOG = LogFactory.getLog(BlurShardServer.class);
	private IndexManager indexManager;
	
	public BlurShardServer(Mele mele) throws IOException, BlurException {
		super(mele);
		indexManager = new IndexManager(mele, new TableManager() {
			
			@Override
			public boolean isTableEnabled(String table) {
				try {
					TableDescriptor describe = describe(table);
					System.out.println("isTableEnabled " + table + " = " + describe);
					if (describe == null) {
						return false;
					}
					return describe.isEnabled;
				} catch (Exception e) {
				    LOG.error("Uknown error while trying to check if table [" + table +
				    		"] is enabled.",e);
					throw new RuntimeException(e);
				}
			}
			
			@Override
			public String getAnalyzerDefinitions(String table) {
				try {
					TableDescriptor describe = describe(table);
					if (describe == null) {
						return "";
					}
					return describe.analyzerDef;
				} catch (Exception e) {
					throw new RuntimeException();
				}
			}
		});
	}

	@Override
	public Hits search(String table, String query, boolean superQueryOn, ScoreType type, String postSuperFilter, String preSuperFilter, 
			final long start, final int fetch, long minimumNumberOfHits, long maxQueryTime) throws BlurException, TException {
	    Hits hits = new Hits();
		HitsIterable hitsIterable = indexManager.search(table, query, superQueryOn, type, postSuperFilter, preSuperFilter, minimumNumberOfHits, maxQueryTime);
		hits.setTotalHits(hitsIterable.getTotalHits());
		hits.setShardInfo(hitsIterable.getShardInfo());
		if (minimumNumberOfHits > 0) {
    		hitsIterable.skipTo(start);
    		int count = 0;
    		Iterator<Hit> iterator = hitsIterable.iterator();
    		while (iterator.hasNext() && count < fetch) {
    		    hits.addToHits(iterator.next());
    		    count++;
    		}
		}
		return hits;
	}
	
	@Override
	protected NODE_TYPE getType() {
		return NODE_TYPE.SHARD;
	}

	@Override
	public Row fetchRow(String table, String id) throws BlurException, TException, MissingShardException {
		return indexManager.fetchRow(table,id);
	}

	@Override
	public void appendRow(String table, Row row) throws BlurException, TException {
		indexManager.appendRow(table,row);
	}

	@Override
	public void removeRow(String table, String id) throws BlurException, TException {
		indexManager.removeRow(table,id);
	}

	@Override
	public void replaceRow(String table, Row row) throws BlurException, TException, MissingShardException {
		indexManager.replaceRow(table,row);
	}

    public void close() throws InterruptedException {
        indexManager.close();
    }
}
