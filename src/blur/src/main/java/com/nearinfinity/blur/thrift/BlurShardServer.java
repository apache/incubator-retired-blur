package com.nearinfinity.blur.thrift;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.IndexManager.TableManager;
import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.ScoreType;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurConfiguration;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.mele.Mele;

public class BlurShardServer extends BlurAdminServer implements BlurConstants {

	private static final Log LOG = LogFactory.getLog(BlurShardServer.class);
	private IndexManager indexManager;
	
	public BlurShardServer(Mele mele, BlurConfiguration configuration) throws IOException, BlurException {
		super(mele,configuration);
		indexManager = new IndexManager(mele, new TableManager() {
			@Override
			public boolean isTableEnabled(String table) {
				try {
					TableDescriptor describe = describe(table);
					if (describe == null) {
						return false;
					}
					return describe.isEnabled;
				} catch (Exception e) {
				    LOG.error("Unknown error while trying to check if table [" + table +
				    		"] is enabled.",e);
					return false;
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
        try {
            HitsIterable hitsIterable = indexManager.search(table, query, superQueryOn, type, postSuperFilter, preSuperFilter, minimumNumberOfHits, maxQueryTime);
            return convertToHits(hitsIterable,start,fetch,minimumNumberOfHits);
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
