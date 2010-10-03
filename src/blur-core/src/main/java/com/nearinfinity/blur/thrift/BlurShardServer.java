package com.nearinfinity.blur.thrift;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.IndexManager.TableManager;
import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.metadata.MetaData;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.EventStoppedExecutionException;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurConfiguration;
import com.nearinfinity.blur.utils.BlurConstants;

public class BlurShardServer extends BlurAdminServer implements BlurConstants {

	private static final Log LOG = LogFactory.getLog(BlurShardServer.class);
	private IndexManager indexManager;
	
	public BlurShardServer(MetaData metaData, BlurConfiguration configuration) throws IOException, BlurException {
		super(metaData,configuration);
		indexManager = new IndexManager(metaData.getMele(), new TableManager() {
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
	public Hits searchInternal(String table, SearchQuery searchQuery) throws BlurException, TException {
        try {
            HitsIterable hitsIterable = indexManager.search(table, searchQuery.queryStr, 
                    searchQuery.superQueryOn, searchQuery.type, searchQuery.postSuperFilter, 
                    searchQuery.preSuperFilter, searchQuery.minimumNumberOfHits, searchQuery.maxQueryTime);
            return convertToHits(hitsIterable,searchQuery.start,searchQuery.fetch,searchQuery.minimumNumberOfHits);
        } catch (Exception e) {
            LOG.error("Unknown error during search of [" +
                    getParametersList("table",table, "searchquery", searchQuery) + "]",e);
            throw new BlurException(e.getMessage());
        }
	}
	
    @Override
	protected NODE_TYPE getType() {
		return NODE_TYPE.SHARD;
	}

	@Override
	public FetchResult fetchRowInternal(String table, Selector selector) throws BlurException, TException, MissingShardException {
	    FetchResult fetchResult = new FetchResult();
	    fetchResult.table = table;
	    fetchResult.id = selector.id;
	    fetchResult.row = indexManager.fetchRow(table,selector);
	    fetchResult.exists = fetchResult.row == null ? false : true;
        return fetchResult;
	}

	@Override
	public void appendRowInternal(String table, Row row) throws BlurException, TException {
		indexManager.appendRow(table,row);
	}

	@Override
	public void removeRowInternal(String table, String id) throws BlurException, TException {
		indexManager.removeRow(table,id);
	}

	@Override
	public void replaceRowInternal(String table, Row row) throws BlurException, TException, MissingShardException {
		indexManager.replaceRow(table,row);
	}
	
    @Override
    public void cancelSearchInternal(long providedUuid) throws BlurException, TException {
        throw new BlurException("not implemented");
    }

    @Override
    public void batchUpdate(String batchId, String table, Map<String, String> shardsToUris) throws BlurException,
            MissingShardException, TException {
        
    }
    
    @Override
    public void appendRowBinary(String table, String id, byte[] rowBytes) throws BlurException, MissingShardException, EventStoppedExecutionException, TException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public byte[] fetchRowBinary(String table, String id, byte[] selector) throws BlurException, MissingShardException,
            EventStoppedExecutionException, TException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void replaceRowBinary(String table, String id, byte[] rowBytes) throws BlurException, MissingShardException,
            EventStoppedExecutionException, TException {
        throw new RuntimeException("not implemented");
    }

    public void close() throws InterruptedException {
        indexManager.close();
    }
}
