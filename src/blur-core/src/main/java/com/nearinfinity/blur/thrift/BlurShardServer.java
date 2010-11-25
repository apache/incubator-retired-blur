package com.nearinfinity.blur.thrift;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import com.nearinfinity.blur.manager.IndexManager;
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
import com.nearinfinity.blur.utils.BlurConfiguration;
import com.nearinfinity.blur.utils.BlurConstants;

public class BlurShardServer extends BlurAdminServer implements BlurConstants {

	private static final Log LOG = LogFactory.getLog(BlurShardServer.class);
	private IndexManager indexManager;
	
	public BlurShardServer(MetaData metaData, BlurConfiguration configuration) throws IOException, BlurException {
		super(metaData,configuration);
	}

    @Override
	public Hits search(String table, SearchQuery searchQuery) throws BlurException, TException {
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
	public FetchResult fetchRow(String table, Selector selector) throws BlurException, TException, MissingShardException {
	    FetchResult fetchResult = new FetchResult();
	    indexManager.fetchRow(table,selector, fetchResult);
        return fetchResult;
	}

	@Override
	public void removeRow(String table, String id) throws BlurException, TException {
	    throw new BlurException("not implemented");
	}

	@Override
	public void replaceRow(String table, Row row) throws BlurException, TException, MissingShardException {
	    throw new BlurException("not implemented");
	}
	
    @Override
    public void cancelSearch(long userUuid) throws BlurException, TException {
        throw new BlurException("not implemented");
    }

    @Override
    public void batchUpdate(String batchId, String table, Map<String, String> shardsToUris) throws BlurException,
            MissingShardException, TException {
        throw new BlurException("not implemented");
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

    @Override
    public List<SearchQuery> currentSearches(String userUuid) throws BlurException, TException {
        throw new BlurException("not implemented");
    }
    
    public IndexManager getIndexManager() {
        return indexManager;
    }

    public void setIndexManager(IndexManager indexManager) {
        this.indexManager = indexManager;
    }
}
