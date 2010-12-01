package com.nearinfinity.blur.thrift;

import static com.nearinfinity.blur.utils.BlurUtil.getParametersList;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.EventStoppedExecutionException;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.SearchQueryStatus;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.utils.BlurConstants;

public class BlurShardServer extends BlurAdminServer implements BlurConstants {

	private static final Log LOG = LogFactory.getLog(BlurShardServer.class);
	private IndexManager indexManager;
	
    @Override
	public Hits search(String table, SearchQuery searchQuery) throws BlurException, TException {
        try {
            HitsIterable hitsIterable = indexManager.search(table, searchQuery);
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
    public void cancelSearch(long uuid) throws BlurException, TException {
        try {
            indexManager.cancelSearch(uuid);
        } catch (Exception e) {
            LOG.error("Unknown error while trying to cancel search [" + getParametersList("uuid",uuid) + "]",e);
            throw new BlurException(e.getMessage());
        }
    }

    @Override
    public List<SearchQueryStatus> currentSearches(String table) throws BlurException, TException {
        try {
            return indexManager.currentSearches(table);
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get current search status [" + getParametersList("table",table) + "]",e);
            throw new BlurException(e.getMessage());
        }
    }
    
    @Override
    public byte[] fetchRowBinary(String table, Selector selector) throws BlurException, MissingShardException,
            EventStoppedExecutionException, TException {
        throw new RuntimeException("not implemented");
    }

    public void close() throws InterruptedException {
        indexManager.close();
    }
    
    @Override
    public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
        throw new RuntimeException("not implemented");
    }
    
    public IndexManager getIndexManager() {
        return indexManager;
    }

    public void setIndexManager(IndexManager indexManager) {
        this.indexManager = indexManager;
    }
}
