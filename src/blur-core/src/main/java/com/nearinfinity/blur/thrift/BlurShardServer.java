package com.nearinfinity.blur.thrift;

import static com.nearinfinity.blur.utils.BlurUtil.getParametersList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;

import com.nearinfinity.blur.manager.IndexManager;
import com.nearinfinity.blur.manager.IndexServer;
import com.nearinfinity.blur.manager.IndexServer.TABLE_STATUS;
import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.FacetQuery;
import com.nearinfinity.blur.thrift.generated.FacetResult;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.SearchQueryStatus;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.BlurUtil;

public class BlurShardServer extends BlurAdminServer implements BlurConstants {

	private static final Log LOG = LogFactory.getLog(BlurShardServer.class);
	private IndexManager indexManager;
	private IndexServer indexServer;
	
    @Override
	public Hits search(String table, SearchQuery searchQuery) throws BlurException, TException {
        try {
            HitsIterable hitsIterable = indexManager.search(table, searchQuery);
            return convertToHits(hitsIterable,searchQuery.start,searchQuery.fetch,searchQuery.minimumNumberOfHits);
        } catch (BlurException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Unknown error during search of [" +
                    getParametersList("table",table, "searchquery", searchQuery) + "]",e);
            throw new BlurException(e.getMessage());
        }
	}
	
	@Override
	public FetchResult fetchRow(String table, Selector selector) throws BlurException, TException {
        try {
            FetchResult fetchResult = new FetchResult();
            indexManager.fetchRow(table,selector, fetchResult);
            return fetchResult;
        } catch (BlurException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get fetch row [" + getParametersList("table",table,"selector",selector) + "]",e);
            throw new BlurException(e.getMessage());
        }
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
    public byte[] fetchRowBinary(String table, Selector selector) throws BlurException, TException {
        try {
            return BlurUtil.toBytes(fetchRow(table,selector));
        } catch (BlurException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get fetch row binary [" + getParametersList("table",table,"selector",selector) + "]",e);
            throw new BlurException(e.getMessage());
        }
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

    public BlurShardServer setIndexManager(IndexManager indexManager) {
        this.indexManager = indexManager;
        return this;
    }

    @Override
    public FacetResult facetSearch(String table, FacetQuery facetQuery) throws BlurException, TException {
        FacetResult facetResult = new FacetResult().setFacetQuery(facetQuery);
        try {
            indexManager.facetSearch(table, facetQuery, facetResult);
            return facetResult;
        } catch (BlurException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get record frequency for [" + getParametersList("table",table,"facetQuery",facetQuery) + "]",e);
            throw new BlurException(e.getMessage());
        }
    }

    @Override
    public long recordFrequency(String table, String columnFamily, String columnName, String value) throws BlurException, TException {
        try {
            return indexManager.recordFrequency(table,columnFamily,columnName,value);
        } catch (BlurException e) {
            throw e;
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get record frequency for [" + getParametersList("table",table,"columnFamily",columnFamily,"columnName",columnName,"value",value) + "]",e);
            throw new BlurException(e.getMessage());
        }
    }

    @Override
    public Schema schema(String table) throws BlurException, TException {
        try {
            return indexManager.schema(table);
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get schema for table [" + getParametersList("table") + "]",e);
            throw new BlurException(e.getMessage());
        }
    }

    @Override
    public List<String> terms(String table, String columnFamily, String columnName, String startWith, short size) throws BlurException, TException {
        try {
            return indexManager.terms(table,columnFamily,columnName,startWith,size);
        } catch (Exception e) {
            LOG.error("Unknown error while trying to get terms list for [" + getParametersList("table",table,"columnFamily",columnFamily,"columnName",columnName,"startWith",startWith,"size",size) + "]",e);
            throw new BlurException(e.getMessage());
        }
    }
    
    @Override
    public List<String> tableList() throws BlurException, TException {
        return indexServer.getTableList();
    }
    
    @Override
    public TableDescriptor describe(String table) throws BlurException, TException {
        Map<String, String> shardServerLayout = shardServerLayout(table);
        TableDescriptor descriptor = new TableDescriptor();
        descriptor.analyzerDef = indexServer.getAnalyzer(table).toString();
        descriptor.shardNames = new ArrayList<String>(shardServerLayout.keySet());
        descriptor.isEnabled = isTableEnabled(table);
        return descriptor;
    }

    public boolean isTableEnabled(String table) {
        TABLE_STATUS tableStatus = indexServer.getTableStatus(table);
        if (tableStatus == TABLE_STATUS.ENABLED) {
            return true;
        } else {
            return false;
        }
    }
    
    @Override
    public IndexServer getIndexServer() {
        return indexServer;
    }

    public BlurShardServer setIndexServer(IndexServer indexServer) {
        this.indexServer = indexServer;
        return this;
    }
}
