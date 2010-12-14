package com.nearinfinity.blur.thrift;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import com.nearinfinity.blur.manager.IndexServer;
import com.nearinfinity.blur.manager.IndexServer.TABLE_STATUS;
import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Iface;
import com.nearinfinity.blur.utils.BlurConstants;

public abstract class BlurAdminServer implements Iface, BlurConstants {
	
	public enum NODE_TYPE {
		CONTROLLER,
		SHARD
	}
	
	private IndexServer indexServer;
	
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
	public List<String> tableList() throws BlurException, TException {
	    return indexServer.getTableList();
	}
	
	protected abstract NODE_TYPE getType();
	
    public static Hits convertToHits(HitsIterable hitsIterable, long start, int fetch, long minimumNumberOfHits) {
        Hits hits = new Hits();
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
        if (hits.hits == null) {
            hits.hits = new ArrayList<Hit>();
        }
        return hits;
    }
    
    public IndexServer getIndexServer() {
        return indexServer;
    }

    public void setIndexServer(IndexServer indexServer) {
        this.indexServer = indexServer;
    }

    @Override
    public List<String> controllerServerList() throws BlurException, TException {
        return null;
    }

    @Override
    public List<String> shardServerList() throws BlurException, TException {
        return null;
    }
}
