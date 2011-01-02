package com.nearinfinity.blur.thrift;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.thrift.TException;

import com.nearinfinity.blur.manager.IndexServer;
import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Hit;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Iface;
import com.nearinfinity.blur.utils.BlurConstants;

public abstract class BlurAdminServer implements Iface, BlurConstants {
	
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
    

    @Override
    public List<String> controllerServerList() throws BlurException, TException {
        return getIndexServer().getControllerServerList();
    }

    @Override
    public List<String> shardServerList() throws BlurException, TException {
        return getIndexServer().getShardServerList();
    }
    
    public abstract IndexServer getIndexServer();
}
