/*
 * Copyright (C) 2011 Near Infinity Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nearinfinity.blur.thrift;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.thrift.TException;

import com.nearinfinity.blur.manager.IndexServer;
import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurResult;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.utils.BlurUtil;

public abstract class BlurBaseServer implements Iface {
	
    public static BlurResults convertToHits(HitsIterable hitsIterable, long start, int fetch, long minimumNumberOfResults, AtomicLongArray facetCounts) {
        BlurResults hits = new BlurResults();
        hits.setTotalResults(hitsIterable.getTotalHits());
        hits.setShardInfo(hitsIterable.getShardInfo());
        if (minimumNumberOfResults > 0) {
            hitsIterable.skipTo(start);
            int count = 0;
            Iterator<BlurResult> iterator = hitsIterable.iterator();
            while (iterator.hasNext() && count < fetch) {
                hits.addToResults(iterator.next());
                count++;
            }
        }
        if (hits.results == null) {
            hits.results = new ArrayList<BlurResult>();
        }
        if (facetCounts != null) {
            hits.facetCounts = BlurUtil.toList(facetCounts);
        }
        return hits;
    }
    

    @Override
    public List<String> controllerServerList() throws BlurException, TException {
        return getIndexServer().getControllerServerList();
    }

    @Override
    public List<String> shardServerList() throws BlurException, TException {
        return getIndexServer().getOnlineShardServers();
    }
    
    public abstract IndexServer getIndexServer();
}
