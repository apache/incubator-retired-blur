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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.thrift.TException;

import com.nearinfinity.blur.manager.IndexServer;
import com.nearinfinity.blur.manager.results.BlurResultIterable;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurResult;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;
import com.nearinfinity.blur.utils.BlurUtil;

public abstract class BlurBaseServer implements Iface {
	
    public static BlurResults convertToHits(BlurResultIterable hitsIterable, BlurQuery query, AtomicLongArray facetCounts, ExecutorService executor, Selector selector, final Iface iface, final String table) throws InterruptedException, ExecutionException {
        BlurResults results = new BlurResults();
        results.setTotalResults(hitsIterable.getTotalResults());
        results.setShardInfo(hitsIterable.getShardInfo());
        if (query.minimumNumberOfResults > 0) {
            hitsIterable.skipTo(query.start);
            int count = 0;
            Iterator<BlurResult> iterator = hitsIterable.iterator();
            while (iterator.hasNext() && count < query.fetch) {
                results.addToResults(iterator.next());
                count++;
            }
        }
        if (results.results == null) {
            results.results = new ArrayList<BlurResult>();
        }
        if (facetCounts != null) {
            results.facetCounts = BlurUtil.toList(facetCounts);
        }
        if (selector != null) {
            List<Future<FetchResult>> futures = new ArrayList<Future<FetchResult>>();
            for (int i = 0; i < results.results.size(); i++) {
                BlurResult result = results.results.get(i);
                final Selector s = new Selector(selector);
                s.setLocationId(result.locationId);
                futures.add(executor.submit(new Callable<FetchResult>() {
                    @Override
                    public FetchResult call() throws Exception {
                        return iface.fetchRow(table, s);
                    }
                }));
            }
            for (int i = 0; i < results.results.size(); i++) {
                Future<FetchResult> future = futures.get(i);
                BlurResult result = results.results.get(i);
                result.setFetchResult(future.get());
            }
        }
        return results;
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
