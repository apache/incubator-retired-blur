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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.SearchQueryStatus;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.BlurSearch.Iface;

public class DoNothingServer implements Iface {

    @Override
    public TableDescriptor describe(String table) throws BlurException, TException {
        return null;
    }

    @Override
    public List<String> tableList() throws BlurException, TException {
        return Arrays.asList("donothing");
    }

    @Override
    public List<String> controllerServerList() throws BlurException, TException {
        return null;
    }

    @Override
    public List<String> shardServerList() throws BlurException, TException {
        return null;
    }

    @Override
    public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
        return null;
    }

    @Override
    public Hits search(String table, SearchQuery searchQuery) throws BlurException, TException {
        return null;
    }

    @Override
    public void cancelSearch(long providedUuid) throws BlurException, TException {
        
    }

    @Override
    public FetchResult fetchRow(String table, Selector selector) throws BlurException, TException {
        return null;
    }

    @Override
    public List<SearchQueryStatus> currentSearches(String arg0) throws BlurException, TException {
        return null;
    }

    @Override
    public long recordFrequency(String arg0, String arg1, String arg2, String arg3) throws BlurException, TException {
        return 0;
    }

    @Override
    public Schema schema(String arg0) throws BlurException, TException {
        return null;
    }

    @Override
    public List<String> terms(String arg0, String arg1, String arg2, String arg3, short arg4) throws BlurException,
            TException {
        return null;
    }
}
