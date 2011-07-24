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

package com.nearinfinity.blur.thrift.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import com.nearinfinity.blur.thrift.commands.BlurCommand;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurQuery;
import com.nearinfinity.blur.thrift.generated.BlurQueryStatus;
import com.nearinfinity.blur.thrift.generated.BlurResults;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.RowMutation;
import com.nearinfinity.blur.thrift.generated.Schema;
import com.nearinfinity.blur.thrift.generated.Selector;
import com.nearinfinity.blur.thrift.generated.TableDescriptor;
import com.nearinfinity.blur.thrift.generated.TableStats;
import com.nearinfinity.blur.thrift.generated.Transaction;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.thrift.generated.Blur.Iface;

public class BlurClientEmbedded extends BlurClient {
    
    private Map<String,Client> nodes = new HashMap<String,Client>();

    @Override
    public <T> T execute(String node, BlurCommand<T> command) throws Exception {
        Client client = nodes.get(node);
        return command.call(client);
    }

    public Map<String, Client> getNodes() {
        return nodes;
    }

    public BlurClientEmbedded setNodes(Map<String, Client> nodes) {
        this.nodes = nodes;
        return this;
    }
    
    public BlurClientEmbedded putNode(String node, Client client) {
        nodes.put(node, client);
        return this;
    }
    
    public BlurClientEmbedded putNode(String node, Iface face) {
        return putNode(node, new EmbeddedClient(face));
    }
    
    public static class EmbeddedClient extends Client {

        private Iface face;

        public EmbeddedClient(Iface face) {
            super(null);
            this.face = face;
        }

        @Override
        public void cancelQuery(String table, long uuid) throws BlurException, TException {
            face.cancelQuery(table, uuid);
        }

        @Override
        public List<String> controllerServerList() throws BlurException, TException {
            return face.controllerServerList();
        }

        @Override
        public List<BlurQueryStatus> currentQueries(String table) throws BlurException, TException {
            return face.currentQueries(table);
        }

        @Override
        public TableDescriptor describe(String table) throws BlurException, TException {
            return face.describe(table);
        }

        @Override
        public FetchResult fetchRow(String table, Selector selector) throws BlurException, TException {
            return face.fetchRow(table, selector);
        }

        @Override
        public long recordFrequency(String table, String columnFamily, String columnName, String value)
                throws BlurException, TException {
            return face.recordFrequency(table, columnFamily, columnName, value);
        }

        @Override
        public Schema schema(String table) throws BlurException, TException {
            return face.schema(table);
        }

        @Override
        public BlurResults query(String table, BlurQuery blurQuery) throws BlurException, TException {
            return face.query(table, blurQuery);
        }

        @Override
        public Map<String, String> shardServerLayout(String table) throws BlurException, TException {
            return face.shardServerLayout(table);
        }

        @Override
        public List<String> shardServerList() throws BlurException, TException {
            return face.shardServerList();
        }

        @Override
        public List<String> tableList() throws BlurException, TException {
            return face.tableList();
        }

        @Override
        public List<String> terms(String table, String columnFamily, String columnName, String startWith, short size)
                throws BlurException, TException {
            return face.terms(table, columnFamily, columnName, startWith, size);
        }

        @Override
        public void mutate(Transaction transaction, List<RowMutation> mutations) throws BlurException, TException {
            face.mutate(transaction, mutations);
        }

        public void createTable(String table, TableDescriptor tableDescriptor) throws BlurException, TException {
            face.createTable(table, tableDescriptor);
        }

        public void disableTable(String table) throws BlurException, TException {
            face.disableTable(table);
        }

        public void enableTable(String table) throws BlurException, TException {
            face.enableTable(table);
        }

        public TableStats getTableStats(String table) throws BlurException, TException {
            return face.getTableStats(table);
        }

        public void mutateAbort(String table, Transaction transaction) throws BlurException, TException {
            face.mutateAbort(transaction);
        }

        public void mutateCommit(String table, Transaction transaction) throws BlurException, TException {
            face.mutateCommit(transaction);
        }

        public Transaction mutateCreateTransaction(String table) throws BlurException, TException {
            return face.mutateCreateTransaction(table);
        }

        public void removeTable(String table, boolean deleteIndexFiles) throws BlurException, TException {
            face.removeTable(table, deleteIndexFiles);
        }

    }
}
