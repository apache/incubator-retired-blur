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

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.nearinfinity.blur.log.Log;
import com.nearinfinity.blur.log.LogFactory;
import com.nearinfinity.blur.thrift.generated.Blur;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.Blur.Client;

public class BlurClientManager {
    
    private static final Log LOG = LogFactory.getLog(BlurClientManager.class);
    private static final int MAX_RETIRES = 10;
    private static final long BACK_OFF_TIME = TimeUnit.MILLISECONDS.toMillis(250);
    
    private static Map<String,BlockingQueue<Client>> clientPool = new ConcurrentHashMap<String, BlockingQueue<Client>>();
    
    @SuppressWarnings("unchecked")
    public static <CLIENT,T> T execute(String connectionStr, AbstractCommand<CLIENT,T> command) throws Exception {
        int retries = 0;
        while (true) {
            Blur.Client client = getClient(connectionStr);
            if (client == null) {
                throw new BlurException("Host [" + connectionStr + "] can not be contacted.",null);
            }
            try {
                return command.call((CLIENT) client);
            } catch (Exception e) {
                trashClient(connectionStr, client);
                client = null;
                if (retries >= MAX_RETIRES) {
                    LOG.error("No more retries [{0}] out of [{1}]",retries,MAX_RETIRES);
                    throw e;
                }
                retries++;
                LOG.error("Retrying call [{0}] retry [{1}] out of [{2}] message [{3}]",command,retries,MAX_RETIRES,e.getMessage());
                Thread.sleep(BACK_OFF_TIME);
            } finally {
                if (client != null) {
                    returnClient(connectionStr, client);
                }
            }
        }
    }

    private static void returnClient(String connectionStr, Client client) throws InterruptedException {
        clientPool.get(connectionStr).put(client);
    }

    private static void trashClient(String connectionStr, Client client) {
        client.getInputProtocol().getTransport().close();
        client.getOutputProtocol().getTransport().close();
    }

    private static Client getClient(String connectionStr) throws InterruptedException, TTransportException {
        BlockingQueue<Client> blockingQueue;
        synchronized (clientPool) {
            blockingQueue = clientPool.get(connectionStr);
            if (blockingQueue == null) {
                blockingQueue = new LinkedBlockingQueue<Client>();
                clientPool.put(connectionStr, blockingQueue);
            }
        }
        if (blockingQueue.isEmpty()) {
            return newClient(connectionStr);
        }
        return blockingQueue.take();
    }

    private static Client newClient(String connectionStr) throws TTransportException {
        String host = getHost(connectionStr);
        int port = getPort(connectionStr);
        TTransport trans = new TSocket(host, port);
        TProtocol proto = new TBinaryProtocol(new TFramedTransport(trans));
        Client client = new Client(proto);
        try {
            trans.open();
        } catch (Exception e) {
            LOG.error("Error trying to open connection to [{0}]",connectionStr);
            return null;
        }
        return client;
    }

    private static int getPort(String connectionStr) {
        return Integer.parseInt(connectionStr.substring(connectionStr.indexOf(':') + 1));
    }

    private static String getHost(String connectionStr) {
        return connectionStr.substring(0,connectionStr.indexOf(':'));
    }

}
