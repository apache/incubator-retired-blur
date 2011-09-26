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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClient;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;

public class AsyncClientPool {
    
    public static final Log LOG = LogFactory.getLog(AsyncClientPool.class);
    
    public static final int DEFAULT_MAX_CONNECTIONS_PER_HOST = 5;
    public static final int DEFAULT_CONNECTION_TIMEOUT = 60000;
    
    private int _maxConnectionsPerHost;
    private long _timeout;
    private long _pollTime = 5;
    private Map<String,AtomicInteger> _numberOfConnections = new ConcurrentHashMap<String,AtomicInteger>();
    
    private Map<Connection,BlockingQueue<TAsyncClient>> _clientMap = new ConcurrentHashMap<Connection, BlockingQueue<TAsyncClient>>();
    private Map<String,Constructor<?>> _constructorCache = new ConcurrentHashMap<String, Constructor<?>>();
    private TProtocolFactory _protocolFactory;
    private TAsyncClientManager _clientManager;
    
    public AsyncClientPool() throws IOException {
        this(DEFAULT_MAX_CONNECTIONS_PER_HOST, DEFAULT_CONNECTION_TIMEOUT);
    }
    
    public AsyncClientPool(int maxConnectionsPerHost, int connectionTimeout) throws IOException {
        _clientManager = new TAsyncClientManager();
        _protocolFactory = new TBinaryProtocol.Factory();
        _maxConnectionsPerHost = maxConnectionsPerHost;
        _timeout = connectionTimeout;
    }
    
    /**
     * Gets a client instance that implements the AsyncIface interface that connects to the given connection string.
     * @param <T>
     * @param asyncIfaceClass the AsyncIface interface to pool.
     * @param connectionStr the connection string.
     * @return the client instance.
     */
    @SuppressWarnings("unchecked")
    public <T> T getClient(final Class<T> asyncIfaceClass, final String connectionStr) {
        final Connection connection = new Connection(connectionStr);
        return (T) Proxy.newProxyInstance(asyncIfaceClass.getClassLoader(), new Class[] { asyncIfaceClass }, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                return execute(new AsyncCall(asyncIfaceClass,method,args,connection));
            }
        });
    }

    private Object execute(AsyncCall call) throws Exception {
        AsyncMethodCallback<?> realCallback = getRealAsyncMethodCallback(call._args);
        TAsyncClient client = newClient(call._clazz,call._connection);
        AsyncMethodCallback<?> retryingCallback = wrapCallback(realCallback,client,call._connection);
        resetArgs(call._args,retryingCallback);
        return call._method.invoke(client, call._args);
    }
    
    private synchronized BlockingQueue<TAsyncClient> getQueue(Connection connection) {
        BlockingQueue<TAsyncClient> blockingQueue = _clientMap.get(connection);
        if (blockingQueue == null) {
            blockingQueue = new LinkedBlockingQueue<TAsyncClient>();
            _clientMap.put(connection, blockingQueue);
        }
        return blockingQueue;
    }

    private void returnClient(Connection connection, TAsyncClient client) throws InterruptedException {
        if (!client.hasError()) {
            getQueue(connection).put(client);
        } else {
            AtomicInteger counter = _numberOfConnections.get(connection._host);
        	if (counter != null) {
        		counter.decrementAndGet();
        	}
        }
    }

    @SuppressWarnings("unchecked")
    private AsyncMethodCallback<?> wrapCallback(AsyncMethodCallback<?> realCallback, TAsyncClient client, Connection connectionStr) {
        return new ClientPoolAsyncMethodCallback(realCallback,client,this,connectionStr);
    }

    private void resetArgs(Object[] args, AsyncMethodCallback<?> callback) {
        args[args.length - 1] = callback;
    }

    private AsyncMethodCallback<?> getRealAsyncMethodCallback(Object[] args) {
        return (AsyncMethodCallback<?>) args[args.length - 1];
    }

    private TAsyncClient newClient(Class<?> c, Connection connection) throws InterruptedException {
        BlockingQueue<TAsyncClient> blockingQueue = getQueue(connection);
        TAsyncClient client = blockingQueue.poll();
        if (client != null) {
            return client;
        }
        
        AtomicInteger counter;
        synchronized (_numberOfConnections) {
        	counter = _numberOfConnections.get(connection._host);
        	if (counter == null) {
        		counter = new AtomicInteger();
        		_numberOfConnections.put(connection._host,counter);
        	}
		}
        
        synchronized (counter) {
            int numOfConnections = counter.get();
            while (numOfConnections >= _maxConnectionsPerHost) {
                client = blockingQueue.poll(_pollTime , TimeUnit.MILLISECONDS);
                if (client != null) {
                    return client;
                }
                LOG.debug("Waiting for client number of connection [" + numOfConnections +
                		"], max connection per host [" + _maxConnectionsPerHost + "]");
                numOfConnections = counter.get();
            }
            LOG.info("Creating a new client for [" + connection + "]");
            String name = c.getName();
            Constructor<?> constructor = _constructorCache.get(name);
            if (constructor == null) {
                String clientClassName = name.replace("$AsyncIface", "$AsyncClient");
                try {
                    Class<?> clazz = Class.forName(clientClassName);
                    constructor = clazz.getConstructor(new Class[]{TProtocolFactory.class,TAsyncClientManager.class,TNonblockingTransport.class});
                    _constructorCache.put(name, constructor);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                client = (TAsyncClient) constructor.newInstance(new Object[]{_protocolFactory,_clientManager,newTransport(connection)});
                client.setTimeout(_timeout);
                counter.incrementAndGet();
                return client;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
		}
    }

    private TNonblockingSocket newTransport(Connection connection) throws IOException {
        return new TNonblockingSocket(connection._host, connection._port);
    }
    
    private static class ClientPoolAsyncMethodCallback<T> implements AsyncMethodCallback<T> {
        
        private AsyncMethodCallback<T> _realCallback;
        private TAsyncClient _client;
        private AsyncClientPool _pool;
        private Connection _connection;

        public ClientPoolAsyncMethodCallback(AsyncMethodCallback<T> realCallback, TAsyncClient client, AsyncClientPool pool, Connection connection) {
            _realCallback = realCallback;
            _client = client;
            _pool = pool;
            _connection = connection;
        }

        @Override
        public void onComplete(T response) {
            _realCallback.onComplete(response);
            try {
                _pool.returnClient(_connection,_client);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onError(Exception exception) {
        	AtomicInteger counter = _pool._numberOfConnections.get(_connection._host);
        	if (counter != null) {
        		counter.decrementAndGet();
        	}
            _realCallback.onError(exception);
        }
    }
    
    private static class Connection {
        String _host;
        int _port;
        public Connection(String connection) {
            String[] split = connection.split(":");
            if (split.length != 2) {
                throw new RuntimeException("Connection [" + connection + "] invalid expecting {hostname:port}");
            }
            _host = split[0];
            _port = Integer.parseInt(split[1]);
        }
        
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((_host == null) ? 0 : _host.hashCode());
            result = prime * result + _port;
            return result;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Connection other = (Connection) obj;
            if (_host == null) {
                if (other._host != null)
                    return false;
            } else if (!_host.equals(other._host))
                return false;
            if (_port != other._port)
                return false;
            return true;
        }
    }
    
    private static class AsyncCall {
        
        Class<?> _clazz;
        Method _method;
        Object[] _args;
        Connection _connection;

        public AsyncCall(Class<?> clazz, Method method, Object[] args, Connection connection) {
            _clazz = clazz;
            _method = method;
            _args = args;
            _connection = connection;
        }
    }
}
