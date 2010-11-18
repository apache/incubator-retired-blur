package com.nearinfinity.blur.thrift;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.nearinfinity.blur.thrift.generated.BlurAdmin;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.BlurAdmin.Client;

public class BlurClientManager {
    
    private static final Log LOG = LogFactory.getLog(BlurClientManager.class);
    private static final int MAX_RETIRES = 2;
    
    private static Map<String,BlockingQueue<Client>> clientPool = new ConcurrentHashMap<String, BlockingQueue<Client>>();
    
    @SuppressWarnings("unchecked")
    public static <CLIENT,T> T execute(String connectionStr, AbstractCommand<CLIENT,T> command) throws Exception {
        int retries = 0;
        while (true) {
            BlurAdmin.Client client = getClient(connectionStr);
            if (client == null) {
                throw new BlurException("Host [" + connectionStr + "] can not be contacted.");
            }
            try {
                return command.call((CLIENT) client);
            } catch (TTransportException e) {
                if (retries >= MAX_RETIRES) {
                    LOG.error("No more retries [" + retries + "] out of [" + MAX_RETIRES + "]");
                    throw e;
                }
                if (equals(e.getType(),TTransportException.NOT_OPEN,TTransportException.TIMED_OUT,TTransportException.UNKNOWN)) {
                    LOG.error("Trash client [" + client + "] retry [" + retries + "] out of [" + MAX_RETIRES + "]", e);
                    trashClient(connectionStr, client);
                    client = null;
                }
                retries++;
                LOG.error("Retrying call [" + client + "] retry [" + retries + "] out of [" + MAX_RETIRES + "]", e);
            } finally {
                if (client != null) {
                    returnClient(connectionStr, client);
                }
            }
        }
    }

    private static boolean equals(int i, int... ors) {
        for (int o : ors) {
            if (i == o) {
                return true;
            }
        }
        return false;
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
            LOG.error("Error trying to open connection to [" + connectionStr + "]");
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
