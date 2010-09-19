package com.nearinfinity.blur.thrift;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.zookeeper.ZooKeeper;

import com.nearinfinity.blur.manager.Partitioner;
import com.nearinfinity.blur.manager.PartitionerManager;
import com.nearinfinity.blur.manager.hits.HitsIterable;
import com.nearinfinity.blur.manager.hits.HitsIterableBlurClient;
import com.nearinfinity.blur.manager.hits.MergerHitsIterable;
import com.nearinfinity.blur.thrift.BlurClientManager.Command;
import com.nearinfinity.blur.thrift.generated.BlurException;
import com.nearinfinity.blur.thrift.generated.FetchResult;
import com.nearinfinity.blur.thrift.generated.Hits;
import com.nearinfinity.blur.thrift.generated.MissingShardException;
import com.nearinfinity.blur.thrift.generated.Row;
import com.nearinfinity.blur.thrift.generated.SearchQuery;
import com.nearinfinity.blur.thrift.generated.Blur.Client;
import com.nearinfinity.blur.utils.BlurConfiguration;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.ForkJoin;
import com.nearinfinity.blur.utils.ForkJoin.ParallelCall;
import com.nearinfinity.mele.Mele;

public class BlurControllerServer extends BlurAdminServer implements BlurConstants {
	
	private static final Log LOG = LogFactory.getLog(BlurControllerServer.class);
    private PartitionerManager partitionerManager;

	public BlurControllerServer(ZooKeeper zooKeeper, Mele mele, BlurConfiguration configuration) throws IOException {
		super(zooKeeper,mele,configuration);
		this.partitionerManager = new PartitionerManager(mele);
	}

	@Override
	public Hits searchInternal(final String table, final SearchQuery searchQuery) throws BlurException, TException {
		try {
			HitsIterable hitsIterable = ForkJoin.execute(executor, shardServerList(), new ParallelCall<String,HitsIterable>() {
				@Override
				public HitsIterable call(final String hostnamePort) throws Exception {
					return BlurClientManager.execute(hostnamePort, new Command<HitsIterable>() {
		                @Override
		                public HitsIterable call(Client client) throws Exception {
		                    return new HitsIterableBlurClient(client,hostnamePort,table,searchQuery);
		                }
		            });
				}
			}).merge(new MergerHitsIterable(searchQuery.minimumNumberOfHits));
			return convertToHits(hitsIterable, searchQuery.start, searchQuery.fetch, searchQuery.minimumNumberOfHits);
		} catch (Exception e) {
			LOG.error("Unknown error during search of [" +
					getParametersList("table",table, "searchquery", searchQuery) + "]",e);
			throw new BlurException(e.getMessage());
		}
	}
	
    @Override
	protected NODE_TYPE getType() {
		return NODE_TYPE.CONTROLLER;
	}

	@Override
	public void appendRowInternal(final String table, final Row row) throws BlurException,
			TException, MissingShardException {
	    String clientHostnamePort = getClientHostnamePort(table,row.id);
        try {
            BlurClientManager.execute(clientHostnamePort, 
                new Command<Boolean>() {
                    @Override
                    public Boolean call(Client client) throws Exception {
                        client.appendRow(table, row);
                        return true;
                    }
                });
        } catch (Exception e) {
            LOG.error("Unknown error during append of row from table [" + table +
                    "] id [" + row.id + "]",e);
            throw new BlurException("Unknown error during append of row from table [" + table +
                    "] id [" + row.id + "]");
        }
	}

	@Override
	public FetchResult fetchRowInternal(final String table, final String id) throws BlurException,
			TException, MissingShardException {
	    String clientHostnamePort = getClientHostnamePort(table,id);
		try {
		    return BlurClientManager.execute(clientHostnamePort, 
		        new Command<FetchResult>() {
                    @Override
                    public FetchResult call(Client client) throws Exception {
                        return client.fetchRow(table, id);
                    }
                });
		} catch (Exception e) {
		    LOG.error("Unknown error during fetch of row from table [" + table +
		    		"] id [" + id + "]",e);
		    throw new BlurException("Unknown error during fetch of row from table [" + table +
                    "] id [" + id + "]");
        }
	}

	@Override
	public void removeRowInternal(final String table, final String id) throws BlurException,
			TException, MissingShardException {
	    String clientHostnamePort = getClientHostnamePort(table,id);
        try {
            BlurClientManager.execute(clientHostnamePort, new Command<Boolean>() {
                    @Override
                    public Boolean call(Client client) throws Exception {
                        client.removeRow(table, id);
                        return true;
                    }
                });
        } catch (Exception e) {
            LOG.error("Unknown error during removal of row from table [" + table +
                    "] id [" + id + "]",e);
            throw new BlurException("Unknown error during removal of row from table [" + table +
                    "] id [" + id + "]");
        }
	}

	@Override
	public void replaceRowInternal(final String table, final Row row) throws BlurException,
			TException, MissingShardException {
	    String clientHostnamePort = getClientHostnamePort(table,row.id);
	    try {
            BlurClientManager.execute(clientHostnamePort, new Command<Boolean>() {
                    @Override
                    public Boolean call(Client client) throws Exception {
                        client.replaceRow(table, row);
                        return true;
                    }
                });
        } catch (Exception e) {
            LOG.error("Unknown error during replacing of row from table [" + table +
                    "] id [" + row.id + "]",e);
            throw new BlurException("Unknown error during replacing of row from table [" + table +
                    "] id [" + row.id + "]");
        }
	}
	
    @Override
    public void cancelSearchInternal(long providedUuid) throws BlurException, TException {
        throw new BlurException("not implemented");
    }
	   
    private String getClientHostnamePort(String table, String id) throws MissingShardException, BlurException, TException {
        Map<String, String> shardServerLayout = shardServerLayout(table);
        Partitioner partitioner = partitionerManager.getPartitioner(table);
        String shard = partitioner.getShard(id);
        String host = shardServerLayout.get(shard);
        if (host == null) {
            throw new MissingShardException("Controller can not locate shard [" + shard + 
                    "] for table [" + table + "] id [" + id + "]");
        }
        return host + ":" + configuration.getBlurShardServerPort();
    }


}