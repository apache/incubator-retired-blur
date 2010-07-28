package com.nearinfinity.blur.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.client.HTable;

import com.nearinfinity.blur.hbase.BlurHits;
import com.nearinfinity.blur.hbase.SearchRPC;
import com.nearinfinity.blur.hbase.ipc.SearchRegionInterface;
import com.nearinfinity.blur.utils.IterableConverter;
import com.nearinfinity.blur.utils.IterableConverter.Converter;

public class SearchTable extends HTable {
	
	static interface ParallelCall<T> {
		T call(SearchRegionInterface searchRegionInterface) throws Exception;
	}
	
	static interface ParallelResult<T> {
		T merge(Join<T> join) throws Exception;
	}
	
	static interface Join<T> {
		T join(List<Future<T>> futures) throws Exception;
	}
	
	private ExecutorService executor = Executors.newCachedThreadPool();
	
	static {
		SearchRPC.initialize();
	}
	
	public SearchTable(byte[] tableName) throws IOException {
		super(tableName);
	}

	public SearchTable(HBaseConfiguration conf, byte[] tableName) throws IOException {
		super(conf, tableName);
	}

	public SearchTable(HBaseConfiguration conf, String tableName) throws IOException {
		super(conf, tableName);
	}

	public SearchTable(String tableName) throws IOException {
		super(tableName);
	}
	
	public long searchFast(String query, String filter) throws Exception {
		return searchFast(query, filter, Long.MAX_VALUE);
	}
	
	public long searchFast(final String query, final String filter, final long minimum) throws Exception {
		return execute(new ParallelCall<Long>() {
			@Override
			public Long call(SearchRegionInterface searchRegionInterface) throws Exception {
				return searchRegionInterface.searchFast(query, filter, minimum);
			}
		}).merge(new Join<Long>(){
			@Override
			public Long join(List<Future<Long>> futures) throws Exception {
				long total = 0;
				for (Future<Long> future : futures) {
					total += future.get();
					if (total >= minimum) {
						return total;
					}
				}
				return total;
			}			
		});
	}
	
	public BlurHits search(final String query, final String filter, final long start, final int fetchCount) throws Exception {
		return execute(new ParallelCall<BlurHits>() {
			@Override
			public BlurHits call(SearchRegionInterface searchRegionInterface) throws Exception {
				return searchRegionInterface.search(query, filter, start, fetchCount);
			}
		}).merge(new Join<BlurHits>(){
			@Override
			public BlurHits join(List<Future<BlurHits>> futures) throws Exception {
				BlurHits hits = null;
				for (Future<BlurHits> future : futures) {
					if (hits == null) {
						hits = future.get();
					} else {
						hits.merge(future.get());
					}
				}
				return hits;
			}			
		});
	}

	private <T> ParallelResult<T> execute(final ParallelCall<T> parallelCall) {
		return new ParallelResult<T>() {
			@Override
			public T merge(Join<T> join) throws Exception {
				List<Future<T>> futures = new ArrayList<Future<T>>();
				for (final SearchRegionInterface regionInterface : getAllSearchRegions(getRegionsInfo())) {
					futures.add(executor.submit(new Callable<T>() {
						@Override
						public T call() throws Exception {
							return parallelCall.call(regionInterface);
						}
					}));
				}
				return join.join(futures);
			}
		};
	}

	private Iterable<SearchRegionInterface> getAllSearchRegions(Map<HRegionInfo, HServerAddress> regionsInfo) {
		return new IterableConverter<HServerAddress,SearchRegionInterface>(regionsInfo.values(), new Converter<HServerAddress,SearchRegionInterface>() {
			@Override
			public SearchRegionInterface convert(HServerAddress address) throws Exception {
				return (SearchRegionInterface) getConnection().getHRegionConnection(address);
			}
		});
	}

	@Override
	public void close() throws IOException {
		executor.shutdownNow();
		super.close();
	}

}
