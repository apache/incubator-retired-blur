package com.nearinfinity.blur.hbase;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ipc.HBaseRPCProtocolVersion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

import com.nearinfinity.blur.manager.DirectoryManagerImpl;
import com.nearinfinity.blur.manager.IndexManagerImpl;
import com.nearinfinity.blur.manager.SearchExecutorImpl;
import com.nearinfinity.blur.manager.SearchManagerImpl;
import com.nearinfinity.blur.manager.UpdatableManager;
import com.nearinfinity.blur.manager.dao.DirectoryManagerDao;

public class BlurRegionServer extends HRegionServer implements BlurRegionInterface {
	
	private static final long TEN_SECONDS = 10000;

	static {
		BlurRPC.initialize();
	}
	
	private DirectoryManagerImpl directoryManager;
	private IndexManagerImpl indexManager;
	private SearchManagerImpl searchManager;
	private SearchExecutorImpl searchExecutor;
	private ExecutorService executor = Executors.newCachedThreadPool();
	private Timer timer;

	public BlurRegionServer(HBaseConfiguration conf) throws IOException {
		super(conf);
		DirectoryManagerDao dao = null;
		this.directoryManager = new DirectoryManagerImpl(dao);
		this.indexManager = new IndexManagerImpl(directoryManager);
		this.searchManager = new SearchManagerImpl(indexManager);
		this.searchExecutor = new SearchExecutorImpl(searchManager);
		updateTask(directoryManager,indexManager,searchManager,searchExecutor);
	}

	@Override
	public BlurHits search(String query, String filter, long start, int fetchCount) {
		return searchExecutor.search(executor, query, filter, start, fetchCount);
	}

	@Override
	public long searchFast(String query, String filter, long minimum) {
		return searchExecutor.searchFast(executor, query, filter, minimum);
	}
	
	@Override
	public long getProtocolVersion(final String protocol, final long clientVersion) throws IOException {
		if (protocol.equals(BlurRegionInterface.class.getName())) {
			return HBaseRPCProtocolVersion.versionID;
		}
		return super.getProtocolVersion(protocol, clientVersion);
	}
	
	private void updateTask(final UpdatableManager... managers) {
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				for (UpdatableManager manager : managers) {
					manager.update();
				}
			}
		};
		this.timer = new Timer("Update-Manager-Timer", true);
		this.timer.schedule(task, TEN_SECONDS, TEN_SECONDS);
	}

}
