	package com.nearinfinity.blur.server;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Server;

import com.nearinfinity.blur.manager.DirectoryManagerImpl;
import com.nearinfinity.blur.manager.IndexManagerImpl;
import com.nearinfinity.blur.manager.SearchExecutorImpl;
import com.nearinfinity.blur.manager.SearchManagerImpl;
import com.nearinfinity.blur.manager.UpdatableManager;
import com.nearinfinity.blur.manager.dao.DirectoryManagerDao;
import com.nearinfinity.blur.utils.BlurConfiguration;
import com.nearinfinity.blur.utils.BlurConstants;
import com.nearinfinity.blur.utils.HttpConstants;

public class BlurNode extends BlurServer implements HttpConstants,BlurConstants {
	
	
	private static final Log LOG = LogFactory.getLog(BlurNode.class);
	private static final long TEN_SECONDS = 10000;
	private DirectoryManagerImpl directoryManager;
	private IndexManagerImpl indexManager;
	private SearchManagerImpl searchManager;
	private Timer timer;
	private BlurConfiguration configuration = new BlurConfiguration();
	
	public BlurNode() {
		init();
	}
	
	private void init() {
		DirectoryManagerDao dao = configuration.getNewInstance(BLUR_DIRECTORY_MANAGER_DAO, DirectoryManagerDao.class);
		this.directoryManager = new DirectoryManagerImpl(dao);
		this.indexManager = new IndexManagerImpl(directoryManager);
		this.searchManager = new SearchManagerImpl(indexManager);
		this.searchExecutor = new SearchExecutorImpl(searchManager);
		update(directoryManager, indexManager, searchManager, searchExecutor);
		runUpdateTask(directoryManager, indexManager, searchManager, searchExecutor);
	}
	
	private void runUpdateTask(final UpdatableManager... managers) {
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				update(managers);
			}
		};
		this.timer = new Timer("Update-Manager-Timer", true);
		this.timer.schedule(task, TEN_SECONDS, TEN_SECONDS);
	}
	
	private void update(UpdatableManager... managers) {
		LOG.info("Running Update");
		for (UpdatableManager manager : managers) {
			manager.update();
		}
	}
	
	public static void main(String[] args) throws Exception {
		int port = Integer.parseInt(args[0]);
		Server server = new Server(port);
		server.setHandler(new BlurNode());
		server.start();
		server.join();
	}
}
