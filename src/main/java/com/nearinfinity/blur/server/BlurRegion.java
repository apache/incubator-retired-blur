package com.nearinfinity.blur.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.nearinfinity.blur.manager.DirectoryManagerImpl;
import com.nearinfinity.blur.manager.IndexManagerImpl;
import com.nearinfinity.blur.manager.SearchExecutorImpl;
import com.nearinfinity.blur.manager.SearchManagerImpl;
import com.nearinfinity.blur.manager.UpdatableManager;
import com.nearinfinity.blur.manager.dao.DirectoryManagerDao;

public class BlurRegion extends AbstractHandler {
	
	private static final Log LOG = LogFactory.getLog(BlurRegion.class);
	private static final String QUERY = "q";
	private static final String FILTER = "f";
	private static final String TEXT_HTML_CHARSET_UTF_8 = "text/html;charset=utf-8";
	private static final long TEN_SECONDS = 10000;

	private DirectoryManagerImpl directoryManager;
	private IndexManagerImpl indexManager;
	private SearchManagerImpl searchManager;
	private SearchExecutorImpl searchExecutor;
	private Timer timer;

	private ExecutorService executor = Executors.newCachedThreadPool();

	public BlurRegion() {
		init();
	}

	private void init() {
		DirectoryManagerDao dao = new DirectoryManagerDao() {
			@Override
			public Map<String, Set<String>> getShardIdsToServe() {
				Map<String, Set<String>> shardIds = new TreeMap<String, Set<String>>();
				shardIds.put("test", new TreeSet<String>(Arrays.asList("test")));
				return shardIds;
			}
			@Override
			public URI getURIForShardId(String table, String shardId) {
				try {
					return new URI("file:///Users/amccurry/testIndex");
				} catch (URISyntaxException e) {
					throw new RuntimeException(e);
				}
			}
		};
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

	public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		System.out.println(target);
		String table = "test";
		baseRequest.setHandled(true);
		long searchFast = -1;
		try {
			String query = getQuery(request);
			String filter = getFilter(request);
			long minimum = getMinimum(request);
			if (query != null) {
				searchFast = searchExecutor.searchFast(executor, table, query, filter, minimum);
			} else {
				response.setContentType(TEXT_HTML_CHARSET_UTF_8);
				response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				PrintWriter printWriter = response.getWriter();
				printWriter.println("{\"error\":\"query is blank\"}");
				printWriter.flush();
			}
			response.setContentType(TEXT_HTML_CHARSET_UTF_8);
			response.setStatus(HttpServletResponse.SC_OK);
			PrintWriter printWriter = response.getWriter();
			printWriter.println("{\"totalhits\":"+searchFast+"}");
			printWriter.flush();
		} catch (Exception e) {
			response.setContentType(TEXT_HTML_CHARSET_UTF_8);
			response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			PrintWriter printWriter = response.getWriter();
			printWriter.println("{\"error\":\""+e.getLocalizedMessage()+"\"}");
			printWriter.flush();
		}
	}

	private long getMinimum(HttpServletRequest request) {
		String minStr = request.getParameter("min");
		if (minStr == null) {
			return Long.MAX_VALUE;
		}
		return Long.parseLong(minStr);
	}

	private String getFilter(HttpServletRequest request) {
		return request.getParameter(FILTER);
	}

	private String getQuery(HttpServletRequest request) {
		return request.getParameter(QUERY);
	}

	public static void main(String[] args) throws Exception {
		Server server = new Server(8080);
		server.setHandler(new BlurRegion());
		server.start();
		server.join();
	}
}
